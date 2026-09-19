package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobEventRepository;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Day 21: the Kafka guarantees the outbox actually delivers, checked against a
 * real broker under both send modes.
 * <p>
 * {@code KafkaTopicsConfig} states the intent this verifies: events are "keyed
 * by job id ... so all events for one aggregate land on the same partition and
 * a consumer sees that job's history in order". Day 20 made the publisher
 * submit a whole batch before awaiting any acknowledgement, putting many
 * records for one key in flight at once - so whether that intent still holds is
 * a question about the new code, not about Kafka's documentation.
 * <p>
 * The experiment runs at publisher concurrency 1, calling
 * {@link OutboxPublisher#publishPending()} directly. Ordering across
 * <em>concurrent</em> passes is already documented as not guaranteed (Day 15:
 * concurrent passes take disjoint batches and race), so including it would
 * confound the one question Day 21 asks - does submit-all-then-wait reorder a
 * key's events <em>within</em> a pass?
 * <p>
 * {@code KEYS * EVENTS_PER_KEY} equals {@code taskmesh.outbox.batch-size}, so
 * every event is claimed by a single pass. That is the maximum-risk shape: in
 * batched mode all 100 records, including all 20 for one key, reach the producer
 * before a single acknowledgement is read.
 * <p>
 * This class is abstract and deliberately <strong>not</strong> named
 * {@code *Test} or {@code *Tests}, so Surefire does not run it without a mode
 * bound. The two concrete subclasses are top-level classes because Surefire's
 * default excludes drop inner classes ({@code **}{@code /*$*}) - the Day 14
 * discovery failure.
 */
abstract class OutboxOrderingContract {

    /** Deterministic and explicit; no randomness in this experiment. */
    static final int KEYS = 5;
    static final int EVENTS_PER_KEY = 20;
    static final int TOTAL_EVENTS = KEYS * EVENTS_PER_KEY;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private OutboxProperties outboxProperties;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaConnectionDetails kafkaConnectionDetails;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    /** Which send mode this subclass exists to exercise. */
    abstract boolean expectedAsyncSends();

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
    }

    /**
     * Days 14 and 15 were each invalidated once by a manipulation that never
     * reached the code under test. Assert the mode before believing anything
     * else in this class.
     */
    @Test
    void theSendModeUnderTestIsTheOneConfigured() {
        assertThat(outboxProperties.asyncSends())
                .as("this subclass exists to exercise async-sends=%s", expectedAsyncSends())
                .isEqualTo(expectedAsyncSends());
    }

    /**
     * The producer configuration as the client itself resolves it, defaults
     * included. {@code ProducerConfig} is the same class {@code KafkaProducer}
     * builds from the same map, so this reproduces the real resolution rather
     * than restating what the YAML sets.
     */
    @Test
    void theProducerIsConfiguredForIdempotentDelivery() {
        ProducerConfig resolved = new ProducerConfig(
                kafkaTemplate.getProducerFactory().getConfigurationProperties());

        assertThat(resolved.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
                .as("idempotence is what stops a retry from reordering or duplicating "
                        + "records within a partition; without it, submit-all-then-wait "
                        + "with several requests in flight could reorder on retry")
                .isTrue();
        assertThat(resolved.getString(ProducerConfig.ACKS_CONFIG))
                .as("acks=all is required for idempotence to be permitted")
                .isIn("all", "-1");
        assertThat(resolved.getInt(ProducerConfig.RETRIES_CONFIG)).isPositive();
        assertThat(resolved.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION))
                .as("idempotence preserves ordering only up to 5 in-flight requests")
                .isBetween(1, 5);
    }

    @Test
    void everyKeysEventsArriveInSequenceOrder() {
        Map<String, List<Long>> written = writeEvents();
        publishEverything();

        Map<String, List<Integer>> observed =
                sequencesByKey(consumeForKeys(written.keySet(), TOTAL_EVENTS));

        assertThat(observed.keySet())
                .as("every key written must be observed")
                .isEqualTo(written.keySet());
        List<Integer> expected = IntStream.rangeClosed(1, EVENTS_PER_KEY).boxed().toList();
        observed.forEach((key, seqs) -> assertThat(seqs)
                .as("key %s must be observed in broker offset order 1..%d", key, EVENTS_PER_KEY)
                .containsExactlyElementsOf(expected));
    }

    @Test
    void eachKeyLandsOnASinglePartitionSoOrderingIsMeaningful() {
        Map<String, List<Long>> written = writeEvents();
        publishEverything();

        Map<String, Set<Integer>> partitions = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record
                : consumeForKeys(written.keySet(), TOTAL_EVENTS)) {
            partitions.computeIfAbsent(record.key(), k -> new HashSet<>()).add(record.partition());
        }
        assertThat(partitions).hasSize(KEYS);
        partitions.forEach((key, parts) -> assertThat(parts)
                .as("per-key ordering is a guarantee only because key %s maps to one partition", key)
                .hasSize(1));
    }

    @Test
    void differentKeysMayOccupyDifferentPartitions() {
        Map<String, List<Long>> written = writeEvents();
        publishEverything();

        Set<Integer> used = new HashSet<>();
        for (ConsumerRecord<String, String> record
                : consumeForKeys(written.keySet(), TOTAL_EVENTS)) {
            used.add(record.partition());
        }
        // Not an ordering guarantee - the point is that keys are spread, so the
        // ordering checked above is genuinely per-key and not an accident of
        // everything sharing one partition.
        assertThat(used)
                .as("%d keys over %d partitions should not collapse onto one",
                        KEYS, outboxProperties.topicPartitions())
                .hasSizeGreaterThan(1);
    }

    @Test
    void noEventIdIsDuplicatedOrMissing() {
        Map<String, List<Long>> written = writeEvents();
        Set<Long> expectedIds = new HashSet<>();
        written.values().forEach(expectedIds::addAll);

        publishEverything();

        List<Long> consumedIds = new ArrayList<>();
        for (ConsumerRecord<String, String> record
                : consumeForKeys(written.keySet(), TOTAL_EVENTS)) {
            consumedIds.add(read(record).get("eventId").asLong());
        }

        assertThat(consumedIds).hasSize(TOTAL_EVENTS);
        assertThat(new HashSet<>(consumedIds))
                .as("every written event id observed exactly once in this run")
                .isEqualTo(expectedIds);
    }

    /**
     * Ordering must survive the interesting case too: part of a key's history
     * published by one pass, the rest by a later one.
     */
    @Test
    void orderingSurvivesAKeySplitAcrossTwoPasses() {
        String key = "order-split-" + UUID.randomUUID().toString().substring(0, 8);
        for (int seq = 1; seq <= 10; seq++) {
            record(key, seq);
        }
        // The second half does not exist yet, so the first pass cannot claim it.
        assertThat(outboxPublisher.publishPending()).isEqualTo(10);

        for (int seq = 11; seq <= 20; seq++) {
            record(key, seq);
        }
        assertThat(outboxPublisher.publishPending()).isEqualTo(10);

        assertThat(sequencesByKey(consumeForKeys(Set.of(key), 20)).get(key))
                .as("a key's history spanning two passes still arrives in order")
                .containsExactlyElementsOf(IntStream.rangeClosed(1, 20).boxed().toList());
    }

    // ---------------------------------------------------------------- helpers

    private Map<String, List<Long>> writeEvents() {
        Map<String, List<Long>> written = new LinkedHashMap<>();
        // A per-invocation run id keeps this run's records distinguishable from
        // those an earlier test method left on the shared broker topic - the
        // database is truncated between tests, the Kafka topic is not.
        String run = UUID.randomUUID().toString().substring(0, 8);
        for (int k = 0; k < KEYS; k++) {
            String key = "order-" + run + "-key" + k;
            List<Long> ids = new ArrayList<>();
            for (int seq = 1; seq <= EVENTS_PER_KEY; seq++) {
                ids.add(record(key, seq));
            }
            written.put(key, ids);
        }
        return written;
    }

    private Long record(String key, int seq) {
        return jobEventRepository.save(JobEvent.record(key, JobEventType.JOB_QUEUED,
                Map.<String, Object>of("seq", seq))).getId();
    }

    private void publishEverything() {
        for (int pass = 0; pass < 50 && jobEventRepository.countByPublishedAtIsNull() > 0; pass++) {
            outboxPublisher.publishPending();
        }
        assertThat(jobEventRepository.countByPublishedAtIsNull())
                .as("all events must be published before ordering or completeness is judged")
                .isZero();
    }

    private JsonNode read(ConsumerRecord<String, String> record) {
        return objectMapper.readTree(record.value());
    }

    /**
     * Sequence numbers per key in <em>broker</em> order - sorted by partition
     * then offset, not by the order the consumer happened to hand them over.
     */
    private Map<String, List<Integer>> sequencesByKey(List<ConsumerRecord<String, String>> records) {
        Map<String, List<ConsumerRecord<String, String>>> byKey = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            byKey.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record);
        }
        Map<String, List<Integer>> out = new LinkedHashMap<>();
        byKey.forEach((key, rs) -> out.put(key, rs.stream()
                .sorted(Comparator.comparingInt(ConsumerRecord<String, String>::partition)
                        .thenComparingLong(ConsumerRecord::offset))
                .map(r -> read(r).get("payload").get("seq").asInt())
                .toList()));
        return out;
    }

    /**
     * Reads the job-events topic from the beginning and keeps only the records
     * belonging to {@code keys}, so records left by earlier test methods on the
     * shared broker cannot inflate or corrupt the result.
     */
    private List<ConsumerRecord<String, String>> consumeForKeys(Set<String> keys, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaConnectionDetails.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "day21-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> mine = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(outboxProperties.jobEventsTopic()));
            long deadline = System.currentTimeMillis() + 30_000;
            while (mine.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.key() != null && keys.contains(record.key())) {
                        mine.add(record);
                    }
                }
            }
        }
        assertThat(mine)
                .as("expected %d records for %d key(s) within the poll deadline", expected, keys.size())
                .hasSize(expected);
        return mine;
    }
}
