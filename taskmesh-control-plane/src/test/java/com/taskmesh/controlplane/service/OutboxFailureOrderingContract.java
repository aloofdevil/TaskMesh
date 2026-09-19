package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobEventRepository;

import tools.jackson.databind.ObjectMapper;

/**
 * Day 21: what a <em>partial failure</em> does to one key's ordering.
 * <p>
 * Day 20 recorded that the batched path loses the sequential path's
 * prefix-on-failure property, but stopped there. This pins the consequence that
 * actually matters to a consumer: if event 3 of a key fails while 4 and 5
 * succeed, the retry publishes 3 <em>after</em> them, so the key is delivered
 * out of sequence. The sequential path cannot do that, because it stops at 3
 * and republishes 3, 4, 5 in order on the next pass.
 * <p>
 * Both modes must still lose nothing and duplicate nothing - only the observed
 * order differs. Each subclass declares the exact order it expects, so the
 * difference is asserted rather than described.
 * <p>
 * The failure is deterministic and application-level, per Day 21's instruction
 * to prefer that over damaging the broker: one record exceeds
 * {@code max.request.size} while its neighbours do not. The events here are
 * ~150 bytes, so only the deliberately fat one is affected.
 */
@SpringBootTest
abstract class OutboxFailureOrderingContract {

    private static final String OVERSIZED = "x".repeat(8192);
    private static final int SEQ_COUNT = 5;
    private static final int FAILING_SEQ = 3;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private OutboxProperties outboxProperties;

    @Autowired
    private KafkaConnectionDetails kafkaConnectionDetails;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    abstract boolean expectedAsyncSends();

    /** How many of the five publish on the first pass, before the blockage clears. */
    abstract int expectedPublishedOnFirstPass();

    /** The sequence order a consumer actually observes for the key, end to end. */
    abstract List<Integer> expectedObservedOrder();

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
    }

    @Test
    void theSendModeUnderTestIsTheOneConfigured() {
        assertThat(outboxProperties.asyncSends()).isEqualTo(expectedAsyncSends());
    }

    @Test
    void aPartialFailureLosesNothingButMayReorderTheKey() {
        String key = "fail-" + UUID.randomUUID().toString().substring(0, 8);
        List<Long> ids = new ArrayList<>();
        for (int seq = 1; seq <= SEQ_COUNT; seq++) {
            ids.add(jobEventRepository.save(JobEvent.record(key, JobEventType.JOB_QUEUED,
                    Map.<String, Object>of("seq", seq,
                            "filler", seq == FAILING_SEQ ? OVERSIZED : "small"))).getId());
        }

        // Pass 1: the fat record is rejected by the producer. How many of its
        // neighbours still publish is exactly what differs between the modes.
        assertThat(outboxPublisher.publishPending())
                .as("first pass under async-sends=%s", expectedAsyncSends())
                .isEqualTo(expectedPublishedOnFirstPass());

        Long failingId = ids.get(FAILING_SEQ - 1);
        assertThat(jobEventRepository.findById(failingId).orElseThrow().isPublished())
                .as("the rejected event is never marked published")
                .isFalse();

        // Clear the blockage in place, so the row - and therefore the event id a
        // consumer deduplicates on - is the same one that failed.
        jdbcTemplate.update("update job_events set payload = ?::jsonb where id = ?",
                "{\"seq\":" + FAILING_SEQ + ",\"filler\":\"small\"}", failingId);

        for (int pass = 0; pass < 5 && jobEventRepository.countByPublishedAtIsNull() > 0; pass++) {
            outboxPublisher.publishPending();
        }

        assertThat(jobEventRepository.countByPublishedAtIsNull())
                .as("nothing is left behind once the cause is gone")
                .isZero();
        assertThat(jobEventRepository.findById(failingId).orElseThrow().getId())
                .as("the event id is stable across the failure and the retry")
                .isEqualTo(failingId);

        List<ConsumerRecord<String, String>> records = consumeForKey(key, SEQ_COUNT);
        List<Integer> observed = records.stream()
                .map(r -> objectMapper.readTree(r.value()).get("payload").get("seq").asInt())
                .toList();
        List<Long> observedIds = records.stream()
                .map(r -> objectMapper.readTree(r.value()).get("eventId").asLong())
                .toList();

        assertThat(observedIds)
                .as("no event lost, none delivered twice")
                .hasSize(SEQ_COUNT)
                .containsExactlyInAnyOrderElementsOf(ids);
        assertThat(observed)
                .as("the delivery order a consumer sees under async-sends=%s", expectedAsyncSends())
                .containsExactlyElementsOf(expectedObservedOrder());
    }

    /**
     * All of a key's records share one partition, so a consumer pinned to the
     * topic still yields that key's records in delivery order once filtered.
     */
    private List<ConsumerRecord<String, String>> consumeForKey(String key, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaConnectionDetails.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "day21-fail-" + UUID.randomUUID());
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
                    if (key.equals(record.key())) {
                        mine.add(record);
                    }
                }
            }
        }
        mine.sort(java.util.Comparator.comparingLong(ConsumerRecord::offset));
        return mine;
    }
}
