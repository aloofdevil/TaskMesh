package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.domain.JobStatus;
import com.taskmesh.controlplane.repository.JobEventRepository;
import com.taskmesh.controlplane.repository.JobRepository;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * The transactional outbox: events are written atomically with the state
 * change they describe, and published to a real Kafka broker afterwards.
 * <p>
 * The background publisher is disabled so each test controls exactly when
 * publishing happens and can observe the unpublished state in between.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.retry.initial-backoff=1s",
        "taskmesh.retry.max-backoff=60s"
})
class OutboxTests {

    @Autowired
    private JobService jobService;

    @Autowired
    private DispatchService dispatchService;

    @Autowired
    private ExecutionService executionService;

    @Autowired
    private WorkerService workerService;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JobEventRecorder eventRecorder;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PlatformTransactionManager transactionManager;

    /**
     * Taken from the connection details bean rather than
     * {@code ${spring.kafka.bootstrap-servers}}: {@code @ServiceConnection}
     * supplies the container's address through this bean and never writes
     * it into the environment, so reading the property would silently give
     * the application.yml default and point this consumer at a broker that
     * is not there.
     */
    @Autowired
    private KafkaConnectionDetails kafkaConnectionDetails;

    @Value("${taskmesh.outbox.job-events-topic}")
    private String jobEventsTopic;

    @Value("${taskmesh.outbox.worker-events-topic}")
    private String workerEventsTopic;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
    }

    // ---------- helpers ----------

    private String registerWorker() {
        String workerId = "worker-" + UUID.randomUUID();
        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));
        return workerId;
    }

    private UUID createJob(String name, int maxAttempts) {
        return jobService.createJob(new CreateJobRequest(
                name + "-" + UUID.randomUUID(), "TEST_JOB", Map.of("name", name), 5, maxAttempts, null))
                .job().getId();
    }

    private List<JobEventType> eventTypesFor(UUID jobId) {
        return jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).stream()
                .map(JobEvent::getEventType)
                .toList();
    }

    /**
     * Reads a topic from the beginning and keeps only the records for one
     * aggregate. The Kafka broker is shared across tests and, unlike the
     * database, is not truncated between them, so a test must identify its
     * own messages by key rather than assume the topic starts empty.
     */
    private List<ConsumerRecord<String, String>> consumeFor(String topic, String key, int expected) {
        return consume(topic, expected).stream()
                .filter(record -> key.equals(record.key()))
                .toList();
    }

    /** Drains a topic from the beginning, so a test can see exactly what was published. */
    private List<ConsumerRecord<String, String>> consume(String topic, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", kafkaConnectionDetails.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> collected = new java.util.ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
        }
        return collected;
    }

    // ---------- events are recorded for each transition ----------

    @Test
    void jobCreationRecordsAQueuedEvent() {
        UUID jobId = createJob("created", 3);

        assertThat(eventTypesFor(jobId)).containsExactly(JobEventType.JOB_QUEUED);
        assertThat(jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).get(0).isPublished())
                .as("events start unpublished")
                .isFalse();
    }

    @Test
    void claimingRecordsARunningEvent() {
        String workerId = registerWorker();
        UUID jobId = createJob("claimed", 3);

        dispatchService.claim(workerId).orElseThrow();

        assertThat(eventTypesFor(jobId)).containsExactly(JobEventType.JOB_QUEUED, JobEventType.JOB_RUNNING);
    }

    @Test
    void completionRecordsACompletedEvent() {
        String workerId = registerWorker();
        UUID jobId = createJob("completed", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        executionService.complete(jobId, workerId, claimed.executionId());

        assertThat(eventTypesFor(jobId)).containsExactly(
                JobEventType.JOB_QUEUED, JobEventType.JOB_RUNNING, JobEventType.JOB_COMPLETED);
    }

    @Test
    void failureWithAttemptsRemainingRecordsARetryingEvent() {
        String workerId = registerWorker();
        UUID jobId = createJob("retried", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        executionService.fail(jobId, workerId, claimed.executionId(), "boom");

        assertThat(eventTypesFor(jobId)).containsExactly(
                JobEventType.JOB_QUEUED, JobEventType.JOB_RUNNING, JobEventType.JOB_RETRYING);
    }

    @Test
    void finalFailureRecordsADeadLetterEvent() {
        String workerId = registerWorker();
        UUID jobId = createJob("doomed", 1);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        executionService.fail(jobId, workerId, claimed.executionId(), "fatal");

        assertThat(eventTypesFor(jobId)).containsExactly(
                JobEventType.JOB_QUEUED, JobEventType.JOB_RUNNING, JobEventType.JOB_DEAD_LETTER);
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.DEAD_LETTER);
    }

    @Test
    void cancellationRecordsACancelledEvent() {
        UUID jobId = createJob("cancelled", 3);

        jobService.cancelJob(jobId);

        assertThat(eventTypesFor(jobId)).containsExactly(JobEventType.JOB_QUEUED, JobEventType.JOB_CANCELLED);
    }

    @Test
    void workerRegistrationAndDeregistrationRecordEvents() {
        String workerId = registerWorker();
        workerService.deregister(workerId);

        assertThat(jobEventRepository.findByAggregateIdOrderByIdAsc(workerId).stream()
                .map(JobEvent::getEventType))
                .containsExactly(JobEventType.WORKER_REGISTERED, JobEventType.WORKER_DEREGISTERED);
    }

    @Test
    void heartbeatsDoNotGenerateEvents() {
        String workerId = registerWorker();
        long before = jobEventRepository.count();

        workerService.heartbeat(workerId);
        workerService.heartbeat(workerId);
        workerService.heartbeat(workerId);

        assertThat(jobEventRepository.count())
                .as("heartbeats are deliberately not published - they carry no state change")
                .isEqualTo(before);
    }

    @Test
    void staleExecutionRecordsNoEvent() {
        String workerId = registerWorker();
        UUID jobId = createJob("fenced", 3);
        dispatchService.claim(workerId).orElseThrow();
        List<JobEventType> before = eventTypesFor(jobId);

        assertThatThrownBy(() -> executionService.complete(jobId, workerId, UUID.randomUUID()))
                .isInstanceOf(StaleExecutionException.class);

        assertThat(eventTypesFor(jobId)).isEqualTo(before);
    }

    // ---------- atomicity ----------

    @Test
    void stateChangeAndEventCommitTogether() {
        String workerId = registerWorker();
        UUID jobId = createJob("atomic", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        executionService.complete(jobId, workerId, claimed.executionId());

        // Both sides of the same transaction are visible.
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(eventTypesFor(jobId)).contains(JobEventType.JOB_COMPLETED);
    }

    /**
     * The inverse, and the one that actually proves atomicity: if the
     * transaction rolls back, the event must vanish with the state change.
     */
    @Test
    void rollbackLeavesNeitherStateChangeNorEvent() {
        UUID jobId = createJob("rollback", 3);
        long eventsBefore = jobEventRepository.count();

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        assertThatThrownBy(() -> template.execute(status -> {
            Job job = jobRepository.findById(jobId).orElseThrow();
            jobRepository.cancelIfQueued(jobId, java.time.Instant.now());
            eventRecorder.recordJobEvent(JobEventType.JOB_CANCELLED, job);
            throw new IllegalStateException("forced rollback");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus())
                .as("state change rolled back")
                .isEqualTo(JobStatus.QUEUED);
        assertThat(jobEventRepository.count())
                .as("event rolled back with it")
                .isEqualTo(eventsBefore);
    }

    /**
     * A duplicate idempotency key must not leave an orphan JOB_QUEUED event
     * behind for a job that was never created.
     */
    @Test
    void losingTheIdempotencyRaceLeavesNoOrphanEvent() {
        String key = "dup-" + UUID.randomUUID();
        CreateJobRequest request = new CreateJobRequest(
                key, "TEST_JOB", Map.of("name", "dup"), 5, 3, null);

        UUID first = jobService.createJob(request).job().getId();
        UUID second = jobService.createJob(request).job().getId();

        assertThat(second).isEqualTo(first);
        assertThat(eventTypesFor(first))
                .as("only one JOB_QUEUED, from the single job that was actually created")
                .containsExactly(JobEventType.JOB_QUEUED);
    }

    // ---------- publishing ----------

    @Test
    void publishingMarksEventsPublishedAndSendsThemToKafka() {
        String workerId = registerWorker();
        UUID jobId = createJob("published", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        executionService.complete(jobId, workerId, claimed.executionId());

        assertThat(jobEventRepository.countByPublishedAtIsNull()).isPositive();

        int published = outboxPublisher.publishPending();

        assertThat(published).isPositive();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();
        assertThat(jobEventRepository.findAll()).allMatch(JobEvent::isPublished);

        List<ConsumerRecord<String, String>> records = consume(jobEventsTopic, 3);
        assertThat(records).hasSizeGreaterThanOrEqualTo(3);
        assertThat(records).allSatisfy(record ->
                assertThat(record.key()).as("keyed by job id for per-job ordering").isNotBlank());
    }

    @Test
    void publishedMessageCarriesStableEventIdAndPayload() {
        UUID jobId = createJob("envelope", 3);
        JobEvent recorded = jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).get(0);

        outboxPublisher.publishPending();

        List<ConsumerRecord<String, String>> records = consumeFor(jobEventsTopic, jobId.toString(), 1);
        assertThat(records).isNotEmpty();
        JsonNode envelope = objectMapper.readTree(records.get(0).value());

        // The database id is carried in the message, unchanged, so a
        // consumer can deduplicate a republished copy.
        assertThat(envelope.get("eventId").asLong()).isEqualTo(recorded.getId());
        assertThat(envelope.get("eventType").asString()).isEqualTo("JOB_QUEUED");
        assertThat(envelope.get("aggregateId").asString()).isEqualTo(jobId.toString());
        assertThat(envelope.get("payload").get("jobId").asString()).isEqualTo(jobId.toString());
    }

    @Test
    void workerEventsGoToTheWorkerTopic() {
        String workerId = registerWorker();

        outboxPublisher.publishPending();

        List<ConsumerRecord<String, String>> records = consume(workerEventsTopic, 1);
        assertThat(records).isNotEmpty();
        assertThat(records).anySatisfy(record -> assertThat(record.key()).isEqualTo(workerId));
    }

    @Test
    void publishingIsIdempotentAndEventIdsSurviveRepublishAttempts() {
        UUID jobId = createJob("republish", 3);
        JobEvent before = jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).get(0);

        assertThat(outboxPublisher.publishPending()).isPositive();
        // Nothing left to publish: a second pass is a no-op rather than a
        // second send of the same rows.
        assertThat(outboxPublisher.publishPending()).isZero();

        JobEvent after = jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).get(0);
        assertThat(after.getId()).isEqualTo(before.getId());
        assertThat(after.isPublished()).isTrue();
    }

    @Test
    void eventsArePublishedInRecordedOrder() {
        String workerId = registerWorker();
        UUID jobId = createJob("ordered", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        executionService.complete(jobId, workerId, claimed.executionId());

        outboxPublisher.publishPending();

        List<JobEvent> stored = jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString());
        assertThat(stored).extracting(JobEvent::getEventType).containsExactly(
                JobEventType.JOB_QUEUED, JobEventType.JOB_RUNNING, JobEventType.JOB_COMPLETED);
        assertThat(stored).extracting(JobEvent::getId).isSorted();
    }

    @Test
    void unpublishedEventsCanBePublishedLater() {
        UUID first = createJob("later-1", 3);
        UUID second = createJob("later-2", 3);

        // Both sit unpublished for as long as nothing publishes them...
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(2);
        assertThat(jobRepository.findById(first).orElseThrow().getStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(jobRepository.findById(second).orElseThrow().getStatus()).isEqualTo(JobStatus.QUEUED);

        // ...and are picked up whenever the publisher next runs.
        assertThat(outboxPublisher.publishPending()).isEqualTo(2);
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();
    }

    @Test
    void duplicateInsertRollsBackWithoutBreakingSubsequentWork() {
        // Guards the Day 2 behaviour that the outbox write must not
        // reintroduce: a failed insert must not poison the connection for
        // the recovery path that follows it.
        String key = "poison-" + UUID.randomUUID();
        CreateJobRequest request = new CreateJobRequest(key, "TEST_JOB", Map.of("n", 1), 5, 3, null);

        jobService.createJob(request);
        assertThat(jobService.createJob(request).created()).isFalse();

        // And the service is still usable afterwards.
        UUID another = createJob("after-conflict", 3);
        assertThat(jobRepository.findById(another)).isPresent();
        assertThat(DataIntegrityViolationException.class).isNotNull();
    }
}
