package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.UUID;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.domain.JobStatus;
import com.taskmesh.controlplane.repository.JobEventRepository;
import com.taskmesh.controlplane.repository.JobRepository;

/**
 * What happens to the job lifecycle when Kafka cannot be reached.
 * <p>
 * This context gets its own Kafka broker, which is then stopped before the
 * tests run. Setting {@code spring.kafka.bootstrap-servers} to a dead port
 * would not work: {@code @ServiceConnection} supplies the address through a
 * connection-details bean that takes precedence over the property, so the
 * client would quietly keep talking to the live shared broker. Stopping a
 * real broker leaves a real address with nothing behind it, which is what
 * an outage actually looks like.
 * <p>
 * Recovery once Kafka returns is verified end-to-end against Docker, since
 * that needs a broker that genuinely comes back.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.DatabaseAndRedis.class, OutboxKafkaOutageTests.DedicatedKafka.class})
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        // Fail quickly rather than spending the default 60s per attempt.
        "spring.kafka.producer.properties.max.block.ms=2000",
        "taskmesh.outbox.send-timeout-ms=3000"
})
class OutboxKafkaOutageTests {

    /** A broker of this context's own, so stopping it affects no other test. */
    @TestConfiguration(proxyBeanMethods = false)
    static class DedicatedKafka {

        @Bean
        @ServiceConnection
        KafkaContainer outageKafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"));
        }
    }

    /** Stopped in {@link #resetState()}, after Spring has read its address. */
    @Autowired
    private KafkaContainer kafkaContainer;

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
    private JobRepository jobRepository;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
        if (kafkaContainer.isRunning()) {
            kafkaContainer.stop();
        }
    }

    @Test
    void theJobLifecycleKeepsWorkingWhileKafkaIsUnreachable() {
        String workerId = "worker-" + UUID.randomUUID();
        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));

        UUID jobId = jobService.createJob(new CreateJobRequest(
                "outage-" + UUID.randomUUID(), "TEST_JOB", Map.of("n", 1), 5, 3, null)).job().getId();
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        executionService.complete(jobId, workerId, claimed.executionId());

        // PostgreSQL is untouched by Kafka being down, because nothing on
        // these paths talks to Kafka at all.
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()))
                .extracting(JobEvent::getEventType)
                .containsExactly(JobEventType.JOB_QUEUED, JobEventType.JOB_RUNNING, JobEventType.JOB_COMPLETED);
    }

    @Test
    void failedPublicationLeavesEventsUnpublishedAndRetryable() {
        UUID jobId = jobService.createJob(new CreateJobRequest(
                "unpublishable-" + UUID.randomUUID(), "TEST_JOB", Map.of("n", 1), 5, 3, null)).job().getId();

        long pendingBefore = jobEventRepository.countByPublishedAtIsNull();
        assertThat(pendingBefore).isPositive();

        // The publisher runs and gets nowhere - but reports honestly
        // rather than marking anything published.
        assertThat(outboxPublisher.publishPending()).isZero();

        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(pendingBefore);
        assertThat(jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()))
                .allMatch(event -> !event.isPublished());

        // Retrying changes nothing while Kafka stays down, and loses nothing.
        assertThat(outboxPublisher.publishPending()).isZero();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(pendingBefore);
    }

    @Test
    void eventIdsAreStableAcrossFailedPublishAttempts() {
        UUID jobId = jobService.createJob(new CreateJobRequest(
                "stable-id-" + UUID.randomUUID(), "TEST_JOB", Map.of("n", 1), 5, 3, null)).job().getId();
        Long idBefore = jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).get(0).getId();

        outboxPublisher.publishPending();
        outboxPublisher.publishPending();

        Long idAfter = jobEventRepository.findByAggregateIdOrderByIdAsc(jobId.toString()).get(0).getId();
        assertThat(idAfter)
                .as("the id a consumer would deduplicate on does not change between attempts")
                .isEqualTo(idBefore);
    }
}
