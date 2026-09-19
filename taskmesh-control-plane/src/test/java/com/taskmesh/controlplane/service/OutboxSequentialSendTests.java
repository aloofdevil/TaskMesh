package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobEventRepository;

/**
 * The other half of the Day 20 comparison: the same partial failure against the
 * default sequential path ({@code taskmesh.outbox.async-sends=false}).
 * <p>
 * This exists so the behavioural difference between the two modes is a tested
 * fact rather than a claim in a document. The sequential path stops at the first
 * failure, so the events it marks published are always a prefix of the batch in
 * id order; {@link OutboxAsyncSendTests#healthyEventsInTheSameBatchStillPublishAroundAFailedOne()}
 * pins the batched path publishing past the same failure.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.DatabaseAndKafka.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.outbox.async-sends=false",
        "spring.kafka.producer.properties.max.request.size=2048"
})
class OutboxSequentialSendTests {

    private static final String OVERSIZED = "x".repeat(8192);

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
    }

    private Long record(String note) {
        return jobEventRepository.save(JobEvent.record("agg-" + UUID.randomUUID(),
                JobEventType.JOB_QUEUED, Map.of("note", note))).getId();
    }

    private boolean isPublished(Long id) {
        return jobEventRepository.findById(id).orElseThrow().isPublished();
    }

    @Test
    void sequentialPublishingStopsAtTheFirstFailure() {
        Long before = record("small-before");
        Long tooBig = record(OVERSIZED);
        Long after = record("small-after");

        assertThat(outboxPublisher.publishPending())
                .as("only the events ahead of the failure are published")
                .isEqualTo(1);

        assertThat(isPublished(before)).isTrue();
        assertThat(isPublished(tooBig)).isFalse();
        assertThat(isPublished(after))
                .as("the sequential path never attempts an event after a failure, "
                        + "so what it publishes is always a prefix of the batch")
                .isFalse();
    }

    @Test
    void theEventsSkippedAfterAFailureAreNotLost() {
        record("small-before");
        Long tooBig = record(OVERSIZED);
        Long after = record("small-after");

        outboxPublisher.publishPending();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(2);

        // Once the blocking event can be sent, the pass proceeds past it and
        // the skipped event publishes too.
        jdbcTemplate.update("update job_events set payload = ?::jsonb where id = ?",
                "{\"note\":\"small-now\"}", tooBig);

        assertThat(outboxPublisher.publishPending()).isEqualTo(2);
        assertThat(isPublished(tooBig)).isTrue();
        assertThat(isPublished(after)).isTrue();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();
    }
}
