package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
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
 * Day 20: the failure semantics of {@code taskmesh.outbox.async-sends=true},
 * which submits a whole batch to the producer before collecting
 * acknowledgements instead of awaiting each send in turn.
 * <p>
 * The risk that change introduces is precisely that a failed send might be
 * marked published anyway - the futures are collected in bulk, so it would be
 * easy to write code that marks the batch rather than the acknowledged events.
 * These tests pin the per-event behaviour instead.
 * <p>
 * The failure injected is a <em>partial</em> one, which the existing
 * {@link OutboxKafkaOutageTests} cannot produce: that class stops a broker, so
 * every send fails. Here the broker is healthy and one record is simply larger
 * than {@code max.request.size}, so the producer rejects that record and
 * accepts its neighbours. That is the case where sequential and batched
 * publishing genuinely differ, and it is the case worth pinning.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.DatabaseAndKafka.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.outbox.async-sends=true",
        // Small enough that one deliberately fat event exceeds it while
        // ordinary events do not.
        "spring.kafka.producer.properties.max.request.size=2048"
})
class OutboxAsyncSendTests {

    /** Comfortably over max.request.size once serialised into the envelope. */
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
    void anEventWhoseSendFailsIsNotMarkedPublished() {
        Long tooBig = record(OVERSIZED);

        assertThat(outboxPublisher.publishPending())
                .as("the rejected event must not be counted as published")
                .isZero();
        assertThat(isPublished(tooBig)).isFalse();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(1);
    }

    @Test
    void healthyEventsInTheSameBatchStillPublishAroundAFailedOne() {
        Long before = record("small-before");
        Long tooBig = record(OVERSIZED);
        Long after = record("small-after");

        // Two of the three are acknowledged, so two are marked published.
        assertThat(outboxPublisher.publishPending()).isEqualTo(2);

        assertThat(isPublished(before)).isTrue();
        assertThat(isPublished(after))
                .as("batched publishing attempts every event, so one after the "
                        + "failure is still sent - this is the documented difference "
                        + "from the sequential path, which stops at the first failure")
                .isTrue();
        assertThat(isPublished(tooBig)).isFalse();
    }

    @Test
    void aFailedEventStaysEligibleAndIsRetriedOnTheNextPass() {
        Long tooBig = record(OVERSIZED);
        Long healthy = record("small");

        assertThat(outboxPublisher.publishPending()).isEqualTo(1);
        assertThat(isPublished(healthy)).isTrue();
        assertThat(isPublished(tooBig)).isFalse();

        // The row is still claimable: a second pass picks it up and fails on it
        // again rather than dropping or silently publishing it.
        assertThat(outboxPublisher.publishPending()).isZero();
        assertThat(isPublished(tooBig)).isFalse();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(1);

        // And once the cause is gone the very same row publishes. The payload is
        // shrunk in place rather than re-inserted so the id - the thing a
        // consumer deduplicates on - is demonstrably unchanged across the
        // failure and the eventual success.
        jdbcTemplate.update("update job_events set payload = ?::jsonb where id = ?",
                "{\"note\":\"small-now\"}", tooBig);

        assertThat(outboxPublisher.publishPending()).isEqualTo(1);
        assertThat(isPublished(tooBig)).isTrue();
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();
    }

    @Test
    void anAlreadyPublishedEventIsNeverRevertedByALaterFailureInTheSameBatch() {
        Long healthy = record("small");
        assertThat(outboxPublisher.publishPending()).isEqualTo(1);
        assertThat(isPublished(healthy)).isTrue();

        java.sql.Timestamp publishedAt = jdbcTemplate.queryForObject(
                "select published_at from job_events where id = ?", java.sql.Timestamp.class, healthy);

        // A failing event arriving afterwards must not disturb the earlier one.
        record(OVERSIZED);
        assertThat(outboxPublisher.publishPending()).isZero();

        assertThat(isPublished(healthy)).isTrue();
        assertThat(jdbcTemplate.queryForObject(
                "select published_at from job_events where id = ?", java.sql.Timestamp.class, healthy))
                .as("a successful publication is not re-stamped or rolled back")
                .isEqualTo(publishedAt);
    }

    @Test
    void everyEventInAHealthyBatchIsPublishedExactlyOnce() {
        List<Long> ids = List.of(record("a"), record("b"), record("c"), record("d"));

        assertThat(outboxPublisher.publishPending()).isEqualTo(4);
        assertThat(ids).allMatch(this::isPublished);
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();

        // A further pass has nothing to do - markPublished is guarded on
        // published_at IS NULL, so nothing is republished from the database side.
        assertThat(outboxPublisher.publishPending()).isZero();
    }
}
