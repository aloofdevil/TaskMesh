package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
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
 * Day 22: {@code published_at} identifies the publishing pass.
 * <p>
 * {@code JobEventRepository.markPublished} sets {@code published_at = now()},
 * and PostgreSQL fixes {@code now()} at <em>transaction start</em> — so every
 * event marked by one pass carries one identical timestamp, and two passes
 * carry different ones.
 * <p>
 * That is what let Day 22 reconstruct publisher-pass structure from production
 * data alone: which events each pass claimed, how many distinct keys it
 * carried, and whether passes committed out of id order — the evidence for the
 * {@code SKIP LOCKED} race behind per-key ordering loss at
 * {@code publisher-concurrency > 1}, gathered without instrumenting anything.
 * <p>
 * Swapping {@code now()} for {@code clock_timestamp()} (which advances
 * <em>within</em> a transaction) would silently invalidate that whole
 * technique while changing nothing a user could see. This test exists so that
 * change cannot pass unnoticed, and is the only test Day 22 adds.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        // Its own topic: this class shares a broker with OutboxTests, whose
        // consumer counts every record on the topic before filtering by key, so
        // foreign records would make it miss its own.
        "taskmesh.outbox.job-events-topic=taskmesh.job-events.pass-identity"
})
class OutboxPassIdentityTests {

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

    private void record(String key, int seq) {
        jobEventRepository.save(JobEvent.record(key, JobEventType.JOB_QUEUED,
                Map.<String, Object>of("seq", seq)));
    }

    private List<Timestamp> publishedAtValues() {
        return jdbcTemplate.queryForList(
                "select published_at from job_events where published_at is not null order by id",
                Timestamp.class);
    }

    @Test
    void everyEventPublishedByOnePassSharesOnePublishedAtValue() {
        String key = "pass-" + UUID.randomUUID().toString().substring(0, 8);
        for (int seq = 1; seq <= 10; seq++) {
            record(key, seq);
        }

        assertThat(outboxPublisher.publishPending()).isEqualTo(10);

        assertThat(publishedAtValues())
                .as("published_at comes from now(), which is constant across a transaction, "
                        + "so one pass stamps one value - this is what makes it a pass id")
                .hasSize(10)
                .containsOnly(publishedAtValues().get(0));
    }

    @Test
    void separatePassesAreDistinguishableByPublishedAt() {
        String key = "pass-" + UUID.randomUUID().toString().substring(0, 8);
        for (int seq = 1; seq <= 5; seq++) {
            record(key, seq);
        }
        assertThat(outboxPublisher.publishPending()).isEqualTo(5);
        Timestamp firstPass = publishedAtValues().get(0);

        for (int seq = 6; seq <= 10; seq++) {
            record(key, seq);
        }
        assertThat(outboxPublisher.publishPending()).isEqualTo(5);

        List<Timestamp> all = publishedAtValues();
        assertThat(all).hasSize(10);
        assertThat(all.subList(0, 5)).containsOnly(firstPass);
        assertThat(all.subList(5, 10))
                .as("a later pass stamps its own distinct value, so passes can be told apart")
                .doesNotContain(firstPass)
                .containsOnly(all.get(5));
    }
}
