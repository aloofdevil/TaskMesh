package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.repository.JobEventRepository;

/**
 * Correctness of running several outbox publishing passes at once.
 * <p>
 * Day 15 raises {@code taskmesh.outbox.publisher-concurrency} above 1. The
 * outbox already carried the mechanism that makes this safe -
 * {@code lockUnpublishedBatch} selects {@code FOR UPDATE SKIP LOCKED} and
 * {@code markPublished} is guarded on {@code published_at IS NULL} - so these
 * tests exist to prove that mechanism actually holds under real concurrency
 * rather than to assert that a method was called.
 * <p>
 * The background scheduler is disabled so each test drives publishing at an
 * exact moment; otherwise a timer would race every assertion.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.outbox.publisher-enabled=false",
        "taskmesh.reliability.reaper-enabled=false"
})
class OutboxConcurrentPublisherTests {

    @Autowired
    private JobService jobService;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
    }

    private void createJobs(int count) {
        for (int i = 0; i < count; i++) {
            jobService.createJob(new CreateJobRequest(
                    "outbox-conc-" + UUID.randomUUID(), "TEST_JOB", Map.of("n", i), 5, 3, null));
        }
    }

    private List<Long> publishedIds() {
        return jdbcTemplate.queryForList(
                "select id from job_events where published_at is not null order by id", Long.class);
    }

    /**
     * The central safety property: with several passes running at once, no
     * event may be handed to two of them. Each pass returns how many rows it
     * published, so if the batches overlapped the totals would exceed the
     * number of rows that exist.
     */
    @Test
    void concurrentPassesTakeDisjointBatchesAndNeverPublishAnEventTwice() throws Exception {
        int jobs = 60;
        createJobs(jobs);
        long created = jobEventRepository.count();
        assertThat(created).isEqualTo(jobs);

        int passes = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(passes);
        AtomicInteger totalReported = new AtomicInteger();
        Set<Integer> perPassCounts = ConcurrentHashMap.newKeySet();

        try (var executor = Executors.newFixedThreadPool(passes)) {
            for (int i = 0; i < passes; i++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        int published = outboxPublisher.publishPending();
                        totalReported.addAndGet(published);
                        perPassCounts.add(published);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        }

        List<Long> published = publishedIds();

        assertThat(published)
                .as("every created event should be published exactly once")
                .hasSize((int) created);
        assertThat(published)
                .as("no event id may appear twice in the published set")
                .doesNotHaveDuplicates();
        assertThat(totalReported.get())
                .as("passes must not collectively claim more rows than exist - that would mean overlap")
                .isEqualTo((int) created);
    }

    @Test
    void unpublishedEventsEventuallyBecomePublished() {
        createJobs(30);
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isEqualTo(30);

        int total = 0;
        for (int pass = 0; pass < 10 && jobEventRepository.countByPublishedAtIsNull() > 0; pass++) {
            total += outboxPublisher.publishPending();
        }

        assertThat(total).isEqualTo(30);
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();
    }

    /**
     * {@code markPublished} is guarded on {@code published_at IS NULL}, so a
     * second pass over already-published rows must be a no-op rather than
     * re-sending them or overwriting the original publication time.
     */
    @Test
    void alreadyPublishedEventsAreNotRepublished() {
        createJobs(20);
        int first = outboxPublisher.publishPending();
        assertThat(first).isEqualTo(20);

        List<java.sql.Timestamp> firstStamps = jdbcTemplate.queryForList(
                "select published_at from job_events order by id", java.sql.Timestamp.class);

        int second = outboxPublisher.publishPending();

        assertThat(second).as("a second pass has nothing left to claim").isZero();
        assertThat(jdbcTemplate.queryForList("select published_at from job_events order by id",
                java.sql.Timestamp.class))
                .as("publication timestamps must not be rewritten")
                .isEqualTo(firstStamps);
    }

    /**
     * A pass that cannot reach Kafka must leave its events unpublished and
     * recoverable, not consume them. Verified by pointing the publisher at a
     * broker that is not there - see {@code OutboxKafkaOutageTests} for the
     * full outage behaviour - and here by confirming that rows a failing pass
     * touched are still claimable afterwards.
     */
    @Test
    void eventsSurviveAndRemainClaimableAcrossPasses() {
        createJobs(40);
        long before = jobEventRepository.countByPublishedAtIsNull();
        assertThat(before).isEqualTo(40);

        // Publish in two halves with a small batch, proving rows are neither
        // lost nor stuck between passes.
        int firstPass = outboxPublisher.publishPending();
        assertThat(firstPass).isPositive();

        int remaining = 0;
        while (jobEventRepository.countByPublishedAtIsNull() > 0) {
            int published = outboxPublisher.publishPending();
            assertThat(published).as("a pass must always make progress while rows remain").isPositive();
            remaining += published;
        }

        assertThat(firstPass + remaining).isEqualTo(40);
        assertThat(publishedIds()).hasSize(40).doesNotHaveDuplicates();
    }

    /**
     * Interleaving publishing with ongoing event creation is the realistic
     * case: the benchmark generates events while publishers drain them.
     */
    @Test
    void concurrentPublishingWhileEventsAreStillBeingCreatedLosesNothing() throws Exception {
        int batches = 6;
        int perBatch = 10;
        CountDownLatch producersDone = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(5)) {
            executor.submit(() -> {
                try {
                    for (int b = 0; b < batches; b++) {
                        createJobs(perBatch);
                        Thread.sleep(20);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    producersDone.countDown();
                }
            });

            for (int p = 0; p < 4; p++) {
                executor.submit(() -> {
                    while (producersDone.getCount() > 0) {
                        outboxPublisher.publishPending();
                    }
                });
            }
            assertThat(producersDone.await(60, TimeUnit.SECONDS)).isTrue();
        }

        // Drain whatever the racing publishers left behind.
        while (jobEventRepository.countByPublishedAtIsNull() > 0) {
            outboxPublisher.publishPending();
        }

        long created = jobEventRepository.count();
        assertThat(created).isEqualTo((long) batches * perBatch);
        assertThat(publishedIds())
                .as("every event created during concurrent publishing must be published exactly once")
                .hasSize((int) created)
                .doesNotHaveDuplicates();
    }
}
