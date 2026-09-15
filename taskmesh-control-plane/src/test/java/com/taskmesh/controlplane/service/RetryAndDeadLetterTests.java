package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobAttempt;
import com.taskmesh.controlplane.domain.JobAttemptStatus;
import com.taskmesh.controlplane.domain.JobStatus;
import com.taskmesh.controlplane.repository.JobAttemptRepository;
import com.taskmesh.controlplane.repository.JobRepository;

/**
 * Day 5 retry policy, backoff and dead-lettering, against real PostgreSQL.
 * <p>
 * The reaper/promoter and the outbox publisher are both switched off so
 * that sweeps happen exactly when a test asks for one - the backoff
 * assertions in particular are meaningless if a background timer can
 * promote a job mid-assertion.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        // A 1s initial backoff keeps the "wait for the backoff to elapse"
        // tests fast while still being a real, observable delay.
        "taskmesh.retry.initial-backoff=1s",
        "taskmesh.retry.max-backoff=60s"
})
class RetryAndDeadLetterTests {

    @Autowired
    private JobService jobService;

    @Autowired
    private DispatchService dispatchService;

    @Autowired
    private ExecutionService executionService;

    @Autowired
    private LeaseReaperService leaseReaper;

    @Autowired
    private WorkerService workerService;

    @Autowired
    private RetryPolicy retryPolicy;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobAttemptRepository jobAttemptRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

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

    private Job job(UUID jobId) {
        return jobRepository.findById(jobId).orElseThrow();
    }

    private JobAttempt attempt(UUID executionId) {
        return jobAttemptRepository.findByExecutionId(executionId).orElseThrow();
    }

    /** Runs one claim-then-fail cycle, promoting past the backoff so the next attempt can start. */
    private ClaimedJob claimAndFail(UUID jobId, String workerId, String reason) {
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        executionService.fail(jobId, workerId, claimed.executionId(), reason);
        return claimed;
    }

    private void elapseBackoff(UUID jobId) {
        jdbcTemplate.update("UPDATE jobs SET scheduled_at = now() - interval '1 second' WHERE id = ?", jobId);
    }

    // ---------- backoff policy ----------

    @Test
    void backoffDoublesPerAttemptAndIsCapped() {
        assertThat(retryPolicy.backoffAfterAttempt(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(retryPolicy.backoffAfterAttempt(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(retryPolicy.backoffAfterAttempt(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(retryPolicy.backoffAfterAttempt(4)).isEqualTo(Duration.ofSeconds(8));
        // Capped rather than doubling forever.
        assertThat(retryPolicy.backoffAfterAttempt(30)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void attemptBudgetBoundaryIsExclusiveOfTheLastAttempt() {
        assertThat(retryPolicy.hasAttemptsRemaining(1, 3)).isTrue();
        assertThat(retryPolicy.hasAttemptsRemaining(2, 3)).isTrue();
        // Third attempt consumed: no fourth execution.
        assertThat(retryPolicy.hasAttemptsRemaining(3, 3)).isFalse();
        assertThat(retryPolicy.hasAttemptsRemaining(4, 3)).isFalse();
    }

    // ---------- retry flow ----------

    @Test
    void failureWithAttemptsRemainingMovesJobToRetryingWithFutureSchedule() {
        String workerId = registerWorker();
        UUID jobId = createJob("retryable", 3);
        Instant beforeFailure = Instant.now();

        ClaimedJob claimed = claimAndFail(jobId, workerId, "boom");

        Job retrying = job(jobId);
        assertThat(retrying.getStatus()).isEqualTo(JobStatus.RETRYING);
        assertThat(retrying.getScheduledAt()).isAfter(beforeFailure);
        assertThat(retrying.getAssignedWorkerId()).isNull();
        assertThat(retrying.getCurrentExecutionId()).isNull();
        assertThat(retrying.getLeaseUntil()).isNull();
        assertThat(retrying.getLastFailureReason()).isEqualTo("boom");
        assertThat(retrying.getAttemptCount()).isEqualTo(1);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.FAILED);
    }

    @Test
    void retryingJobIsNotClaimableBeforeItsBackoffElapses() {
        String workerId = registerWorker();
        UUID jobId = createJob("waiting", 3);
        claimAndFail(jobId, workerId, "boom");

        // Still in backoff: neither promotable nor claimable.
        assertThat(leaseReaper.promoteDueRetries()).isZero();
        assertThat(dispatchService.claim(workerId)).isEmpty();
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RETRYING);
    }

    @Test
    void retryingJobBecomesQueuedAndClaimableOnceBackoffElapses() {
        String workerId = registerWorker();
        UUID jobId = createJob("promotable", 3);
        ClaimedJob first = claimAndFail(jobId, workerId, "boom");

        elapseBackoff(jobId);
        assertThat(leaseReaper.promoteDueRetries()).isEqualTo(1);
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.QUEUED);

        ClaimedJob second = dispatchService.claim(workerId).orElseThrow();
        assertThat(second.jobId()).isEqualTo(jobId);
        assertThat(second.executionId()).isNotEqualTo(first.executionId());
        assertThat(second.attemptNumber()).isEqualTo(2);
        assertThat(job(jobId).getAttemptCount()).isEqualTo(2);
    }

    @Test
    void realBackoffDelayIsObservedWithoutManuallyAdjustingTheSchedule() throws Exception {
        String workerId = registerWorker();
        UUID jobId = createJob("real-wait", 3);
        claimAndFail(jobId, workerId, "boom");

        // 1s configured backoff: not due immediately...
        assertThat(leaseReaper.promoteDueRetries()).isZero();
        // ...but due after it actually elapses. This is the only test that
        // waits on real time, to prove the delay is not just bookkeeping.
        Thread.sleep(1500);
        assertThat(leaseReaper.promoteDueRetries()).isEqualTo(1);
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.QUEUED);
    }

    // ---------- dead letter ----------

    @Test
    void jobIsDeadLetteredAfterExactlyMaxAttemptsExecutions() {
        String workerId = registerWorker();
        UUID jobId = createJob("doomed", 3);

        // Attempt 1
        claimAndFail(jobId, workerId, "fail 1");
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RETRYING);
        elapseBackoff(jobId);
        leaseReaper.promoteDueRetries();

        // Attempt 2
        claimAndFail(jobId, workerId, "fail 2");
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RETRYING);
        elapseBackoff(jobId);
        leaseReaper.promoteDueRetries();

        // Attempt 3 - budget spent, no fourth execution
        claimAndFail(jobId, workerId, "fail 3");

        Job dead = job(jobId);
        assertThat(dead.getStatus()).isEqualTo(JobStatus.DEAD_LETTER);
        assertThat(dead.getAttemptCount()).isEqualTo(3);
        assertThat(dead.getLastFailureReason()).isEqualTo("fail 3");
        assertThat(dead.getCompletedAt()).isNotNull();
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId)).hasSize(3);
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId))
                .allMatch(a -> a.getStatus() == JobAttemptStatus.FAILED);
    }

    @Test
    void deadLetteredJobIsNotClaimable() {
        String workerId = registerWorker();
        UUID jobId = createJob("single-shot", 1);

        claimAndFail(jobId, workerId, "only chance");
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.DEAD_LETTER);

        // Not claimable, and not promotable either - it is terminal.
        assertThat(dispatchService.claim(workerId)).isEmpty();
        assertThat(leaseReaper.promoteDueRetries()).isZero();
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.DEAD_LETTER);
    }

    @Test
    void maxAttemptsOfOneMeansNoRetryAtAll() {
        String workerId = registerWorker();
        UUID jobId = createJob("no-retries", 1);

        claimAndFail(jobId, workerId, "boom");

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.DEAD_LETTER);
        assertThat(job(jobId).getAttemptCount()).isEqualTo(1);
    }

    // ---------- fencing still holds ----------

    @Test
    void staleExecutionCannotTriggerARetryOrDeadLetter() {
        String workerId = registerWorker();
        UUID jobId = createJob("fenced", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        assertThatThrownBy(() -> executionService.fail(jobId, workerId, UUID.randomUUID(), "not mine"))
                .isInstanceOf(StaleExecutionException.class);

        Job untouched = job(jobId);
        assertThat(untouched.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(untouched.getAttemptCount()).isEqualTo(1);
        assertThat(untouched.getLastFailureReason()).isNull();
        assertThat(untouched.getScheduledAt()).isBefore(Instant.now().plusSeconds(1));
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.RUNNING);
    }

    @Test
    void staleExecutionCannotDeadLetterAJobOnItsFinalAttempt() {
        String workerId = registerWorker();
        UUID jobId = createJob("final-attempt", 1);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        assertThatThrownBy(() -> executionService.fail(jobId, workerId, UUID.randomUUID(), "not mine"))
                .isInstanceOf(StaleExecutionException.class);

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.RUNNING);
    }

    // ---------- lease expiry participates in the same policy ----------

    @Test
    void leaseExpiryConsumesAnAttemptAndSchedulesARetry() {
        String workerId = registerWorker();
        UUID jobId = createJob("abandoned", 3);
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        jdbcTemplate.update("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = ?", jobId);

        assertThat(leaseReaper.reapExpiredLeases()).isEqualTo(1);

        Job retrying = job(jobId);
        assertThat(retrying.getStatus()).isEqualTo(JobStatus.RETRYING);
        assertThat(retrying.getScheduledAt()).isAfter(Instant.now());
        assertThat(retrying.getAttemptCount()).isEqualTo(1);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.LEASE_EXPIRED);
    }

    /**
     * A job whose worker keeps dying must still terminate. Without applying
     * the retry budget to lease expiry, this would be an infinite loop of
     * claim-crash-requeue.
     */
    @Test
    void repeatedLeaseExpiryEventuallyDeadLettersRatherThanLoopingForever() {
        String workerId = registerWorker();
        UUID jobId = createJob("crash-loop", 2);

        for (int attemptNo = 1; attemptNo <= 2; attemptNo++) {
            dispatchService.claim(workerId).orElseThrow();
            jdbcTemplate.update("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = ?", jobId);
            leaseReaper.reapExpiredLeases();
            if (attemptNo < 2) {
                elapseBackoff(jobId);
                leaseReaper.promoteDueRetries();
            }
        }

        Job dead = job(jobId);
        assertThat(dead.getStatus()).isEqualTo(JobStatus.DEAD_LETTER);
        assertThat(dead.getAttemptCount()).isEqualTo(2);
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId))
                .hasSize(2)
                .allMatch(a -> a.getStatus() == JobAttemptStatus.LEASE_EXPIRED);
    }

    // ---------- concurrency ----------

    /**
     * Only one worker may pick up a promoted retry, and it must consume
     * exactly one attempt - a promoted job that several workers grabbed at
     * once would burn the retry budget several times over.
     */
    @Test
    void concurrentClaimsOnAPromotedRetryConsumeExactlyOneAttempt() throws Exception {
        String firstWorker = registerWorker();
        UUID jobId = createJob("contended-retry", 5);
        claimAndFail(jobId, firstWorker, "boom");
        elapseBackoff(jobId);
        leaseReaper.promoteDueRetries();

        int contenders = 8;
        List<String> workerIds = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            workerIds.add(registerWorker());
        }

        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch go = new CountDownLatch(1);
        List<ClaimedJob> claims = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (String workerId : workerIds) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                dispatchService.claim(workerId).ifPresent(claims::add);
            }));
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(claims).hasSize(1);
        assertThat(job(jobId).getAttemptCount()).isEqualTo(2);
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId)).hasSize(2);
    }

    /**
     * Concurrent promotion sweeps must not promote the same job twice, or a
     * job could be counted into the queue more than once.
     */
    @Test
    void concurrentPromotionSweepsPromoteEachJobOnce() throws Exception {
        String workerId = registerWorker();
        List<UUID> jobIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UUID jobId = createJob("promote-race-" + i, 3);
            jobIds.add(jobId);
        }
        for (UUID jobId : jobIds) {
            ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
            executionService.fail(claimed.jobId(), workerId, claimed.executionId(), "boom");
            elapseBackoff(claimed.jobId());
        }

        int sweepers = 4;
        ExecutorService pool = Executors.newFixedThreadPool(sweepers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger totalPromoted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < sweepers; i++) {
            futures.add(pool.submit(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                totalPromoted.addAndGet(leaseReaper.promoteDueRetries());
            }));
        }
        go.countDown();
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Five jobs were due; across all concurrent sweeps each was
        // promoted exactly once.
        assertThat(totalPromoted.get()).isEqualTo(5);
        assertThat(jobRepository.findAll()).allMatch(j -> j.getStatus() == JobStatus.QUEUED);
    }
}
