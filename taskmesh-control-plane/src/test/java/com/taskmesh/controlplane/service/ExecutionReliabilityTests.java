package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.ExecutionReportRequest;
import com.taskmesh.common.worker.LeaseResponse;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobAttempt;
import com.taskmesh.controlplane.domain.JobAttemptStatus;
import com.taskmesh.controlplane.domain.JobStatus;
import com.taskmesh.controlplane.repository.JobAttemptRepository;
import com.taskmesh.controlplane.repository.JobRepository;

import tools.jackson.databind.ObjectMapper;

/**
 * Day 4 reliability: leases, renewal, fenced completion/failure, the lease
 * reaper, and crash recovery - against a real PostgreSQL, because every
 * guarantee here rests on PostgreSQL's row locking, conditional updates
 * and clock.
 * <p>
 * The scheduled reaper is switched off so that sweeps happen exactly when
 * a test asks for one; a background timer firing mid-test would make the
 * race tests assert against a moving target. {@code LeaseReaperScheduler}
 * is a thin wrapper around the service these tests call directly, and the
 * scheduled path is exercised in the Docker verification instead.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "taskmesh.reliability.reaper-enabled=false")
class ExecutionReliabilityTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

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
    private JobRepository jobRepository;

    @Autowired
    private JobAttemptRepository jobAttemptRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, workers CASCADE");
    }

    // ---------- helpers ----------

    private String registerWorker() {
        String workerId = "worker-" + UUID.randomUUID();
        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));
        return workerId;
    }

    private UUID createJob(String name) {
        return jobService.createJob(new CreateJobRequest(
                name + "-" + UUID.randomUUID(), "TEST_JOB", Map.of("name", name), 5, 3, null)).job().getId();
    }

    /** Expires a lease using the database's own clock, rather than waiting 30 real seconds. */
    private void expireLease(UUID jobId) {
        jdbcTemplate.update("UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = ?", jobId);
    }

    /**
     * Brings a job's retry backoff forward and promotes it, so a test can
     * reach the next attempt without waiting out a real delay. Day 5 sends
     * failed and lease-expired jobs to RETRYING rather than straight back
     * to QUEUED, so reassignment now goes through this step.
     */
    private void makeRetryClaimable(UUID jobId) {
        jdbcTemplate.update("UPDATE jobs SET scheduled_at = now() - interval '1 second' WHERE id = ?", jobId);
        leaseReaper.promoteDueRetries();
    }

    private Job job(UUID jobId) {
        return jobRepository.findById(jobId).orElseThrow();
    }

    private JobAttempt attempt(UUID executionId) {
        return jobAttemptRepository.findByExecutionId(executionId).orElseThrow();
    }

    // ---------- leases ----------

    @Test
    void claimingAJobCreatesALease() {
        String workerId = registerWorker();
        UUID jobId = createJob("leased");

        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        assertThat(claimed.leaseUntil()).isNotNull().isAfter(Instant.now());
        Job persisted = job(jobId);
        assertThat(persisted.getLeaseUntil()).isNotNull();
        assertThat(persisted.getStatus()).isEqualTo(JobStatus.RUNNING);
        // 30s lease, allowing generous slack for clock differences between JVM and database.
        assertThat(persisted.getLeaseUntil()).isBetween(Instant.now().plusSeconds(10), Instant.now().plusSeconds(60));
    }

    @Test
    void owningExecutionCanRenewItsLease() {
        String workerId = registerWorker();
        UUID jobId = createJob("renewable");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        Instant originalLease = job(jobId).getLeaseUntil();

        // Move the lease back so a successful renewal is unmistakably an extension.
        jdbcTemplate.update("UPDATE jobs SET lease_until = now() + interval '5 seconds' WHERE id = ?", jobId);
        Instant shortenedLease = job(jobId).getLeaseUntil();

        LeaseResponse response = executionService.renewLease(jobId, workerId, claimed.executionId());

        assertThat(response.leaseUntil()).isAfter(shortenedLease);
        assertThat(job(jobId).getLeaseUntil()).isAfter(shortenedLease);
        assertThat(originalLease).isNotNull();
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void anotherWorkerCannotRenewSomeoneElsesLease() {
        String owner = registerWorker();
        String impostor = registerWorker();
        UUID jobId = createJob("not-yours");
        ClaimedJob claimed = dispatchService.claim(owner).orElseThrow();
        Instant leaseBefore = job(jobId).getLeaseUntil();

        assertThatThrownBy(() -> executionService.renewLease(jobId, impostor, claimed.executionId()))
                .isInstanceOf(StaleExecutionException.class);

        assertThat(job(jobId).getLeaseUntil()).isEqualTo(leaseBefore);
        assertThat(job(jobId).getAssignedWorkerId()).isEqualTo(owner);
    }

    @Test
    void wrongExecutionIdCannotRenewLease() {
        String workerId = registerWorker();
        UUID jobId = createJob("wrong-exec");
        dispatchService.claim(workerId).orElseThrow();
        Instant leaseBefore = job(jobId).getLeaseUntil();

        assertThatThrownBy(() -> executionService.renewLease(jobId, workerId, UUID.randomUUID()))
                .isInstanceOf(StaleExecutionException.class);

        assertThat(job(jobId).getLeaseUntil()).isEqualTo(leaseBefore);
    }

    @Test
    void anAlreadyExpiredLeaseCannotBeRenewed() {
        String workerId = registerWorker();
        UUID jobId = createJob("too-late");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        expireLease(jobId);

        // Renewing after expiry must fail even though the reaper has not run
        // yet: the job is already fair game for reassignment at this point.
        assertThatThrownBy(() -> executionService.renewLease(jobId, workerId, claimed.executionId()))
                .isInstanceOf(StaleExecutionException.class);
    }

    // ---------- completion ----------

    @Test
    void owningExecutionCanCompleteAndAttemptBecomesSucceeded() {
        String workerId = registerWorker();
        UUID jobId = createJob("completable");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        executionService.complete(jobId, workerId, claimed.executionId());

        Job completed = job(jobId);
        assertThat(completed.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(completed.getCompletedAt()).isNotNull();
        assertThat(completed.getLeaseUntil()).isNull();
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.SUCCEEDED);
        assertThat(attempt(claimed.executionId()).getCompletedAt()).isNotNull();
    }

    @Test
    void wrongExecutionIdCannotCompleteJob() {
        String workerId = registerWorker();
        UUID jobId = createJob("fenced-complete");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        assertThatThrownBy(() -> executionService.complete(jobId, workerId, UUID.randomUUID()))
                .isInstanceOf(StaleExecutionException.class);

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.RUNNING);
    }

    @Test
    void staleCompletionOverHttpReturns409StaleExecution() throws Exception {
        String workerId = registerWorker();
        UUID jobId = createJob("http-stale");
        dispatchService.claim(workerId).orElseThrow();

        String body = objectMapper.writeValueAsString(
                new ExecutionReportRequest(workerId, UUID.randomUUID()));

        mockMvc.perform(post("/internal/jobs/{jobId}/complete", jobId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.error").value("STALE_EXECUTION"));

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void completionOverHttpSucceedsForTheOwningExecution() throws Exception {
        String workerId = registerWorker();
        UUID jobId = createJob("http-complete");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        String body = objectMapper.writeValueAsString(
                new ExecutionReportRequest(workerId, claimed.executionId()));

        mockMvc.perform(post("/internal/jobs/{jobId}/complete", jobId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isNoContent());

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.COMPLETED);
    }

    // ---------- failure ----------

    @Test
    void owningExecutionCanReportFailureAndJobIsScheduledForRetry() {
        String workerId = registerWorker();
        UUID jobId = createJob("failing");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        executionService.fail(jobId, workerId, claimed.executionId(), "boom");

        Job failed = job(jobId);
        // Day 5: a failure with attempts remaining waits out a backoff in
        // RETRYING instead of becoming immediately claimable again.
        assertThat(failed.getStatus()).isEqualTo(JobStatus.RETRYING);
        assertThat(failed.getScheduledAt()).isAfter(Instant.now());
        assertThat(failed.getAssignedWorkerId()).isNull();
        assertThat(failed.getCurrentExecutionId()).isNull();
        assertThat(failed.getLeaseUntil()).isNull();
        assertThat(failed.getLastFailureReason()).isEqualTo("boom");
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.FAILED);
        assertThat(attempt(claimed.executionId()).getFailureReason()).isEqualTo("boom");
    }

    @Test
    void staleFailureCannotModifyJobState() {
        String workerId = registerWorker();
        UUID jobId = createJob("stale-failure");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        assertThatThrownBy(() -> executionService.fail(jobId, workerId, UUID.randomUUID(), "not mine"))
                .isInstanceOf(StaleExecutionException.class);

        Job untouched = job(jobId);
        assertThat(untouched.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(untouched.getAssignedWorkerId()).isEqualTo(workerId);
        assertThat(untouched.getCurrentExecutionId()).isEqualTo(claimed.executionId());
        assertThat(untouched.getLastFailureReason()).isNull();
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.RUNNING);
    }

    // ---------- reaper and crash recovery ----------

    @Test
    void reaperDetectsExpiredLeaseAndMarksAttemptLeaseExpired() {
        String workerId = registerWorker();
        UUID jobId = createJob("abandoned");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        expireLease(jobId);

        int recovered = leaseReaper.reapExpiredLeases();

        assertThat(recovered).isEqualTo(1);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.LEASE_EXPIRED);
        assertThat(attempt(claimed.executionId()).getCompletedAt()).isNotNull();
    }

    @Test
    void reapedJobBecomesEligibleForReassignment() {
        String workerId = registerWorker();
        UUID jobId = createJob("recoverable");
        dispatchService.claim(workerId).orElseThrow();
        expireLease(jobId);

        leaseReaper.reapExpiredLeases();

        Job requeued = job(jobId);
        assertThat(requeued.getStatus()).isEqualTo(JobStatus.RETRYING);
        assertThat(requeued.getAssignedWorkerId()).isNull();
        assertThat(requeued.getCurrentExecutionId()).isNull();
        assertThat(requeued.getLeaseUntil()).isNull();

        // Claimable again once the backoff has elapsed.
        makeRetryClaimable(jobId);
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.QUEUED);
    }

    @Test
    void reaperDoesNotTouchJobsWhoseLeaseIsStillValid() {
        String workerId = registerWorker();
        UUID jobId = createJob("healthy");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        assertThat(leaseReaper.reapExpiredLeases()).isZero();

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.RUNNING);
    }

    @Test
    void reaperIsIdempotentAcrossRepeatedSweeps() {
        String workerId = registerWorker();
        UUID jobId = createJob("swept-twice");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        expireLease(jobId);

        assertThat(leaseReaper.reapExpiredLeases()).isEqualTo(1);
        // A second sweep must find nothing: the job is QUEUED again and the
        // attempt is already terminal, so nothing is double-processed.
        assertThat(leaseReaper.reapExpiredLeases()).isZero();
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId)).hasSize(1);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.LEASE_EXPIRED);
    }

    /**
     * The full crash-recovery story, and the core Day 4 guarantee: a worker
     * dies mid-job, the job is recovered and re-executed elsewhere, and the
     * dead worker can no longer affect it.
     */
    @Test
    void crashedWorkersJobIsReassignedAndTheOldExecutionIsFencedOut() {
        String crashedWorker = registerWorker();
        String rescuerWorker = registerWorker();
        UUID jobId = createJob("crash-recovery");

        ClaimedJob first = dispatchService.claim(crashedWorker).orElseThrow();
        expireLease(jobId);
        leaseReaper.reapExpiredLeases();
        makeRetryClaimable(jobId);

        // Another worker picks the job up, with a brand-new execution id.
        ClaimedJob second = dispatchService.claim(rescuerWorker).orElseThrow();
        assertThat(second.jobId()).isEqualTo(jobId);
        assertThat(second.executionId()).isNotEqualTo(first.executionId());
        assertThat(second.attemptNumber()).isEqualTo(2);
        assertThat(job(jobId).getAssignedWorkerId()).isEqualTo(rescuerWorker);

        // The crashed worker comes back and tries to report success. It must
        // not be able to overwrite the execution that replaced it.
        assertThatThrownBy(() -> executionService.complete(jobId, crashedWorker, first.executionId()))
                .isInstanceOf(StaleExecutionException.class);
        assertThatThrownBy(() -> executionService.fail(jobId, crashedWorker, first.executionId(), "late failure"))
                .isInstanceOf(StaleExecutionException.class);
        assertThatThrownBy(() -> executionService.renewLease(jobId, crashedWorker, first.executionId()))
                .isInstanceOf(StaleExecutionException.class);

        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(job(jobId).getCurrentExecutionId()).isEqualTo(second.executionId());
        assertThat(attempt(first.executionId()).getStatus()).isEqualTo(JobAttemptStatus.LEASE_EXPIRED);

        // And the new execution can still finish the job normally.
        executionService.complete(jobId, rescuerWorker, second.executionId());
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(attempt(second.executionId()).getStatus()).isEqualTo(JobAttemptStatus.SUCCEEDED);
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId)).hasSize(2);
    }

    // ---------- races ----------

    @Test
    void onlyOneWorkerCanClaimARequeuedJob() throws Exception {
        UUID jobId = createJob("contended-requeue");
        String firstHolder = registerWorker();
        dispatchService.claim(firstHolder).orElseThrow();
        expireLease(jobId);
        leaseReaper.reapExpiredLeases();
        makeRetryClaimable(jobId);

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
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RUNNING);
        // Attempt 1 expired, attempt 2 is the single winner of the race.
        assertThat(jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId)).hasSize(2);
    }

    /**
     * Completion racing the reaper. Whichever commits first wins and the
     * other becomes a no-op; what must never happen is a job that is both
     * COMPLETED and requeued, or whose status disagrees with its attempt.
     */
    @Test
    void completionRacingTheReaperLeavesConsistentState() throws Exception {
        for (int round = 0; round < 5; round++) {
            jdbcTemplate.execute("TRUNCATE jobs, job_attempts, workers CASCADE");
            String workerId = registerWorker();
            UUID jobId = createJob("race-" + round);
            ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
            expireLease(jobId);

            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch go = new CountDownLatch(1);
            AtomicReference<Boolean> completionSucceeded = new AtomicReference<>();

            Future<?> completion = pool.submit(() -> {
                await(go);
                try {
                    executionService.complete(jobId, workerId, claimed.executionId());
                    completionSucceeded.set(true);
                } catch (StaleExecutionException e) {
                    completionSucceeded.set(false);
                }
            });
            Future<?> reap = pool.submit(() -> {
                await(go);
                leaseReaper.reapExpiredLeases();
            });

            go.countDown();
            completion.get(30, TimeUnit.SECONDS);
            reap.get(30, TimeUnit.SECONDS);
            pool.shutdown();

            Job finalJob = job(jobId);
            JobAttempt finalAttempt = attempt(claimed.executionId());

            if (Boolean.TRUE.equals(completionSucceeded.get())) {
                // Completion won: the job is done and the reaper must not
                // have resurrected it.
                assertThat(finalJob.getStatus()).isEqualTo(JobStatus.COMPLETED);
                assertThat(finalAttempt.getStatus()).isEqualTo(JobAttemptStatus.SUCCEEDED);
                assertThat(finalJob.getCurrentExecutionId()).isEqualTo(claimed.executionId());
            } else {
                // The reaper won: the job was scheduled for another attempt
                // and the completion changed nothing.
                assertThat(finalJob.getStatus()).isEqualTo(JobStatus.RETRYING);
                assertThat(finalAttempt.getStatus()).isEqualTo(JobAttemptStatus.LEASE_EXPIRED);
                assertThat(finalJob.getCurrentExecutionId()).isNull();
            }
        }
    }

    /**
     * Renewal racing expiry. Unlike completion, renewal is refused outright
     * once the lease has lapsed, so the outcome is not a coin flip: the
     * renewal always loses and the job is always recovered.
     */
    @Test
    void renewalRacingExpiryAlwaysLosesToTheReaper() throws Exception {
        String workerId = registerWorker();
        UUID jobId = createJob("renew-vs-reap");
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        expireLease(jobId);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Boolean> renewalSucceeded = new AtomicReference<>();

        Future<?> renewal = pool.submit(() -> {
            await(go);
            try {
                executionService.renewLease(jobId, workerId, claimed.executionId());
                renewalSucceeded.set(true);
            } catch (StaleExecutionException e) {
                renewalSucceeded.set(false);
            }
        });
        Future<?> reap = pool.submit(() -> {
            await(go);
            leaseReaper.reapExpiredLeases();
        });

        go.countDown();
        renewal.get(30, TimeUnit.SECONDS);
        reap.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(renewalSucceeded.get()).isFalse();
        assertThat(job(jobId).getStatus()).isEqualTo(JobStatus.RETRYING);
        assertThat(attempt(claimed.executionId()).getStatus()).isEqualTo(JobAttemptStatus.LEASE_EXPIRED);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
