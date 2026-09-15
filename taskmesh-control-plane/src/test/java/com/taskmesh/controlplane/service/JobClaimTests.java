package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

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
 * Atomic job claiming against a real PostgreSQL (Testcontainers). The
 * database is not mocked anywhere here: the whole point of these tests is
 * that {@code FOR UPDATE SKIP LOCKED} behaves the way the design claims,
 * which only a real database can demonstrate.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class JobClaimTests {

    @Autowired
    private JobService jobService;

    @Autowired
    private DispatchService dispatchService;

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

    private String registerWorker() {
        String workerId = "worker-" + UUID.randomUUID();
        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));
        return workerId;
    }

    private UUID createJob(String name, int priority, Instant scheduledAt) {
        CreateJobRequest request = new CreateJobRequest(
                name + "-" + UUID.randomUUID(), "TEST_JOB", Map.of("name", name), priority, 3, scheduledAt);
        return jobService.createJob(request).job().getId();
    }

    private UUID createJob(String name, int priority) {
        return createJob(name, priority, null);
    }

    // ---------- basic claiming ----------

    @Test
    void claimingMovesJobFromQueuedToRunningAndAssignsTheWorker() {
        String workerId = registerWorker();
        UUID jobId = createJob("solo", 5);

        Optional<ClaimedJob> claimed = dispatchService.claim(workerId);

        assertThat(claimed).isPresent();
        assertThat(claimed.get().jobId()).isEqualTo(jobId);
        assertThat(claimed.get().type()).isEqualTo("TEST_JOB");
        assertThat(claimed.get().payload()).containsEntry("name", "solo");
        assertThat(claimed.get().attemptNumber()).isEqualTo(1);
        assertThat(claimed.get().executionId()).isNotNull();

        Job persisted = jobRepository.findById(jobId).orElseThrow();
        assertThat(persisted.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(persisted.getAssignedWorkerId()).isEqualTo(workerId);
        assertThat(persisted.getCurrentExecutionId()).isEqualTo(claimed.get().executionId());
        assertThat(persisted.getAttemptCount()).isEqualTo(1);
        assertThat(persisted.getStartedAt()).isNotNull();
    }

    @Test
    void claimingCreatesARunningAttemptRecord() {
        String workerId = registerWorker();
        UUID jobId = createJob("with-attempt", 5);

        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        List<JobAttempt> attempts = jobAttemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId);
        assertThat(attempts).hasSize(1);
        JobAttempt attempt = attempts.get(0);
        assertThat(attempt.getAttemptNumber()).isEqualTo(1);
        assertThat(attempt.getWorkerId()).isEqualTo(workerId);
        assertThat(attempt.getExecutionId()).isEqualTo(claimed.executionId());
        assertThat(attempt.getStatus()).isEqualTo(JobAttemptStatus.RUNNING);
    }

    @Test
    void aClaimedJobIsNoLongerAvailableToAnotherWorker() {
        String workerA = registerWorker();
        String workerB = registerWorker();
        UUID jobId = createJob("only-one", 5);

        Optional<ClaimedJob> first = dispatchService.claim(workerA);
        Optional<ClaimedJob> second = dispatchService.claim(workerB);

        assertThat(first).isPresent();
        assertThat(first.get().jobId()).isEqualTo(jobId);
        assertThat(second).isEmpty();

        assertThat(jobRepository.findById(jobId).orElseThrow().getAssignedWorkerId()).isEqualTo(workerA);
    }

    @Test
    void claimReturnsEmptyWhenThereIsNoWork() {
        String workerId = registerWorker();

        assertThat(dispatchService.claim(workerId)).isEmpty();
    }

    // ---------- eligibility ----------

    @Test
    void jobScheduledInTheFutureIsNotClaimed() {
        String workerId = registerWorker();
        UUID futureJob = createJob("future", 9, Instant.now().plus(1, ChronoUnit.HOURS));

        assertThat(dispatchService.claim(workerId)).isEmpty();
        assertThat(jobRepository.findById(futureJob).orElseThrow().getStatus()).isEqualTo(JobStatus.QUEUED);
    }

    @Test
    void cancelledJobIsNotClaimed() {
        String workerId = registerWorker();
        UUID jobId = createJob("cancelled", 9);
        jobService.cancelJob(jobId);

        assertThat(dispatchService.claim(workerId)).isEmpty();
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.CANCELLED);
    }

    // ---------- ordering ----------

    @Test
    void higherPriorityJobIsClaimedFirst() {
        String workerId = registerWorker();
        UUID lowPriority = createJob("low", 1);
        UUID highPriority = createJob("high", 10);

        assertThat(dispatchService.claim(workerId).orElseThrow().jobId()).isEqualTo(highPriority);
        assertThat(dispatchService.claim(workerId).orElseThrow().jobId()).isEqualTo(lowPriority);
    }

    @Test
    void atEqualPriorityTheEarlierScheduledJobIsClaimedFirst() {
        String workerId = registerWorker();
        Instant now = Instant.now();
        UUID later = createJob("later", 5, now.minus(1, ChronoUnit.MINUTES));
        UUID earlier = createJob("earlier", 5, now.minus(10, ChronoUnit.MINUTES));

        assertThat(dispatchService.claim(workerId).orElseThrow().jobId()).isEqualTo(earlier);
        assertThat(dispatchService.claim(workerId).orElseThrow().jobId()).isEqualTo(later);
    }

    // ---------- worker validation ----------

    @Test
    void deregisteredWorkerCannotClaim() {
        String workerId = registerWorker();
        createJob("unclaimable", 5);
        workerService.deregister(workerId);

        assertThatThrownBy(() -> dispatchService.claim(workerId))
                .isInstanceOf(WorkerNotActiveException.class);

        assertThat(jobRepository.findAll()).allMatch(job -> job.getStatus() == JobStatus.QUEUED);
    }

    @Test
    void unknownWorkerCannotClaim() {
        createJob("unclaimable", 5);

        assertThatThrownBy(() -> dispatchService.claim("never-registered"))
                .isInstanceOf(WorkerNotFoundException.class);
    }

    // ---------- the concurrency proof ----------

    /**
     * The central Day 3 guarantee: when more workers than jobs all claim at
     * the same instant, every job goes to exactly one worker and no job is
     * handed out twice.
     * <p>
     * Eight threads are released simultaneously against five queued jobs.
     * Without {@code SKIP LOCKED} the concurrent claimers would either
     * serialise behind the same row or read the same row before any of them
     * committed, and two workers would walk away with the same job.
     */
    @Test
    void concurrentWorkersNeverClaimTheSameJobTwice() throws Exception {
        int jobCount = 5;
        int workerCount = 8;

        List<UUID> jobIds = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            jobIds.add(createJob("concurrent-" + i, 5));
        }
        List<String> workerIds = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            workerIds.add(registerWorker());
        }

        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        CountDownLatch ready = new CountDownLatch(workerCount);
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

        // Exactly the five available jobs were handed out...
        assertThat(claims).hasSize(jobCount);
        // ...each to exactly one worker, with no job appearing twice.
        assertThat(claims.stream().map(ClaimedJob::jobId).distinct().toList()).hasSize(jobCount);
        assertThat(claims.stream().map(ClaimedJob::jobId)).containsExactlyInAnyOrderElementsOf(jobIds);
        // ...and every attempt got its own execution id.
        assertThat(claims.stream().map(ClaimedJob::executionId).distinct().toList()).hasSize(jobCount);

        // The database agrees: five RUNNING jobs, five distinct assignees, five attempts.
        List<Job> allJobs = jobRepository.findAll();
        assertThat(allJobs).hasSize(jobCount);
        assertThat(allJobs).allMatch(job -> job.getStatus() == JobStatus.RUNNING);
        assertThat(allJobs.stream().map(Job::getAssignedWorkerId).distinct().toList()).hasSize(jobCount);
        assertThat(allJobs).allMatch(job -> job.getAttemptCount() == 1);
        assertThat(jobAttemptRepository.count()).isEqualTo(jobCount);
    }
}
