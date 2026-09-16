package com.taskmesh.controlplane.service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobAttempt;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobAttemptRepository;
import com.taskmesh.controlplane.repository.JobRepository;

/**
 * Hands work to workers. This is the server side of the pull model: the
 * control plane never pushes to a worker, it only answers a worker that
 * asks for work.
 */
@Service
public class DispatchService {

    private static final Logger log = LoggerFactory.getLogger(DispatchService.class);

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final WorkerService workerService;
    private final ReliabilityProperties properties;
    private final JobEventRecorder eventRecorder;
    private final TaskMeshMetrics metrics;

    public DispatchService(JobRepository jobRepository, JobAttemptRepository jobAttemptRepository,
            WorkerService workerService, ReliabilityProperties properties, JobEventRecorder eventRecorder,
            TaskMeshMetrics metrics) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.workerService = workerService;
        this.properties = properties;
        this.eventRecorder = eventRecorder;
        this.metrics = metrics;
    }

    /**
     * Claims one job for a worker, or returns empty if nothing is eligible.
     * <p>
     * Everything below runs in a single transaction, and that is the whole
     * point. The row lock taken by
     * {@link JobRepository#lockNextClaimableJobs(int)} is held until this
     * transaction commits, so the window between "this job was selected"
     * and "this job is RUNNING and assigned" is never visible to another
     * claimer: a concurrent claim either skips the locked row
     * ({@code SKIP LOCKED}) or, if it arrives after the commit, no longer
     * sees the row as QUEUED. Selecting and then updating in two separate
     * transactions would reopen exactly that window and let two workers
     * claim the same job.
     * <p>
     * The attempt row is inserted in the same transaction, so a job that is
     * RUNNING always has a matching attempt - the two can never disagree,
     * and the table's {@code UNIQUE (job_id, attempt_number)} constraint
     * would reject a duplicate claim even if the locking were somehow
     * bypassed.
     */
    @Transactional
    public Optional<ClaimedJob> claim(String workerId) {
        long startNanos = System.nanoTime();
        try {
            return doClaim(workerId);
        } finally {
            // Timed whether or not work was found: an empty poll is the
            // common case, and its cost is what every idle worker pays.
            metrics.recordClaimLatency(System.nanoTime() - startNanos);
        }
    }

    private Optional<ClaimedJob> doClaim(String workerId) {
        workerService.requireActive(workerId);

        List<Job> locked = jobRepository.lockNextClaimableJobs(1);
        if (locked.isEmpty()) {
            return Optional.empty();
        }

        Job job = locked.get(0);
        // A fresh execution id per claim is what lets a later reassignment
        // invalidate this one: the previous holder's id stops matching the
        // job the moment a new claim overwrites it.
        UUID executionId = UUID.randomUUID();
        // The lease deadline is anchored to the database's clock, since
        // that is the clock the reaper judges expiry against. Inside this
        // transaction now() is fixed at transaction start, so the lease
        // runs from when the claim began.
        Instant leaseUntil = jobRepository.databaseTime().plusSeconds(properties.leaseDurationSeconds());
        job.claimedBy(workerId, executionId, leaseUntil);

        jobAttemptRepository.save(JobAttempt.start(job.getId(), job.getAttemptCount(), executionId, workerId));
        // Same transaction as the claim itself, so a job that is RUNNING
        // always has the event that says so.
        eventRecorder.recordJobEvent(JobEventType.JOB_RUNNING, job);
        metrics.jobClaimed();

        log.info("Job {} claimed by worker {} (attempt {}, execution {}, lease until {})", job.getId(), workerId,
                job.getAttemptCount(), executionId, leaseUntil);

        return Optional.of(new ClaimedJob(
                job.getId(),
                executionId,
                job.getType(),
                job.getPayload(),
                job.getPriority(),
                job.getAttemptCount(),
                job.getMaxAttempts(),
                leaseUntil));
    }
}
