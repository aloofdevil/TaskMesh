package com.taskmesh.controlplane.service;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobAttemptRepository;
import com.taskmesh.controlplane.repository.JobRepository;

/**
 * Recovers jobs whose worker stopped renewing - the crash-recovery path.
 * <p>
 * This is deliberately separate from the scheduler that drives it
 * ({@code LeaseReaperScheduler}) so a sweep can be triggered directly, and
 * so tests can run it at an exact moment instead of racing a timer.
 */
@Service
public class LeaseReaperService {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaperService.class);

    private static final String LEASE_EXPIRED_REASON = "Lease expired; worker stopped renewing";

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final ReliabilityProperties properties;
    private final RetryPolicy retryPolicy;
    private final JobEventRecorder eventRecorder;
    private final TaskMeshMetrics metrics;

    public LeaseReaperService(JobRepository jobRepository, JobAttemptRepository jobAttemptRepository,
            ReliabilityProperties properties, RetryPolicy retryPolicy, JobEventRecorder eventRecorder,
            TaskMeshMetrics metrics) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.properties = properties;
        this.retryPolicy = retryPolicy;
        this.eventRecorder = eventRecorder;
        this.metrics = metrics;
    }

    /**
     * Sweeps one batch of lapsed leases and returns how many jobs were
     * recovered.
     * <p>
     * The whole sweep is one transaction, and it begins by row-locking the
     * expired jobs with {@code FOR UPDATE SKIP LOCKED}. That lock is what
     * makes this safe against a worker completing at the same moment: the
     * two compete for the same row and PostgreSQL serialises them, so
     * either the worker's completion commits first and this sweep's
     * {@code status = 'RUNNING'} condition then matches nothing, or this
     * sweep commits first and the worker's completion finds its execution
     * is no longer current. Both orders leave consistent state; neither
     * produces a job that is both COMPLETED and requeued.
     * <p>
     * Attempts are closed out before ownership is cleared, because
     * clearing {@code current_execution_id} is what makes the old
     * execution unidentifiable - the execution ids have to be captured
     * while they are still on the rows.
     */
    @Transactional
    public int reapExpiredLeases() {
        List<Job> expired = jobRepository.lockExpiredLeases(properties.reaperBatchSize());
        if (expired.isEmpty()) {
            return 0;
        }

        List<UUID> executionIds = expired.stream()
                .map(Job::getCurrentExecutionId)
                .filter(Objects::nonNull)
                .toList();
        if (!executionIds.isEmpty()) {
            jobAttemptRepository.markLeaseExpired(executionIds);
        }

        int recovered = 0;
        for (Job job : expired) {
            recovered += recoverExpiredJob(job);
        }
        return recovered;
    }

    /**
     * Applies the same retry policy to an abandoned execution as to a
     * reported failure. An expired lease has still consumed an attempt - the
     * count was incremented when the job was claimed - so a job whose worker
     * keeps dying must eventually dead-letter rather than be re-dispatched
     * forever.
     * <p>
     * Each job is handled with its own conditional statement rather than one
     * bulk update, because the branch and the backoff both depend on that
     * job's own attempt count, and the policy that decides them lives in
     * {@link RetryPolicy} rather than being duplicated in SQL. The batch is
     * bounded, so this stays a small number of statements per sweep.
     */
    private int recoverExpiredJob(Job job) {
        boolean retryable = retryPolicy.hasAttemptsRemaining(job.getAttemptCount(), job.getMaxAttempts());

        int updated;
        if (retryable) {
            long backoffSeconds = retryPolicy.backoffAfterAttempt(job.getAttemptCount()).toSeconds();
            updated = jobRepository.scheduleRetryAfterLeaseExpiry(job.getId(), LEASE_EXPIRED_REASON, backoffSeconds);
        } else {
            updated = jobRepository.deadLetterAfterLeaseExpiry(job.getId(), LEASE_EXPIRED_REASON);
        }
        if (updated == 0) {
            // Something else got there first - most likely the worker's own
            // completion committing just before this sweep. Leave it alone.
            return 0;
        }

        metrics.leaseExpired();
        Job afterRecovery = jobRepository.findById(job.getId()).orElseThrow();
        eventRecorder.recordJobEvent(
                retryable ? JobEventType.JOB_RETRYING : JobEventType.JOB_DEAD_LETTER, afterRecovery);

        if (retryable) {
            metrics.jobRetryScheduled();
            log.warn("Lease expired for job {} held by worker {} (execution {}); retrying at {}",
                    job.getId(), job.getAssignedWorkerId(), job.getCurrentExecutionId(),
                    afterRecovery.getScheduledAt());
        } else {
            metrics.jobDeadLettered();
            log.warn("Lease expired for job {} held by worker {} after {} attempt(s); dead-lettered",
                    job.getId(), job.getAssignedWorkerId(), job.getAttemptCount());
        }
        return 1;
    }

    /**
     * Promotes jobs whose retry backoff has elapsed from RETRYING to
     * QUEUED, making them claimable again.
     * <p>
     * This runs in the same sweep as lease reaping rather than on a
     * scheduler of its own. Both are liveness duties of the control plane,
     * and folding promotion into a loop that must already be alive avoids
     * adding a second component whose silent death would strand jobs -
     * here, a stalled sweep is a single failure that is already visible
     * because leases would stop being recovered too.
     */
    @Transactional
    public int promoteDueRetries() {
        List<Job> due = jobRepository.lockDueRetries(properties.reaperBatchSize());
        if (due.isEmpty()) {
            return 0;
        }

        int promoted = jobRepository.promoteDueRetries(due.stream().map(Job::getId).toList());
        due.forEach(job -> {
            Job afterPromotion = jobRepository.findById(job.getId()).orElseThrow();
            eventRecorder.recordJobEvent(JobEventType.JOB_QUEUED, afterPromotion);
            log.info("Job {} backoff elapsed; returned to the queue for attempt {}",
                    job.getId(), job.getAttemptCount() + 1);
        });
        return promoted;
    }
}
