package com.taskmesh.controlplane.service;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.controlplane.domain.Job;
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

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final ReliabilityProperties properties;

    public LeaseReaperService(JobRepository jobRepository, JobAttemptRepository jobAttemptRepository,
            ReliabilityProperties properties) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.properties = properties;
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

        List<UUID> jobIds = expired.stream().map(Job::getId).toList();
        List<UUID> executionIds = expired.stream()
                .map(Job::getCurrentExecutionId)
                .filter(java.util.Objects::nonNull)
                .toList();

        if (!executionIds.isEmpty()) {
            jobAttemptRepository.markLeaseExpired(executionIds);
        }
        int requeued = jobRepository.requeueAfterLeaseExpiry(jobIds);

        expired.forEach(job -> log.warn("Lease expired for job {} held by worker {} (execution {}); requeued",
                job.getId(), job.getAssignedWorkerId(), job.getCurrentExecutionId()));

        return requeued;
    }
}
