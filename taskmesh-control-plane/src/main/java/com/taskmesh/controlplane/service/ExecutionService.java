package com.taskmesh.controlplane.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.common.worker.LeaseResponse;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.repository.JobAttemptRepository;
import com.taskmesh.controlplane.repository.JobRepository;

/**
 * Handles what a worker reports about an execution it owns: lease
 * renewals, successful completion, and failure.
 * <p>
 * Every method here follows the same shape - attempt the fenced
 * conditional UPDATE first, and treat an affected-row count of 0 as
 * rejection. Nothing reads the job and then decides in Java whether the
 * caller is allowed to write, because between that read and the write the
 * reaper could requeue the job and another worker could claim it.
 */
@Service
public class ExecutionService {

    private static final Logger log = LoggerFactory.getLogger(ExecutionService.class);

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final ReliabilityProperties properties;

    public ExecutionService(JobRepository jobRepository, JobAttemptRepository jobAttemptRepository,
            ReliabilityProperties properties) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.properties = properties;
    }

    @Transactional
    public LeaseResponse renewLease(UUID jobId, String workerId, UUID executionId) {
        int renewed = jobRepository.renewLease(jobId, workerId, executionId, properties.leaseDurationSeconds());
        if (renewed == 0) {
            throw staleOrMissing(jobId, executionId);
        }
        Job job = jobRepository.findById(jobId).orElseThrow(() -> new JobNotFoundException(jobId));
        return new LeaseResponse(jobId, executionId, job.getLeaseUntil());
    }

    /**
     * Records successful completion. The job update is applied first: it is
     * the fence, so if it changes nothing the attempt must be left alone
     * too - otherwise a stale worker could still rewrite attempt history
     * for an execution that has been superseded.
     */
    @Transactional
    public void complete(UUID jobId, String workerId, UUID executionId) {
        int completed = jobRepository.completeIfOwnedByExecution(jobId, workerId, executionId);
        if (completed == 0) {
            throw staleOrMissing(jobId, executionId);
        }
        jobAttemptRepository.markSucceeded(executionId);
        log.info("Job {} completed by worker {} (execution {})", jobId, workerId, executionId);
    }

    /**
     * Records a failed execution and releases the job back to the queue.
     * Day 4 requeues unconditionally; the retry budget, backoff and
     * dead-lettering are Day 5.
     */
    @Transactional
    public void fail(UUID jobId, String workerId, UUID executionId, String failureReason) {
        int failed = jobRepository.failIfOwnedByExecution(jobId, workerId, executionId, failureReason);
        if (failed == 0) {
            throw staleOrMissing(jobId, executionId);
        }
        jobAttemptRepository.markFailed(executionId, failureReason);
        log.info("Job {} failed on worker {} (execution {}): {}", jobId, workerId, executionId, failureReason);
    }

    /**
     * Distinguishes "no such job" (404) from "you are not the current
     * execution" (409). The lookup happens only after the conditional
     * update has already declined to change anything, so it is purely for
     * producing the right error - never for deciding whether the write was
     * allowed.
     */
    private RuntimeException staleOrMissing(UUID jobId, UUID executionId) {
        if (jobRepository.findById(jobId).isEmpty()) {
            return new JobNotFoundException(jobId);
        }
        return new StaleExecutionException(jobId, executionId);
    }
}
