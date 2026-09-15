package com.taskmesh.controlplane.service;

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

    public DispatchService(JobRepository jobRepository, JobAttemptRepository jobAttemptRepository,
            WorkerService workerService) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.workerService = workerService;
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
        workerService.requireActive(workerId);

        List<Job> locked = jobRepository.lockNextClaimableJobs(1);
        if (locked.isEmpty()) {
            return Optional.empty();
        }

        Job job = locked.get(0);
        UUID executionId = UUID.randomUUID();
        job.claimedBy(workerId, executionId);

        jobAttemptRepository.save(JobAttempt.start(job.getId(), job.getAttemptCount(), executionId, workerId));

        log.info("Job {} claimed by worker {} (attempt {}, execution {})", job.getId(), workerId,
                job.getAttemptCount(), executionId);

        return Optional.of(new ClaimedJob(
                job.getId(),
                executionId,
                job.getType(),
                job.getPayload(),
                job.getPriority(),
                job.getAttemptCount(),
                job.getMaxAttempts()));
    }
}
