package com.taskmesh.controlplane.service;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobRepository;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final PayloadHasher payloadHasher;
    private final JobEventRecorder eventRecorder;
    private final TransactionTemplate transactionTemplate;

    public JobService(JobRepository jobRepository, PayloadHasher payloadHasher, JobEventRecorder eventRecorder,
            PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.payloadHasher = payloadHasher;
        this.eventRecorder = eventRecorder;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Creates a job, or returns the existing one if the idempotency key was
     * already used with the same payload.
     * <p>
     * The lookup-then-insert below is not by itself race-free: two
     * concurrent requests for the same new key can both pass the initial
     * {@code findByIdempotencyKey} check (ordinary READ COMMITTED
     * visibility), so the actual protection against a duplicate job is the
     * database's {@code UNIQUE(idempotency_key)} constraint. {@code saveAndFlush}
     * forces the INSERT to execute immediately (rather than at end-of-transaction),
     * so the constraint violation surfaces here as a catchable
     * {@link DataIntegrityViolationException} rather than escaping after
     * this method returns. The loser of the race then re-reads the row the
     * winner committed and treats it exactly like the "existing job" case.
     * <p>
     * Deliberately NOT wrapped in a single {@code @Transactional}: Postgres
     * aborts an entire transaction after any failed statement ("current
     * transaction is aborted, commands ignored until end of transaction
     * block"), so if the lookup, the failed insert, and the follow-up
     * lookup all shared one transaction, that follow-up lookup would itself
     * fail. Each repository call below runs in its own transaction (Spring
     * Data's default when no wider transaction is active), so a failed
     * insert's aborted transaction is isolated to that one call and never
     * poisons the read that comes after it.
     */
    public JobCreationResult createJob(CreateJobRequest request) {
        String payloadHash = payloadHasher.hash(request.payload());

        Optional<Job> existing = jobRepository.findByIdempotencyKey(request.idempotencyKey());
        if (existing.isPresent()) {
            return new JobCreationResult(matchOrThrow(existing.get(), payloadHash), false);
        }

        short priority = (short) (request.priority() != null ? request.priority() : CreateJobRequest.DEFAULT_PRIORITY);
        int maxAttempts = request.maxAttempts() != null ? request.maxAttempts() : CreateJobRequest.DEFAULT_MAX_ATTEMPTS;
        // "As soon as possible" has to mean "now" on the database's clock,
        // because the database is what decides eligibility - see
        // JobRepository.databaseTime().
        Instant scheduledAt = request.scheduledAt() != null ? request.scheduledAt() : jobRepository.databaseTime();

        Job job = Job.createQueued(request.idempotencyKey(), payloadHash, request.type(), request.payload(),
                priority, maxAttempts, scheduledAt);

        try {
            return new JobCreationResult(insertWithQueuedEvent(job), true);
        } catch (DataIntegrityViolationException raceLost) {
            Job winner = jobRepository.findByIdempotencyKey(request.idempotencyKey()).orElseThrow(() -> raceLost);
            return new JobCreationResult(matchOrThrow(winner, payloadHash), false);
        }
    }

    /**
     * Inserts the job and its JOB_QUEUED outbox event as one unit.
     * <p>
     * An explicit transaction rather than {@code @Transactional} on
     * {@link #createJob}, for two reasons. The two writes have to commit
     * together, so that a job can never exist without the event announcing
     * it. But the surrounding method must *not* share that transaction:
     * when the unique idempotency key loses a race, PostgreSQL aborts the
     * whole transaction, and any statement issued afterwards in it fails
     * too. Scoping the transaction to just these two writes means the
     * failed insert rolls back cleanly - taking the event with it - and the
     * recovery lookup in the caller runs in a fresh, healthy transaction.
     */
    private Job insertWithQueuedEvent(Job job) {
        return transactionTemplate.execute(status -> {
            Job saved = jobRepository.saveAndFlush(job);
            eventRecorder.recordJobEvent(JobEventType.JOB_QUEUED, saved);
            return saved;
        });
    }

    @Transactional(readOnly = true)
    public Job getJob(UUID id) {
        return jobRepository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    /**
     * Cancels a job. The atomic conditional update happens first - it is
     * the actual concurrency guard - and only if it affects no rows do we
     * do a follow-up read to tell a missing job (404) apart from one that
     * exists but is no longer QUEUED (409).
     */
    @Transactional
    public Job cancelJob(UUID id) {
        int updated = jobRepository.cancelIfQueued(id, Instant.now());
        if (updated == 1) {
            Job cancelled = jobRepository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
            eventRecorder.recordJobEvent(JobEventType.JOB_CANCELLED, cancelled);
            return cancelled;
        }
        Job existing = jobRepository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
        throw new JobNotCancellableException(id, existing.getStatus());
    }

    private Job matchOrThrow(Job existing, String payloadHash) {
        if (!existing.getPayloadHash().equals(payloadHash)) {
            throw new IdempotencyKeyConflictException(existing.getIdempotencyKey());
        }
        return existing;
    }

    /** @param created true if this call inserted a new job, false if an existing job was returned. */
    public record JobCreationResult(Job job, boolean created) {
    }
}
