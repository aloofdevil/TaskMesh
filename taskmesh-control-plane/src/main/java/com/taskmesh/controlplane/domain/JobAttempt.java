package com.taskmesh.controlplane.domain;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * One execution attempt of a job, as persisted in the {@code job_attempts}
 * table created by {@code V1__init.sql}. A row is inserted in the same
 * transaction that claims the job, so the attempt history and the job's
 * {@code attempt_count} can never disagree - the table's
 * {@code UNIQUE (job_id, attempt_number)} constraint enforces that.
 * <p>
 * Day 3 mapping only: {@code completed_at} and {@code failure_reason} exist
 * in the schema but are written when a worker reports a result (Day 4), so
 * they are intentionally left unmapped rather than added as fields nothing
 * sets.
 * <p>
 * {@code job_id} is a plain {@link UUID} rather than a {@code @ManyToOne}
 * association: an attempt is written, never navigated from, so an
 * association would only add lazy-loading machinery for no benefit.
 */
@Entity
@Table(name = "job_attempts")
public class JobAttempt {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "job_id", nullable = false, updatable = false)
    private UUID jobId;

    @Column(name = "attempt_number", nullable = false, updatable = false)
    private int attemptNumber;

    @Column(name = "execution_id", nullable = false, updatable = false)
    private UUID executionId;

    @Column(name = "worker_id", nullable = false, updatable = false)
    private String workerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private JobAttemptStatus status;

    @Column(name = "started_at", nullable = false, updatable = false)
    private Instant startedAt;

    protected JobAttempt() {
        // required by JPA
    }

    private JobAttempt(UUID id, UUID jobId, int attemptNumber, UUID executionId, String workerId, Instant startedAt) {
        this.id = id;
        this.jobId = jobId;
        this.attemptNumber = attemptNumber;
        this.executionId = executionId;
        this.workerId = workerId;
        this.status = JobAttemptStatus.RUNNING;
        this.startedAt = startedAt;
    }

    /** Records the start of an attempt. The only way an attempt is created in Day 3. */
    public static JobAttempt start(UUID jobId, int attemptNumber, UUID executionId, String workerId) {
        return new JobAttempt(UUID.randomUUID(), jobId, attemptNumber, executionId, workerId, Instant.now());
    }

    public UUID getId() {
        return id;
    }

    public UUID getJobId() {
        return jobId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public UUID getExecutionId() {
        return executionId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public JobAttemptStatus getStatus() {
        return status;
    }

    public Instant getStartedAt() {
        return startedAt;
    }
}
