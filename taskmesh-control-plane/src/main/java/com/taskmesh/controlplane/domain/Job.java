package com.taskmesh.controlplane.domain;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * A job, as persisted in the {@code jobs} table created by
 * {@code V1__init.sql}. This is not a full mapping of the table:
 * {@code lease_until}, {@code last_failure_reason}, {@code result} and
 * {@code completed_at} exist in the schema for later days (leases, retries,
 * result reporting) and are intentionally left unmapped - Hibernate's
 * schema validator only checks the columns an entity actually maps, so
 * those future-use columns stay untouched.
 * <p>
 * Day 3 added {@code assigned_worker_id}, {@code current_execution_id} and
 * {@code started_at}, which claiming writes.
 */
@Entity
@Table(name = "jobs")
public class Job {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "idempotency_key", updatable = false)
    private String idempotencyKey;

    @Column(name = "payload_hash", updatable = false)
    private String payloadHash;

    @Column(name = "type", nullable = false, updatable = false)
    private String type;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private Map<String, Object> payload;

    @Column(name = "priority", nullable = false, updatable = false)
    private short priority;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private JobStatus status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "max_attempts", nullable = false, updatable = false)
    private int maxAttempts;

    @Column(name = "scheduled_at", nullable = false, updatable = false)
    private Instant scheduledAt;

    @Column(name = "assigned_worker_id")
    private String assignedWorkerId;

    /**
     * Identifies the currently running attempt. Day 3 mints it on claim;
     * the fencing rules that reject writes carrying a stale execution id
     * are Day 4.
     */
    @Column(name = "current_execution_id")
    private UUID currentExecutionId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /**
     * Backs the {@code version BIGINT NOT NULL DEFAULT 0} column. Left null
     * until Hibernate seeds it on insert (Spring Data JPA's default
     * new-vs-existing check falls back to "version == null" for entities
     * with an assigned, non-generated id such as this one, so leaving it
     * null here is what makes {@code JobRepository.save(...)} issue an
     * INSERT rather than an UPDATE/merge for a brand-new job).
     * <p>
     * Note: {@link com.taskmesh.controlplane.repository.JobRepository#cancelIfQueued}
     * updates this column directly via a bulk JPQL update, bypassing
     * Hibernate's normal optimistic-lock increment - this column is not
     * used as a concurrency guard in Day 2. The real guard is the
     * conditional {@code WHERE status = 'QUEUED'} in that same query.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    protected Job() {
        // required by JPA
    }

    private Job(UUID id, String idempotencyKey, String payloadHash, String type, Map<String, Object> payload,
            short priority, int maxAttempts, Instant scheduledAt) {
        // createdAt/updatedAt are audit timestamps that nothing compares
        // against the database clock, so the JVM clock is fine for them.
        // scheduledAt is different: the claim query tests it against
        // PostgreSQL's now(), so the caller must supply a value from that
        // same clock rather than letting this constructor invent one.
        Instant now = Instant.now();
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.payloadHash = payloadHash;
        this.type = type;
        this.payload = payload;
        this.priority = priority;
        this.status = JobStatus.QUEUED;
        this.attemptCount = 0;
        this.maxAttempts = maxAttempts;
        this.scheduledAt = Objects.requireNonNull(scheduledAt, "scheduledAt");
        this.createdAt = now;
        this.updatedAt = now;
    }

    /**
     * Creates a new job in {@link JobStatus#QUEUED} - the only status a job
     * is ever constructed in.
     *
     * @param scheduledAt when the job becomes claimable; must come from the
     *                    database clock (see {@code JobRepository.databaseTime()})
     */
    public static Job createQueued(String idempotencyKey, String payloadHash, String type,
            Map<String, Object> payload, short priority, int maxAttempts, Instant scheduledAt) {
        return new Job(UUID.randomUUID(), idempotencyKey, payloadHash, type, payload, priority, maxAttempts,
                scheduledAt);
    }

    /**
     * Transitions {@code QUEUED -> RUNNING} for a worker that has just won
     * the claim. Only ever called on a row the claiming transaction already
     * holds a lock on (see {@code JobRepository.lockNextClaimableJobs}), so
     * the status check here is a domain invariant guarding against misuse,
     * not the concurrency control - the row lock is.
     * <p>
     * {@code startedAt} records the first time the job ever started, so it
     * is not overwritten by later attempts.
     */
    public void claimedBy(String workerId, UUID executionId) {
        if (status != JobStatus.QUEUED) {
            throw new IllegalStateException("Job " + id + " cannot be claimed because it is " + status);
        }
        Instant now = Instant.now();
        this.status = JobStatus.RUNNING;
        this.assignedWorkerId = workerId;
        this.currentExecutionId = executionId;
        this.attemptCount = this.attemptCount + 1;
        if (this.startedAt == null) {
            this.startedAt = now;
        }
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getPayloadHash() {
        return payloadHash;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public short getPriority() {
        return priority;
    }

    public JobStatus getStatus() {
        return status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Instant getScheduledAt() {
        return scheduledAt;
    }

    public String getAssignedWorkerId() {
        return assignedWorkerId;
    }

    public UUID getCurrentExecutionId() {
        return currentExecutionId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Long getVersion() {
        return version;
    }
}
