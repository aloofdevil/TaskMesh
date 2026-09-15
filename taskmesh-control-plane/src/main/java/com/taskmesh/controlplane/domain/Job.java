package com.taskmesh.controlplane.domain;

import java.time.Instant;
import java.util.Map;
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
 * {@code V1__init.sql}. This is a Day 2 mapping, not a full mapping of the
 * table: {@code assigned_worker_id}, {@code current_execution_id},
 * {@code lease_until}, {@code last_failure_reason}, {@code result},
 * {@code started_at} and {@code completed_at} exist in the schema for later
 * days (worker claiming, leases, retries) and are intentionally left
 * unmapped here - Hibernate's schema validator only checks the columns an
 * entity actually maps, so the future-use columns are untouched by Day 2.
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

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

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
        this.scheduledAt = scheduledAt != null ? scheduledAt : now;
        this.createdAt = now;
        this.updatedAt = now;
    }

    /**
     * Creates a new job in {@link JobStatus#QUEUED}. This is the only
     * creation path in Day 2 - jobs are never constructed in any other
     * status.
     */
    public static Job createQueued(String idempotencyKey, String payloadHash, String type,
            Map<String, Object> payload, short priority, int maxAttempts, Instant scheduledAt) {
        return new Job(UUID.randomUUID(), idempotencyKey, payloadHash, type, payload, priority, maxAttempts,
                scheduledAt);
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

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Long getVersion() {
        return version;
    }
}
