package com.taskmesh.controlplane.domain;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * A worker process, as persisted in the {@code workers} table created by
 * {@code V1__init.sql}. PostgreSQL is authoritative for worker records;
 * Redis only holds a TTL'd liveness projection of this row (see
 * {@code WorkerLivenessCache}), so losing Redis never loses a worker.
 * <p>
 * The id is chosen by the worker itself, so registration is an upsert: a
 * restarted worker re-registers under the same id rather than creating a
 * second row.
 */
@Entity
@Table(name = "workers")
public class Worker {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private String id;

    @Column(name = "hostname", nullable = false)
    private String hostname;

    @Column(name = "capacity", nullable = false)
    private int capacity;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private WorkerStatus status;

    @Column(name = "registered_at", nullable = false, updatable = false)
    private Instant registeredAt;

    @Column(name = "last_heartbeat_at", nullable = false)
    private Instant lastHeartbeatAt;

    protected Worker() {
        // required by JPA
    }

    private Worker(String id, String hostname, int capacity, Instant now) {
        this.id = id;
        this.hostname = hostname;
        this.capacity = capacity;
        this.status = WorkerStatus.ACTIVE;
        this.registeredAt = now;
        this.lastHeartbeatAt = now;
    }

    public static Worker register(String id, String hostname, int capacity) {
        return new Worker(id, hostname, capacity, Instant.now());
    }

    /**
     * Re-registration of a worker that already has a row: refresh what the
     * worker reported and (re)activate it, so a restarted - or previously
     * deregistered - worker comes back as itself.
     */
    public void reregister(String hostname, int capacity) {
        this.hostname = hostname;
        this.capacity = capacity;
        this.status = WorkerStatus.ACTIVE;
        this.lastHeartbeatAt = Instant.now();
    }

    public boolean isActive() {
        return status == WorkerStatus.ACTIVE;
    }

    public String getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }

    public int getCapacity() {
        return capacity;
    }

    public WorkerStatus getStatus() {
        return status;
    }

    public Instant getRegisteredAt() {
        return registeredAt;
    }

    public Instant getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }
}
