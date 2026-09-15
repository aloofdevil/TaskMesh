package com.taskmesh.controlplane.domain;

/**
 * Lifecycle events published to Kafka.
 * <p>
 * Each type knows which topic it belongs on, so the outbox table does not
 * need a topic column and the routing rule lives in exactly one place.
 * <p>
 * There is deliberately no {@code WORKER_HEARTBEAT}: workers heartbeat
 * every 5 seconds each, so emitting an event per heartbeat would dominate
 * the topic with messages that carry no state change and that no consumer
 * could usefully act on. Worker liveness is already observable in
 * PostgreSQL and Redis; only the transitions that change what the system
 * knows about a worker are published.
 */
public enum JobEventType {

    JOB_QUEUED(Aggregate.JOB),
    JOB_RUNNING(Aggregate.JOB),
    JOB_COMPLETED(Aggregate.JOB),
    JOB_RETRYING(Aggregate.JOB),
    JOB_DEAD_LETTER(Aggregate.JOB),
    JOB_CANCELLED(Aggregate.JOB),

    WORKER_REGISTERED(Aggregate.WORKER),
    WORKER_DEREGISTERED(Aggregate.WORKER);

    /** Which stream of events this belongs to. */
    public enum Aggregate {
        JOB,
        WORKER
    }

    private final Aggregate aggregate;

    JobEventType(Aggregate aggregate) {
        this.aggregate = aggregate;
    }

    public Aggregate aggregate() {
        return aggregate;
    }
}
