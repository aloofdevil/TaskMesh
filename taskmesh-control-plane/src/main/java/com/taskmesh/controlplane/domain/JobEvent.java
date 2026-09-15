package com.taskmesh.controlplane.domain;

import java.time.Instant;
import java.util.Map;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * One lifecycle event, in the {@code job_events} table created by
 * {@code V1__init.sql}. This table is both the durable audit log and the
 * transactional outbox: a row is written in the same transaction as the
 * state change it describes, and a background publisher later forwards it
 * to Kafka and stamps {@code published_at}.
 * <p>
 * The {@code BIGSERIAL} id is the event's stable identity. It is assigned
 * at insert and never changes, so a message republished after a publisher
 * crash carries the same id as the first copy and a consumer can use it to
 * deduplicate.
 */
@Entity
@Table(name = "job_events")
public class JobEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    /** The job or worker this event is about; also the Kafka message key. */
    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private String aggregateId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, updatable = false)
    private JobEventType eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private Map<String, Object> payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** Null until the publisher has successfully handed the event to Kafka. */
    @Column(name = "published_at")
    private Instant publishedAt;

    protected JobEvent() {
        // required by JPA
    }

    private JobEvent(String aggregateId, JobEventType eventType, Map<String, Object> payload) {
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    public static JobEvent record(String aggregateId, JobEventType eventType, Map<String, Object> payload) {
        return new JobEvent(aggregateId, eventType, payload);
    }

    public Long getId() {
        return id;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public JobEventType getEventType() {
        return eventType;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public boolean isPublished() {
        return publishedAt != null;
    }
}
