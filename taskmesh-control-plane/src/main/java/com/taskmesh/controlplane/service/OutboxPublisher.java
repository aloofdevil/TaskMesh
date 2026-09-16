package com.taskmesh.controlplane.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobEventRepository;

import tools.jackson.databind.ObjectMapper;

/**
 * Forwards committed outbox events to Kafka.
 * <p>
 * This is the second half of the transactional outbox, and the only place
 * in the control plane that talks to Kafka. Nothing on a request path
 * publishes: state changes commit to PostgreSQL with their events, and this
 * runs afterwards, separately. That is what makes a Kafka outage survivable
 * - jobs keep being submitted, claimed, completed and retried while events
 * simply accumulate unpublished, and drain in order once Kafka returns.
 * <p>
 * <strong>Delivery semantics: at-least-once.</strong> An event is marked
 * published only after Kafka acknowledges it, so an event is never lost.
 * But if the process dies between Kafka's acknowledgement and the
 * {@code published_at} update, the next pass will send it again. The event
 * carries its stable {@code job_events.id} in the message, so a consumer
 * that cares can deduplicate on it. Exactly-once delivery is not
 * implemented and is not claimed.
 */
@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final JobEventRepository jobEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxProperties properties;
    private final TaskMeshMetrics metrics;

    public OutboxPublisher(JobEventRepository jobEventRepository, KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper, OutboxProperties properties, TaskMeshMetrics metrics) {
        this.jobEventRepository = jobEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Publishes one batch and returns how many events were confirmed.
     * <p>
     * Events are marked published only after Kafka acknowledges them, and
     * only those that were acknowledged - so a partial failure leaves the
     * rest unpublished for a later pass rather than losing them. The whole
     * pass is one transaction holding row locks on the batch, which keeps
     * two control-plane instances from publishing the same rows.
     */
    @Transactional
    public int publishPending() {
        List<JobEvent> pending = jobEventRepository.lockUnpublishedBatch(properties.batchSize());
        if (pending.isEmpty()) {
            return 0;
        }

        List<Long> published = new ArrayList<>();
        for (JobEvent event : pending) {
            if (!send(event)) {
                // Stop at the first failure: Kafka is likely down, and
                // continuing would just pile up timeouts. Preserving order
                // also means a later event never overtakes an earlier one.
                break;
            }
            published.add(event.getId());
        }

        if (published.isEmpty()) {
            return 0;
        }
        jobEventRepository.markPublished(published);
        metrics.outboxPublished(published.size());
        return published.size();
    }

    private boolean send(JobEvent event) {
        try {
            String topic = topicFor(event.getEventType());
            String message = objectMapper.writeValueAsString(envelope(event));
            // Block on the acknowledgement: "published" has to mean Kafka
            // actually took it, not merely that it was buffered.
            kafkaTemplate.send(topic, event.getAggregateId(), message)
                    .get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted publishing event {}; it stays unpublished", event.getId());
            return false;
        } catch (Exception e) {
            metrics.outboxPublishFailed();
            log.warn("Could not publish event {} ({}) to Kafka; it stays unpublished and will be retried: {}",
                    event.getId(), event.getEventType(), e.getMessage());
            return false;
        }
    }

    private String topicFor(JobEventType eventType) {
        return switch (eventType.aggregate()) {
            case JOB -> properties.jobEventsTopic();
            case WORKER -> properties.workerEventsTopic();
        };
    }

    /** The published message. {@code eventId} is stable across republishes, for consumer-side deduplication. */
    private Map<String, Object> envelope(JobEvent event) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", event.getId());
        envelope.put("eventType", event.getEventType().name());
        envelope.put("aggregateId", event.getAggregateId());
        envelope.put("occurredAt", event.getCreatedAt().toString());
        envelope.put("payload", event.getPayload());
        return envelope;
    }
}
