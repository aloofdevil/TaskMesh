package com.taskmesh.controlplane.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives {@link OutboxPublisher} on a timer. Split from the publisher for
 * the same reason as the lease reaper: so tests can publish at an exact
 * moment instead of racing a background timer.
 */
@Component
@ConditionalOnProperty(prefix = "taskmesh.outbox", name = "publisher-enabled", havingValue = "true",
        matchIfMissing = true)
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final OutboxPublisher publisher;

    public OutboxPublisherScheduler(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${taskmesh.outbox.poll-interval-ms}")
    public void publish() {
        try {
            int published = publisher.publishPending();
            if (published > 0) {
                log.debug("Published {} outbox event(s) to Kafka", published);
            }
        } catch (RuntimeException e) {
            // Never let a bad pass kill the schedule - unpublished events
            // would then sit in the outbox forever.
            log.error("Outbox publishing pass failed; will retry on next tick", e);
        }
    }
}
