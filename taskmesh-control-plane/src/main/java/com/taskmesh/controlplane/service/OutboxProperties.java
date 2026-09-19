package com.taskmesh.controlplane.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Outbox publisher tuning and topic names.
 *
 * @param publisherEnabled whether the background publisher runs
 * @param pollIntervalMs   how often to look for unpublished events
 * @param batchSize        maximum events published per pass
 * @param sendTimeoutMs    how long to wait for Kafka to acknowledge a send before
 *                         treating it as failed and leaving the event unpublished
 * @param asyncSends       false (the default) sends each event and waits for its
 *                         acknowledgement before sending the next. true submits the
 *                         whole batch first and then collects the acknowledgements.
 *                         Only the Kafka send/wait shape differs; what gets marked
 *                         published is decided per event either way. See
 *                         docs/day20-async-outbox-send-experiment.md for the
 *                         failure-semantics difference this introduces.
 * @param jobEventsTopic   topic for job lifecycle events
 * @param workerEventsTopic topic for worker lifecycle events
 */
@ConfigurationProperties(prefix = "taskmesh.outbox")
public record OutboxProperties(
        boolean publisherEnabled,
        long pollIntervalMs,
        int batchSize,
        int publisherConcurrency,
        long sendTimeoutMs,
        boolean asyncSends,
        String jobEventsTopic,
        String workerEventsTopic,
        int topicPartitions) {
}
