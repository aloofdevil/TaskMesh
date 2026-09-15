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
 * @param jobEventsTopic   topic for job lifecycle events
 * @param workerEventsTopic topic for worker lifecycle events
 */
@ConfigurationProperties(prefix = "taskmesh.outbox")
public record OutboxProperties(
        boolean publisherEnabled,
        long pollIntervalMs,
        int batchSize,
        long sendTimeoutMs,
        String jobEventsTopic,
        String workerEventsTopic) {
}
