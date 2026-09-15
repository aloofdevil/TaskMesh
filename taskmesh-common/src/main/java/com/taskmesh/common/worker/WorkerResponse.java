package com.taskmesh.common.worker;

import java.time.Instant;

/**
 * The control plane's view of a registered worker. {@code status} is a
 * plain String rather than a shared enum: the worker does not branch on it,
 * and keeping the control plane's internal status enum out of the wire
 * contract means new statuses (DEAD, added on Day 4) do not break older
 * workers.
 */
public record WorkerResponse(
        String workerId,
        String hostname,
        int capacity,
        String status,
        Instant registeredAt,
        Instant lastHeartbeatAt) {
}
