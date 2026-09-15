package com.taskmesh.common.worker;

import java.util.Map;
import java.util.UUID;

/**
 * A job handed to a worker by a successful claim - everything the worker
 * needs to execute it, and nothing more (no lease, worker assignment, or
 * internal bookkeeping columns).
 * <p>
 * {@code executionId} identifies this specific attempt. Day 3 only
 * establishes the identity; the fencing rules that make it meaningful
 * (rejecting writes from a stale execution) are Day 4.
 */
public record ClaimedJob(
        UUID jobId,
        UUID executionId,
        String type,
        Map<String, Object> payload,
        int priority,
        int attemptNumber,
        int maxAttempts) {
}
