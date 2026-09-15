package com.taskmesh.common.worker;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A job handed to a worker by a successful claim - everything the worker
 * needs to execute it, and nothing more (no lease, worker assignment, or
 * internal bookkeeping columns).
 * <p>
 * {@code executionId} is the fencing token for this attempt: every
 * subsequent write the worker makes (renew, complete, fail) must carry it,
 * and the control plane rejects it once a newer execution owns the job.
 * <p>
 * {@code leaseUntil} is when this execution's claim on the job lapses if
 * the worker stops renewing. It is computed by PostgreSQL, so it is
 * comparable with the control plane's expiry checks regardless of the
 * worker's own clock.
 */
public record ClaimedJob(
        UUID jobId,
        UUID executionId,
        String type,
        Map<String, Object> payload,
        int priority,
        int attemptNumber,
        int maxAttempts,
        Instant leaseUntil) {
}
