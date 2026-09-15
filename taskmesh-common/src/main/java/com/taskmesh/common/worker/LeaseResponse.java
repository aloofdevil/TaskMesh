package com.taskmesh.common.worker;

import java.time.Instant;
import java.util.UUID;

/** The extended lease returned by a successful renewal. */
public record LeaseResponse(UUID jobId, UUID executionId, Instant leaseUntil) {
}
