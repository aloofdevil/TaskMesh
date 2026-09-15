package com.taskmesh.controlplane.api.dto;

import java.time.Instant;
import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * The client-facing job creation request. {@code priority}, {@code maxAttempts}
 * and {@code scheduledAt} are optional; when omitted, the service applies
 * the same defaults as the {@code jobs} table's column defaults ({@link #DEFAULT_PRIORITY},
 * {@link #DEFAULT_MAX_ATTEMPTS}, "now").
 * <p>
 * Server-controlled fields (status, attemptCount, assignedWorkerId,
 * currentExecutionId, leaseUntil, version, ...) are deliberately not
 * present here - a client cannot set them.
 */
public record CreateJobRequest(
        @NotBlank(message = "idempotencyKey must not be blank")
        @Size(max = 255, message = "idempotencyKey must be at most 255 characters")
        String idempotencyKey,

        @NotBlank(message = "type must not be blank")
        @Size(max = 100, message = "type must be at most 100 characters")
        String type,

        @NotNull(message = "payload must be provided")
        Map<String, Object> payload,

        @Min(value = 1, message = "priority must be between 1 and 10")
        @Max(value = 10, message = "priority must be between 1 and 10")
        Integer priority,

        @Min(value = 1, message = "maxAttempts must be between 1 and 20")
        @Max(value = 20, message = "maxAttempts must be between 1 and 20")
        Integer maxAttempts,

        Instant scheduledAt) {

    public static final int DEFAULT_PRIORITY = 5;
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
}
