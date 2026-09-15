package com.taskmesh.common.worker;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Sent by a worker to the control plane on startup. The worker chooses its
 * own id (pod name in Kubernetes, hostname + suffix locally) so that a
 * restarted worker re-registers as itself rather than leaking a new row.
 */
public record WorkerRegistrationRequest(
        @NotBlank(message = "workerId must not be blank")
        @Size(max = 255, message = "workerId must be at most 255 characters")
        String workerId,

        @NotBlank(message = "hostname must not be blank")
        @Size(max = 255, message = "hostname must be at most 255 characters")
        String hostname,

        @NotNull(message = "capacity must be provided")
        @Min(value = 1, message = "capacity must be between 1 and 64")
        @Max(value = 64, message = "capacity must be between 1 and 64")
        Integer capacity) {
}
