package com.taskmesh.common.worker;

import java.util.UUID;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** A worker reporting that its execution of a job failed. */
public record FailJobRequest(
        @NotBlank(message = "workerId must not be blank")
        @Size(max = 255, message = "workerId must be at most 255 characters")
        String workerId,

        @NotNull(message = "executionId must be provided")
        UUID executionId,

        @Size(max = 2000, message = "failureReason must be at most 2000 characters")
        String failureReason) {
}
