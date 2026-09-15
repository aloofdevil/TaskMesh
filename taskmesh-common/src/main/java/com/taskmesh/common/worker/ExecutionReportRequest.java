package com.taskmesh.common.worker;

import java.util.UUID;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Identifies the execution a worker is reporting about, for lease renewal
 * and for successful completion.
 * <p>
 * The {@code executionId} is what makes these calls safe: the control plane
 * applies the change only while this execution still owns the job, so a
 * worker whose lease lapsed and whose job was reassigned cannot write over
 * the execution that replaced it.
 */
public record ExecutionReportRequest(
        @NotBlank(message = "workerId must not be blank")
        @Size(max = 255, message = "workerId must be at most 255 characters")
        String workerId,

        @NotNull(message = "executionId must be provided")
        UUID executionId) {
}
