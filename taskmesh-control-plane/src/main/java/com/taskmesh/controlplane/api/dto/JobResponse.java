package com.taskmesh.controlplane.api.dto;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobStatus;

/**
 * Client-facing job representation. Deliberately excludes internal
 * worker-execution/lease fields (assignedWorkerId, currentExecutionId,
 * leaseUntil, failureReason, version) - those are server-internal and not
 * part of the Day 2 API surface.
 */
public record JobResponse(
        UUID id,
        String type,
        Map<String, Object> payload,
        int priority,
        JobStatus status,
        Instant scheduledAt,
        int attemptCount,
        int maxAttempts,
        Instant createdAt,
        Instant updatedAt) {

    public static JobResponse from(Job job) {
        return new JobResponse(
                job.getId(),
                job.getType(),
                job.getPayload(),
                job.getPriority(),
                job.getStatus(),
                job.getScheduledAt(),
                job.getAttemptCount(),
                job.getMaxAttempts(),
                job.getCreatedAt(),
                job.getUpdatedAt());
    }
}
