package com.taskmesh.controlplane.service;

import java.util.UUID;

import com.taskmesh.controlplane.domain.JobStatus;

/**
 * Thrown when cancellation is requested for a job that is no longer
 * QUEUED. Day 2 does not implement cancellation of running jobs (see
 * docs/architecture.md) - this is the rejection for that case. Mapped to
 * 409 by ApiExceptionHandler.
 */
public class JobNotCancellableException extends RuntimeException {

    public JobNotCancellableException(UUID id, JobStatus currentStatus) {
        super("Job " + id + " cannot be cancelled because it is " + currentStatus);
    }
}
