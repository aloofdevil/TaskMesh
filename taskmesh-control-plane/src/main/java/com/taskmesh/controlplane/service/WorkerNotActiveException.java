package com.taskmesh.controlplane.service;

import com.taskmesh.controlplane.domain.WorkerStatus;

/**
 * Thrown when a worker that exists but is not ACTIVE tries to heartbeat or
 * claim work - a deregistered worker must not be handed jobs. Mapped to 409
 * by ApiExceptionHandler.
 */
public class WorkerNotActiveException extends RuntimeException {

    public WorkerNotActiveException(String workerId, WorkerStatus status) {
        super("Worker '" + workerId + "' is not active (status is " + status + ")");
    }
}
