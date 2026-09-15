package com.taskmesh.controlplane.service;

/** Thrown when a worker id is not registered. Mapped to 404 by ApiExceptionHandler. */
public class WorkerNotFoundException extends RuntimeException {

    public WorkerNotFoundException(String workerId) {
        super("Worker '" + workerId + "' is not registered");
    }
}
