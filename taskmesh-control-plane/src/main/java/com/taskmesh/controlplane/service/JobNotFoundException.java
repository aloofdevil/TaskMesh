package com.taskmesh.controlplane.service;

import java.util.UUID;

/** Thrown when a job id does not exist. Mapped to 404 by ApiExceptionHandler. */
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID id) {
        super("Job " + id + " was not found");
    }
}
