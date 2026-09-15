package com.taskmesh.controlplane.service;

import java.util.UUID;

/**
 * Thrown when a worker tries to renew, complete or fail a job that its
 * execution no longer owns - because the lease lapsed and the job was
 * reassigned, because the job already reached a terminal state, or because
 * the worker/execution identifiers simply do not match the job's current
 * ones.
 * <p>
 * This is the visible half of fencing. The invisible half is that the
 * conditional UPDATE which raises it changed nothing, so a stale worker
 * cannot alter job status, attempt status, ownership or lease state.
 * Mapped to 409 with error code {@code STALE_EXECUTION}.
 */
public class StaleExecutionException extends RuntimeException {

    public static final String ERROR_CODE = "STALE_EXECUTION";

    public StaleExecutionException(UUID jobId, UUID executionId) {
        super("Execution " + executionId + " no longer owns job " + jobId
                + "; it was reassigned, already finished, or never owned it");
    }
}
