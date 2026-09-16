package com.taskmesh.loadgen;

import java.util.UUID;

/**
 * The control plane answered 409 STALE_EXECUTION: this execution no longer
 * owns the job. The logical worker must abandon it - never report a result
 * for it - exactly as the real worker does.
 */
public class StaleExecutionException extends RuntimeException {

    private final UUID executionId;

    public StaleExecutionException(UUID executionId, String body) {
        super("Execution " + executionId + " is stale: " + body);
        this.executionId = executionId;
    }

    public UUID executionId() {
        return executionId;
    }
}
