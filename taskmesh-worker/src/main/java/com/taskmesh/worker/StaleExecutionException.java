package com.taskmesh.worker;

/**
 * Raised when the control plane answers 409 {@code STALE_EXECUTION}: this
 * worker's execution no longer owns the job, because its lease lapsed and
 * the job was reassigned.
 * <p>
 * The only correct response is to stop touching that execution - no
 * further renewals, no success or failure report. Whatever work was done
 * is abandoned, because another execution now owns the job and reporting
 * would be an attempt to overwrite it.
 */
public class StaleExecutionException extends RuntimeException {

    public StaleExecutionException(String message) {
        super(message);
    }
}
