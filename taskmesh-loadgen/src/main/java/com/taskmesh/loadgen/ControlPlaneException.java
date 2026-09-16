package com.taskmesh.loadgen;

/** A request to the control plane failed - transport error or a non-2xx status that is not a fence. */
public class ControlPlaneException extends RuntimeException {

    public ControlPlaneException(String message) {
        super(message);
    }

    public ControlPlaneException(String message, Throwable cause) {
        super(message, cause);
    }
}
