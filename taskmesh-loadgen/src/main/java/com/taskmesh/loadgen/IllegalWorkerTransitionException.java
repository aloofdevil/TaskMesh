package com.taskmesh.loadgen;

/** Thrown when a logical worker is driven through a transition its state machine does not permit. */
public class IllegalWorkerTransitionException extends IllegalStateException {

    public IllegalWorkerTransitionException(String workerId, WorkerLifecycleState from, WorkerLifecycleState to) {
        super("Worker %s cannot move from %s to %s".formatted(workerId, from, to));
    }
}
