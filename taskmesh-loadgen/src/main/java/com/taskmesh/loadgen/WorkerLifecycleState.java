package com.taskmesh.loadgen;

import java.util.EnumSet;
import java.util.Set;

/**
 * The lifecycle of one logical worker.
 * <p>
 * Made explicit, with the legal transitions encoded here rather than left
 * implicit in the loop, so that a bug in the simulator surfaces as a loud
 * {@link IllegalWorkerTransitionException} instead of as a quietly wrong
 * benchmark number. A load generator that silently mis-sequences its own
 * workers would produce results that look plausible and mean nothing.
 *
 * <pre>
 * CREATED -> REGISTERING -> ACTIVE -> DRAINING -> STOPPED
 *                |             |          |
 *                +-------------+----------+--> FAILED
 * </pre>
 */
public enum WorkerLifecycleState {

    /** Constructed, not yet talking to the control plane. */
    CREATED,

    /** Registration request in flight. */
    REGISTERING,

    /** Registered; heartbeating, polling, executing and renewing. */
    ACTIVE,

    /** Shutting down: no new claims, finishing what is in flight. */
    DRAINING,

    /** Deregistered (or given up on deregistering); loop has exited. */
    STOPPED,

    /** Gave up - typically registration never succeeded. Terminal. */
    FAILED;

    private static final Set<WorkerLifecycleState> NONE = EnumSet.noneOf(WorkerLifecycleState.class);

    /**
     * Registration is allowed to be retried, so REGISTERING may loop back
     * to itself; that is how a worker survives a control plane that is not
     * up yet, exactly as the real {@code WorkerRuntime} does.
     */
    public Set<WorkerLifecycleState> allowedNext() {
        return switch (this) {
            case CREATED -> EnumSet.of(REGISTERING, STOPPED, FAILED);
            case REGISTERING -> EnumSet.of(REGISTERING, ACTIVE, DRAINING, STOPPED, FAILED);
            case ACTIVE -> EnumSet.of(DRAINING, STOPPED, FAILED);
            case DRAINING -> EnumSet.of(STOPPED, FAILED);
            case STOPPED, FAILED -> NONE;
        };
    }

    public boolean canTransitionTo(WorkerLifecycleState next) {
        return allowedNext().contains(next);
    }

    public boolean isTerminal() {
        return this == STOPPED || this == FAILED;
    }
}
