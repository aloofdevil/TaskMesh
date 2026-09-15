package com.taskmesh.controlplane.domain;

/**
 * The job lifecycle states, as decided in the approved architecture
 * (docs/architecture.md, "Job state machine"). Deliberately does not include
 * a persistent {@code SUBMITTED} state (unobservable - jobs are born QUEUED)
 * or a terminal {@code FAILED} state (failure resolves immediately to either
 * RETRYING or DEAD_LETTER, so a "failed" query never returns jobs that are
 * mid-retry).
 * <p>
 * Day 2 only creates jobs in {@link #QUEUED} and only implements the
 * {@code QUEUED -> CANCELLED} transition. The remaining transitions are
 * implemented by the scheduler, worker-reporting, and retry logic in later
 * days.
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    RETRYING,
    COMPLETED,
    DEAD_LETTER,
    CANCELLED
}
