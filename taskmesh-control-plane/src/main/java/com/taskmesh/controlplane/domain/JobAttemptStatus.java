package com.taskmesh.controlplane.domain;

/**
 * Per-attempt lifecycle states, matching the {@code job_attempts_status_ck}
 * constraint in {@code V1__init.sql}.
 * <p>
 * Day 3 only ever writes {@link #RUNNING}, created when a job is claimed.
 * The terminal states are written when workers report results (Day 4) and
 * when the lease reaper expires an abandoned attempt (Day 4).
 */
public enum JobAttemptStatus {
    RUNNING,
    SUCCEEDED,
    FAILED,
    LEASE_EXPIRED,
    ABORTED
}
