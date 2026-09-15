package com.taskmesh.controlplane.domain;

/**
 * Worker lifecycle states, matching the {@code workers_status_ck} constraint
 * in {@code V1__init.sql}.
 * <p>
 * Day 3 only ever writes {@link #ACTIVE} (on registration) and
 * {@link #DEREGISTERED} (on graceful shutdown). {@link #DEAD} exists in the
 * schema and here for completeness but is never written yet - the reaper
 * that detects missed heartbeats and marks workers DEAD is Day 4.
 */
public enum WorkerStatus {
    ACTIVE,
    DEAD,
    DEREGISTERED
}
