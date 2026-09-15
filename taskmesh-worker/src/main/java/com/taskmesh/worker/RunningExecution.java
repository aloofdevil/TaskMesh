package com.taskmesh.worker;

import java.util.concurrent.atomic.AtomicBoolean;

import com.taskmesh.common.worker.ClaimedJob;

/**
 * A job this worker is currently executing.
 * <p>
 * {@code fenced} is set when the control plane tells us this execution is
 * stale. It is read by the execution thread before it reports a result and
 * by the renewal task before it renews, so that once fenced, this worker
 * stops trying to influence the job at all. It is only a local
 * optimisation - the authoritative fence is the control plane's
 * conditional update, which would reject us anyway. Its value is in not
 * wasting calls and in not logging misleading success.
 */
class RunningExecution {

    private final ClaimedJob job;
    private final AtomicBoolean fenced = new AtomicBoolean(false);

    RunningExecution(ClaimedJob job) {
        this.job = job;
    }

    ClaimedJob job() {
        return job;
    }

    boolean isFenced() {
        return fenced.get();
    }

    void markFenced() {
        fenced.set(true);
    }
}
