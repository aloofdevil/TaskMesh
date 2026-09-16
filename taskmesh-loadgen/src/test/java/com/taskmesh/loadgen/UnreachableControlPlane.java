package com.taskmesh.loadgen;

import java.util.Optional;
import java.util.UUID;

import com.taskmesh.common.worker.ClaimedJob;

/** A control plane that is simply not there - every call fails at the transport level. */
final class UnreachableControlPlane implements ControlPlane {

    @Override
    public void register(String workerId, String hostname, int capacity) {
        throw new ControlPlaneException("connection refused");
    }

    @Override
    public void heartbeat(String workerId) {
        throw new ControlPlaneException("connection refused");
    }

    @Override
    public void deregister(String workerId) {
        throw new ControlPlaneException("connection refused");
    }

    @Override
    public Optional<ClaimedJob> claim(String workerId) {
        throw new ControlPlaneException("connection refused");
    }

    @Override
    public void renewLease(UUID jobId, String workerId, UUID executionId) {
        throw new ControlPlaneException("connection refused");
    }

    @Override
    public void complete(UUID jobId, String workerId, UUID executionId) {
        throw new ControlPlaneException("connection refused");
    }

    @Override
    public void fail(UUID jobId, String workerId, UUID executionId, String failureReason) {
        throw new ControlPlaneException("connection refused");
    }
}
