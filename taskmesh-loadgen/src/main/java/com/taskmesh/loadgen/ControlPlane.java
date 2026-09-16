package com.taskmesh.loadgen;

import java.util.Optional;
import java.util.UUID;

import com.taskmesh.common.worker.ClaimedJob;

/**
 * The worker protocol, as the control plane actually exposes it.
 * <p>
 * An interface rather than a concrete client so the logical worker's
 * lifecycle can be driven against an in-memory implementation in tests -
 * real sequencing, real state transitions, no network and no mocking
 * framework asserting that a method was called.
 */
public interface ControlPlane {

    /** POST /internal/workers/register */
    void register(String workerId, String hostname, int capacity);

    /** POST /internal/workers/{workerId}/heartbeat */
    void heartbeat(String workerId);

    /** POST /internal/workers/{workerId}/deregister */
    void deregister(String workerId);

    /** POST /internal/workers/{workerId}/claim - empty when the control plane answers 204. */
    Optional<ClaimedJob> claim(String workerId);

    /**
     * POST /internal/jobs/{jobId}/lease/renew
     *
     * @throws StaleExecutionException when the control plane fences this execution
     */
    void renewLease(UUID jobId, String workerId, UUID executionId);

    /**
     * POST /internal/jobs/{jobId}/complete
     *
     * @throws StaleExecutionException when the control plane fences this execution
     */
    void complete(UUID jobId, String workerId, UUID executionId);

    /**
     * POST /internal/jobs/{jobId}/fail
     *
     * @throws StaleExecutionException when the control plane fences this execution
     */
    void fail(UUID jobId, String workerId, UUID executionId, String failureReason);
}
