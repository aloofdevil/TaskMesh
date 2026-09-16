package com.taskmesh.loadgen;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.taskmesh.common.worker.ClaimedJob;

/**
 * An in-memory control plane that enforces the same rules the real one
 * does: a job goes to exactly one claimer, and only the current execution
 * id may renew or complete it.
 * <p>
 * This exists so the logical worker's lifecycle can be tested against real
 * sequencing and real fencing rather than against a mock that merely
 * records calls. A test asserting "complete() was invoked" would pass even
 * if the worker completed a job it had been fenced out of - which is the
 * one thing that must never happen.
 */
final class FakeControlPlane implements ControlPlane {

    private final Deque<ClaimedJob> availableJobs = new ArrayDeque<>();
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private final Map<UUID, UUID> currentExecutionByJob = new ConcurrentHashMap<>();

    final AtomicInteger registerCalls = new AtomicInteger();
    final AtomicInteger heartbeatCalls = new AtomicInteger();
    final AtomicInteger deregisterCalls = new AtomicInteger();
    final AtomicInteger claimCalls = new AtomicInteger();
    final AtomicInteger renewCalls = new AtomicInteger();
    final AtomicInteger completeCalls = new AtomicInteger();
    final AtomicInteger failCalls = new AtomicInteger();

    /** Completions accepted as authoritative - i.e. not fenced out. */
    final Set<UUID> acceptedCompletions = ConcurrentHashMap.newKeySet();

    synchronized void offerJob(UUID jobId) {
        UUID executionId = UUID.randomUUID();
        currentExecutionByJob.put(jobId, executionId);
        availableJobs.add(new ClaimedJob(jobId, executionId, "TEST_JOB", Map.of(), 5, 1, 3,
                Instant.now().plusSeconds(30)));
    }

    /** Simulates a reassignment: the job moves on under a brand new execution id. */
    synchronized void reassign(UUID jobId) {
        currentExecutionByJob.put(jobId, UUID.randomUUID());
    }

    @Override
    public void register(String workerId, String hostname, int capacity) {
        registerCalls.incrementAndGet();
        registered.add(workerId);
    }

    @Override
    public void heartbeat(String workerId) {
        heartbeatCalls.incrementAndGet();
        if (!registered.contains(workerId)) {
            throw new ControlPlaneException("HTTP 404 - unknown worker " + workerId);
        }
    }

    @Override
    public void deregister(String workerId) {
        deregisterCalls.incrementAndGet();
        registered.remove(workerId);
    }

    @Override
    public synchronized Optional<ClaimedJob> claim(String workerId) {
        claimCalls.incrementAndGet();
        return Optional.ofNullable(availableJobs.poll());
    }

    @Override
    public void renewLease(UUID jobId, String workerId, UUID executionId) {
        renewCalls.incrementAndGet();
        requireCurrent(jobId, executionId);
    }

    @Override
    public void complete(UUID jobId, String workerId, UUID executionId) {
        completeCalls.incrementAndGet();
        requireCurrent(jobId, executionId);
        acceptedCompletions.add(jobId);
    }

    @Override
    public void fail(UUID jobId, String workerId, UUID executionId, String failureReason) {
        failCalls.incrementAndGet();
        requireCurrent(jobId, executionId);
    }

    private void requireCurrent(UUID jobId, UUID executionId) {
        UUID current = currentExecutionByJob.get(jobId);
        if (!executionId.equals(current)) {
            throw new StaleExecutionException(executionId,
                    "{\"error\":\"STALE_EXECUTION\",\"message\":\"Execution " + executionId
                            + " no longer owns job " + jobId + "\"}");
        }
    }
}
