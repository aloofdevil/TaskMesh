package com.taskmesh.loadgen;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Everything the generator counts about itself.
 * <p>
 * These are <em>generator-side</em> numbers. They say what this process
 * offered and observed, never what the control plane did - job outcomes are
 * read back from PostgreSQL, because a load generator that grades its own
 * homework is not evidence.
 * <p>
 * The scheduling-delay histogram is the most important instrument here. It
 * records, for every due action, how late the generator actually was in
 * starting it. If that number stays small the generator kept up and the
 * measured latencies belong to the control plane; if it grows, the
 * generator itself is the bottleneck and every other latency figure in the
 * run is contaminated. That distinction is the whole reason this class
 * exists.
 */
public final class LoadGenMetrics {

    // ---- workers ----
    private final AtomicInteger workersConfigured = new AtomicInteger();
    private final AtomicInteger workersStarted = new AtomicInteger();
    private final AtomicInteger workersRegistered = new AtomicInteger();
    private final AtomicInteger registrationFailures = new AtomicInteger();
    private final AtomicInteger workersActive = new AtomicInteger();
    private final AtomicInteger peakWorkersActive = new AtomicInteger();
    private final AtomicInteger workersFailed = new AtomicInteger();
    private final AtomicInteger workersStopped = new AtomicInteger();
    private final AtomicInteger workersDeregistered = new AtomicInteger();

    // ---- heartbeats ----
    private final AtomicLong heartbeatAttempts = new AtomicLong();
    private final AtomicLong heartbeatSuccesses = new AtomicLong();
    private final AtomicLong heartbeatFailures = new AtomicLong();
    private final LatencyHistogram heartbeatLatency = new LatencyHistogram();

    // ---- polling ----
    private final AtomicLong pollAttempts = new AtomicLong();
    private final AtomicLong claims = new AtomicLong();
    private final AtomicLong emptyPolls = new AtomicLong();
    private final AtomicLong pollFailures = new AtomicLong();
    private final LatencyHistogram pollLatency = new LatencyHistogram();

    // ---- execution ----
    private final AtomicLong executionsStarted = new AtomicLong();
    private final AtomicLong executionsCompleted = new AtomicLong();
    private final AtomicLong executionsFailed = new AtomicLong();
    private final AtomicLong executionsAbandonedFenced = new AtomicLong();
    private final AtomicInteger executionsInFlight = new AtomicInteger();
    private final LatencyHistogram completionLatency = new LatencyHistogram();

    // ---- leases ----
    private final AtomicLong renewAttempts = new AtomicLong();
    private final AtomicLong renewSuccesses = new AtomicLong();
    private final AtomicLong renewFailures = new AtomicLong();
    private final AtomicLong staleExecutionResponses = new AtomicLong();
    private final LatencyHistogram renewLatency = new LatencyHistogram();

    // ---- registration ----
    private final LatencyHistogram registrationLatency = new LatencyHistogram();

    // ---- http ----
    private final AtomicLong httpRequests = new AtomicLong();
    private final AtomicLong httpSuccesses = new AtomicLong();
    private final AtomicLong httpFailures = new AtomicLong();
    private final Map<Integer, AtomicLong> statusCodes = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> transportErrors = new ConcurrentHashMap<>();

    // ---- generator scheduling ----
    private final LatencyHistogram schedulingDelay = new LatencyHistogram();
    private final AtomicLong actionsDue = new AtomicLong();

    public void workersConfigured(int n) {
        workersConfigured.set(n);
    }

    public void workerStarted() {
        workersStarted.incrementAndGet();
    }

    public void workerRegistered(long nanos) {
        workersRegistered.incrementAndGet();
        registrationLatency.recordNanos(nanos);
    }

    public void registrationFailed() {
        registrationFailures.incrementAndGet();
    }

    public void workerBecameActive() {
        int now = workersActive.incrementAndGet();
        peakWorkersActive.accumulateAndGet(now, Math::max);
    }

    public void workerFailed() {
        workersActive.updateAndGet(v -> Math.max(0, v - 1));
        workersFailed.incrementAndGet();
    }

    public void workerStopped() {
        workersActive.updateAndGet(v -> Math.max(0, v - 1));
        workersStopped.incrementAndGet();
    }

    public void workerDeregistered() {
        workersDeregistered.incrementAndGet();
    }

    public void heartbeatAttempt() {
        heartbeatAttempts.incrementAndGet();
    }

    public void heartbeatSuccess(long nanos) {
        heartbeatSuccesses.incrementAndGet();
        heartbeatLatency.recordNanos(nanos);
    }

    public void heartbeatFailure() {
        heartbeatFailures.incrementAndGet();
    }

    public void pollAttempt() {
        pollAttempts.incrementAndGet();
    }

    public void claimed(long nanos) {
        claims.incrementAndGet();
        pollLatency.recordNanos(nanos);
    }

    public void emptyPoll(long nanos) {
        emptyPolls.incrementAndGet();
        pollLatency.recordNanos(nanos);
    }

    public void pollFailure() {
        pollFailures.incrementAndGet();
    }

    public void executionStarted() {
        executionsStarted.incrementAndGet();
        executionsInFlight.incrementAndGet();
    }

    public void executionCompleted(long nanos) {
        executionsCompleted.incrementAndGet();
        executionsInFlight.updateAndGet(v -> Math.max(0, v - 1));
        completionLatency.recordNanos(nanos);
    }

    public void executionFailed() {
        executionsFailed.incrementAndGet();
        executionsInFlight.updateAndGet(v -> Math.max(0, v - 1));
    }

    public void executionAbandonedFenced() {
        executionsAbandonedFenced.incrementAndGet();
        executionsInFlight.updateAndGet(v -> Math.max(0, v - 1));
    }

    public void renewAttempt() {
        renewAttempts.incrementAndGet();
    }

    public void renewSuccess(long nanos) {
        renewSuccesses.incrementAndGet();
        renewLatency.recordNanos(nanos);
    }

    public void renewFailure() {
        renewFailures.incrementAndGet();
    }

    public void staleExecution() {
        staleExecutionResponses.incrementAndGet();
    }

    public void httpStatus(int status) {
        httpRequests.incrementAndGet();
        if (status >= 200 && status < 300) {
            httpSuccesses.incrementAndGet();
        } else {
            httpFailures.incrementAndGet();
        }
        statusCodes.computeIfAbsent(status, k -> new AtomicLong()).incrementAndGet();
    }

    public void httpTransportError(String kind) {
        httpRequests.incrementAndGet();
        httpFailures.incrementAndGet();
        transportErrors.computeIfAbsent(kind, k -> new AtomicLong()).incrementAndGet();
    }

    /** How late this action was against the time it was scheduled for. */
    public void schedulingDelayNanos(long nanos) {
        actionsDue.incrementAndGet();
        schedulingDelay.recordNanos(Math.max(0, nanos));
    }

    public int workersConfiguredCount() {
        return workersConfigured.get();
    }

    public int workersStartedCount() {
        return workersStarted.get();
    }

    public int workersRegisteredCount() {
        return workersRegistered.get();
    }

    public int registrationFailureCount() {
        return registrationFailures.get();
    }

    public int workersActiveCount() {
        return workersActive.get();
    }

    /** The high-water mark, which is what the offered request rate must be derived from. */
    public int peakWorkersActiveCount() {
        return peakWorkersActive.get();
    }

    public int workersFailedCount() {
        return workersFailed.get();
    }

    public int workersStoppedCount() {
        return workersStopped.get();
    }

    public int workersDeregisteredCount() {
        return workersDeregistered.get();
    }

    public long heartbeatAttemptCount() {
        return heartbeatAttempts.get();
    }

    public long heartbeatSuccessCount() {
        return heartbeatSuccesses.get();
    }

    public long heartbeatFailureCount() {
        return heartbeatFailures.get();
    }

    public long pollAttemptCount() {
        return pollAttempts.get();
    }

    public long claimCount() {
        return claims.get();
    }

    public long emptyPollCount() {
        return emptyPolls.get();
    }

    public long pollFailureCount() {
        return pollFailures.get();
    }

    public long executionsStartedCount() {
        return executionsStarted.get();
    }

    public long executionsCompletedCount() {
        return executionsCompleted.get();
    }

    public long executionsFailedCount() {
        return executionsFailed.get();
    }

    public long executionsAbandonedFencedCount() {
        return executionsAbandonedFenced.get();
    }

    public int executionsInFlightCount() {
        return executionsInFlight.get();
    }

    public long renewAttemptCount() {
        return renewAttempts.get();
    }

    public long renewSuccessCount() {
        return renewSuccesses.get();
    }

    public long renewFailureCount() {
        return renewFailures.get();
    }

    public long staleExecutionCount() {
        return staleExecutionResponses.get();
    }

    public long httpRequestCount() {
        return httpRequests.get();
    }

    public long httpSuccessCount() {
        return httpSuccesses.get();
    }

    public long httpFailureCount() {
        return httpFailures.get();
    }

    public Map<Integer, Long> statusCodeDistribution() {
        Map<Integer, Long> snapshot = new java.util.TreeMap<>();
        statusCodes.forEach((code, count) -> snapshot.put(code, count.get()));
        return snapshot;
    }

    public Map<String, Long> transportErrorDistribution() {
        Map<String, Long> snapshot = new java.util.TreeMap<>();
        transportErrors.forEach((kind, count) -> snapshot.put(kind, count.get()));
        return snapshot;
    }

    public LatencyHistogram registrationLatencyHistogram() {
        return registrationLatency;
    }

    public LatencyHistogram heartbeatLatencyHistogram() {
        return heartbeatLatency;
    }

    public LatencyHistogram pollLatencyHistogram() {
        return pollLatency;
    }

    public LatencyHistogram renewLatencyHistogram() {
        return renewLatency;
    }

    public LatencyHistogram completionLatencyHistogram() {
        return completionLatency;
    }

    public LatencyHistogram schedulingDelayHistogram() {
        return schedulingDelay;
    }

    public long actionsDueCount() {
        return actionsDue.get();
    }
}
