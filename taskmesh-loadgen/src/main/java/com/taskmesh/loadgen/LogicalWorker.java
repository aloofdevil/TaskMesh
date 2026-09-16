package com.taskmesh.loadgen;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import com.taskmesh.common.worker.ClaimedJob;

/**
 * One simulated worker: its own identity, schedules, capacity and in-flight
 * executions, driven by a single deadline loop.
 * <p>
 * <strong>One logical worker is one virtual thread</strong> - not a thread
 * per scheduled activity. The real {@code WorkerRuntime} has three
 * {@code @Scheduled} methods plus an execution pool; naively mirroring that
 * would cost four threads per worker, or 100,000 threads at the 25,000
 * target. Instead this loop keeps four kinds of deadline (heartbeat, poll,
 * lease renewal, and each execution's finish time), sleeps until the
 * earliest, and acts on whatever is due.
 * <p>
 * The behavioural consequences of that choice are deliberate and match the
 * real worker rather than working around it:
 * <ul>
 * <li>HTTP calls block the loop, exactly as they occupy a scheduler thread
 * in the real worker. Blocking parks the virtual thread instead of an OS
 * thread, so it stays affordable at scale.</li>
 * <li>Because calls block, other deadlines can slip. That is real worker
 * behaviour, and the slip is measured as scheduling delay rather than
 * hidden - it is how generator saturation is detected.</li>
 * <li>Polling stops while {@code inFlight == capacity}, as the real
 * {@code pollForWork()} does.</li>
 * <li>A fenced execution is abandoned silently: no completion, no failure
 * report. Reporting anything for it would corrupt the very invariant the
 * benchmark exists to check.</li>
 * </ul>
 * <p>
 * State is confined to this object and touched only by its own virtual
 * thread, except {@link #requestStop()} and {@link #state()}.
 */
public final class LogicalWorker implements Runnable {

    /** How long a worker keeps retrying registration before being declared FAILED. */
    private static final int MAX_REGISTRATION_ATTEMPTS = 5;

    private final String workerId;
    private final String hostname;
    private final LoadGenConfig config;
    private final ControlPlane controlPlane;
    private final LoadGenMetrics metrics;

    /** Per-worker phase offsets, so the fleet does not act in lockstep. */
    private final long heartbeatOffsetNanos;
    private final long pollOffsetNanos;
    private final long renewOffsetNanos;

    private volatile WorkerLifecycleState state = WorkerLifecycleState.CREATED;
    private volatile boolean stopRequested;

    private final Map<UUID, Execution> inFlight = new LinkedHashMap<>();

    private long nextHeartbeatNanos;
    private long nextPollNanos;
    private long nextRenewNanos;

    /** One claimed job held by this worker. */
    private static final class Execution {
        final ClaimedJob job;
        final long finishAtNanos;
        final long startedAtNanos;
        boolean fenced;

        Execution(ClaimedJob job, long finishAtNanos, long startedAtNanos) {
            this.job = job;
            this.finishAtNanos = finishAtNanos;
            this.startedAtNanos = startedAtNanos;
        }
    }

    public LogicalWorker(String workerId, String hostname, LoadGenConfig config,
            ControlPlane controlPlane, LoadGenMetrics metrics, Random jitter) {
        this.workerId = workerId;
        this.hostname = hostname;
        this.config = config;
        this.controlPlane = controlPlane;
        this.metrics = metrics;
        // A uniform offset across each interval. Without this, every worker
        // fires on the same absolute boundaries and 25,000 workers would
        // deliver one enormous burst per second instead of a steady rate -
        // measuring a thundering herd rather than the control plane.
        this.heartbeatOffsetNanos = nextOffset(jitter, config.heartbeatInterval());
        this.pollOffsetNanos = nextOffset(jitter, config.pollInterval());
        this.renewOffsetNanos = nextOffset(jitter, config.leaseRenewInterval());
    }

    private static long nextOffset(Random random, Duration interval) {
        long nanos = interval.toNanos();
        return nanos <= 0 ? 0 : Math.floorMod(random.nextLong(), nanos);
    }

    public String workerId() {
        return workerId;
    }

    public WorkerLifecycleState state() {
        return state;
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    /** Asked to wind down. Checked by the loop; safe to call from any thread. */
    public void requestStop() {
        this.stopRequested = true;
    }

    private void transitionTo(WorkerLifecycleState next) {
        WorkerLifecycleState current = state;
        if (current == next) {
            return;
        }
        if (!current.canTransitionTo(next)) {
            throw new IllegalWorkerTransitionException(workerId, current, next);
        }
        state = next;
    }

    @Override
    public void run() {
        metrics.workerStarted();
        try {
            if (!register()) {
                transitionTo(WorkerLifecycleState.FAILED);
                metrics.workerFailed();
                return;
            }
            transitionTo(WorkerLifecycleState.ACTIVE);
            metrics.workerBecameActive();

            long now = System.nanoTime();
            nextHeartbeatNanos = now + heartbeatOffsetNanos;
            nextPollNanos = now + pollOffsetNanos;
            nextRenewNanos = now + renewOffsetNanos;

            runLoop();
            drain();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            if (!state.isTerminal()) {
                transitionTo(WorkerLifecycleState.FAILED);
                metrics.workerFailed();
            }
            return;
        }
        if (!state.isTerminal()) {
            transitionTo(WorkerLifecycleState.STOPPED);
            metrics.workerStopped();
        }
    }

    private boolean register() throws InterruptedException {
        transitionTo(WorkerLifecycleState.REGISTERING);
        for (int attempt = 1; attempt <= MAX_REGISTRATION_ATTEMPTS && !stopRequested; attempt++) {
            long start = System.nanoTime();
            try {
                controlPlane.register(workerId, hostname, config.capacity());
                metrics.workerRegistered(System.nanoTime() - start);
                return true;
            } catch (RuntimeException e) {
                metrics.registrationFailed();
                if (attempt == MAX_REGISTRATION_ATTEMPTS) {
                    return false;
                }
                Thread.sleep(Duration.ofMillis(200L * attempt));
            }
        }
        return false;
    }

    private void runLoop() throws InterruptedException {
        while (!stopRequested) {
            long now = System.nanoTime();
            long due = earliestDeadline(now);
            long sleepNanos = due - now;
            if (sleepNanos > 0) {
                // Bounded so a stop request is noticed promptly even when the
                // next real deadline is far away.
                Thread.sleep(Duration.ofNanos(Math.min(sleepNanos, Duration.ofMillis(250).toNanos())));
                if (System.nanoTime() < due) {
                    continue;
                }
            }
            long actualStart = System.nanoTime();
            metrics.schedulingDelayNanos(actualStart - due);
            fireDueActions(actualStart);
        }
    }

    private long earliestDeadline(long now) {
        long due = nextHeartbeatNanos;
        if (canPoll() && nextPollNanos < due) {
            due = nextPollNanos;
        }
        if (!inFlight.isEmpty() && nextRenewNanos < due) {
            due = nextRenewNanos;
        }
        for (Execution execution : inFlight.values()) {
            if (execution.finishAtNanos < due) {
                due = execution.finishAtNanos;
            }
        }
        return Math.max(due, now - Duration.ofSeconds(1).toNanos());
    }

    private boolean canPoll() {
        return !stopRequested && state == WorkerLifecycleState.ACTIVE && inFlight.size() < config.capacity();
    }

    private void fireDueActions(long now) {
        if (now >= nextHeartbeatNanos) {
            doHeartbeat();
            nextHeartbeatNanos = advance(nextHeartbeatNanos, config.heartbeatInterval(), now);
        }
        finishDueExecutions(now);
        if (!inFlight.isEmpty() && now >= nextRenewNanos) {
            renewLeases();
            nextRenewNanos = advance(nextRenewNanos, config.leaseRenewInterval(), now);
        }
        if (canPoll() && now >= nextPollNanos) {
            doPoll();
            nextPollNanos = advance(nextPollNanos, config.pollInterval(), now);
        }
    }

    /**
     * Advances a deadline by whole intervals. Skipping missed ticks rather
     * than replaying them keeps a worker that fell behind from firing a
     * catch-up burst, which would distort the offered rate.
     */
    private static long advance(long deadline, Duration interval, long now) {
        long step = interval.toNanos();
        long next = deadline + step;
        if (next <= now) {
            long missed = ((now - deadline) / step) + 1;
            next = deadline + (missed * step);
        }
        return next;
    }

    private void doHeartbeat() {
        metrics.heartbeatAttempt();
        long start = System.nanoTime();
        try {
            controlPlane.heartbeat(workerId);
            metrics.heartbeatSuccess(System.nanoTime() - start);
        } catch (RuntimeException e) {
            metrics.heartbeatFailure();
        }
    }

    private void doPoll() {
        metrics.pollAttempt();
        long start = System.nanoTime();
        try {
            Optional<ClaimedJob> claimed = controlPlane.claim(workerId);
            long elapsed = System.nanoTime() - start;
            if (claimed.isEmpty()) {
                metrics.emptyPoll(elapsed);
                return;
            }
            metrics.claimed(elapsed);
            startExecution(claimed.get());
        } catch (RuntimeException e) {
            metrics.pollFailure();
        }
    }

    private void startExecution(ClaimedJob job) {
        long now = System.nanoTime();
        inFlight.put(job.executionId(), new Execution(job, now + config.jobDuration().toNanos(), now));
        metrics.executionStarted();
    }

    /**
     * Simulated work is a deadline, not a sleep and not a busy loop: the
     * point is to exercise the TaskMesh protocol, not to burn generator CPU
     * that would then be mistaken for control-plane cost.
     */
    private void finishDueExecutions(long now) {
        List<Execution> finished = new ArrayList<>();
        for (Execution execution : inFlight.values()) {
            if (now >= execution.finishAtNanos) {
                finished.add(execution);
            }
        }
        for (Execution execution : finished) {
            reportCompletion(execution);
        }
    }

    private void reportCompletion(Execution execution) {
        inFlight.remove(execution.job.executionId());
        if (execution.fenced) {
            // Fenced while running: the job belongs to another execution now.
            // Reporting anything here would be exactly the bug that fencing
            // exists to prevent.
            metrics.executionAbandonedFenced();
            return;
        }
        long start = System.nanoTime();
        try {
            controlPlane.complete(execution.job.jobId(), workerId, execution.job.executionId());
            metrics.executionCompleted(System.nanoTime() - start);
        } catch (StaleExecutionException e) {
            metrics.staleExecution();
            metrics.executionAbandonedFenced();
        } catch (RuntimeException e) {
            metrics.executionFailed();
        }
    }

    private void renewLeases() {
        List<Execution> current = new ArrayList<>(inFlight.values());
        for (Execution execution : current) {
            if (execution.fenced) {
                continue;
            }
            metrics.renewAttempt();
            long start = System.nanoTime();
            try {
                controlPlane.renewLease(execution.job.jobId(), workerId, execution.job.executionId());
                metrics.renewSuccess(System.nanoTime() - start);
            } catch (StaleExecutionException e) {
                // Stop renewing and stop intending to report a result, exactly
                // as the real worker does on a fence.
                execution.fenced = true;
                metrics.staleExecution();
            } catch (RuntimeException e) {
                // A transient failure is not a fence; the lease may well survive.
                metrics.renewFailure();
            }
        }
    }

    /**
     * Winds down: stop claiming (already guaranteed by {@code stopRequested}),
     * let in-flight executions finish and report, then deregister. Mirrors the
     * real worker's shutdown ordering so the benchmark leaves no orphaned
     * RUNNING jobs behind.
     */
    private void drain() throws InterruptedException {
        transitionTo(WorkerLifecycleState.DRAINING);

        long deadline = System.nanoTime() + config.jobDuration().toNanos() + Duration.ofSeconds(5).toNanos();
        while (!inFlight.isEmpty() && System.nanoTime() < deadline) {
            long now = System.nanoTime();
            finishDueExecutions(now);
            if (!inFlight.isEmpty()) {
                Thread.sleep(Duration.ofMillis(50));
            }
        }

        try {
            controlPlane.deregister(workerId);
            metrics.workerDeregistered();
        } catch (RuntimeException e) {
            // Deregistration is best-effort: the control plane's lease reaper
            // recovers anything this worker still held.
        }
    }
}
