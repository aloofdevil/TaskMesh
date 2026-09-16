package com.taskmesh.worker;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;

import jakarta.annotation.PreDestroy;

/**
 * The worker loop: register, heartbeat, poll for work, execute, renew the
 * lease while executing, and report the outcome.
 * <p>
 * Execution itself is deliberately trivial - a sleep standing in for real
 * work - because what Day 4 is about is the reliability protocol around
 * it: holding a lease, renewing it, and accepting being fenced when the
 * lease is lost.
 */
@Component
@ConditionalOnProperty(prefix = "taskmesh.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class WorkerRuntime {

    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);

    private final ControlPlaneClient controlPlane;
    private final WorkerIdentity identity;
    private final WorkerProperties properties;

    /** Jobs currently being executed, keyed by execution id. Bounds concurrency to the advertised capacity. */
    private final Map<UUID, RunningExecution> inFlight = new ConcurrentHashMap<>();
    private final ExecutorService executor;

    /** Guards heartbeat/poll so they do nothing until registration has actually succeeded. */
    private volatile boolean registered;

    /** Set on SIGTERM. Once true this worker claims nothing further. */
    private volatile boolean shuttingDown;

    public WorkerRuntime(ControlPlaneClient controlPlane, WorkerIdentity identity, WorkerProperties properties) {
        this.controlPlane = controlPlane;
        this.identity = identity;
        this.properties = properties;
        this.executor = Executors.newFixedThreadPool(properties.capacity());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        log.info("Worker {} starting against control plane {}", identity.workerId(), properties.controlPlaneUrl());
        tryRegister();
    }

    /**
     * Heartbeats, and doubles as the registration retry. The control plane
     * may not be up yet when the worker starts, and a control plane that
     * has lost this worker's row answers 404 - both are recoverable by
     * simply registering again rather than by crashing the worker.
     */
    @Scheduled(fixedDelayString = "${taskmesh.worker.heartbeat-interval-ms}")
    public void heartbeat() {
        if (!registered) {
            tryRegister();
            return;
        }
        try {
            controlPlane.heartbeat(identity.workerId());
        } catch (HttpClientErrorException.NotFound e) {
            log.warn("Control plane does not know worker {}; re-registering", identity.workerId());
            registered = false;
        } catch (RuntimeException e) {
            log.warn("Heartbeat failed for worker {}: {}", identity.workerId(), e.getMessage());
        }
    }

    /**
     * Renews the lease on every job still in flight.
     * <p>
     * Runs on its own schedule rather than from the execution threads so
     * that a job whose work blocks for a long time still keeps its lease -
     * tying renewal to execution progress would let a slow-but-healthy job
     * lose its lease and be pointlessly reassigned.
     */
    @Scheduled(fixedDelayString = "${taskmesh.worker.lease-renew-interval-ms}")
    public void renewLeases() {
        inFlight.values().stream()
                .filter(execution -> !execution.isFenced())
                .forEach(this::renewLease);
    }

    private void renewLease(RunningExecution execution) {
        ClaimedJob job = execution.job();
        try {
            controlPlane.renewLease(job.jobId(), identity.workerId(), job.executionId());
        } catch (StaleExecutionException e) {
            // The lease lapsed and the job has moved on without us. Stop
            // renewing and, crucially, stop intending to report a result.
            log.warn("Fenced while renewing job {} (execution {}); abandoning it", job.jobId(), job.executionId());
            execution.markFenced();
        } catch (RuntimeException e) {
            // A transient failure is not a fence: the lease has not
            // necessarily lapsed, and the next tick may well succeed.
            log.warn("Lease renewal failed for job {}: {}", job.jobId(), e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "${taskmesh.worker.poll-interval-ms}")
    public void pollForWork() {
        // Stopping claiming is the first thing shutdown does: taking on new
        // work while terminating would only guarantee it gets abandoned.
        if (shuttingDown || !registered || inFlight.size() >= properties.capacity()) {
            return;
        }
        try {
            controlPlane.claim(identity.workerId()).ifPresent(this::startExecution);
        } catch (RuntimeException e) {
            log.warn("Claim failed for worker {}: {}", identity.workerId(), e.getMessage());
        }
    }

    private void startExecution(ClaimedJob job) {
        RunningExecution execution = new RunningExecution(job);
        inFlight.put(job.executionId(), execution);
        log.info("Claimed job {} (type={}, priority={}, attempt {}/{}, execution {}, lease until {})",
                job.jobId(), job.type(), job.priority(), job.attemptNumber(), job.maxAttempts(),
                job.executionId(), job.leaseUntil());

        executor.submit(() -> runJob(execution));
    }

    private void runJob(RunningExecution execution) {
        ClaimedJob job = execution.job();
        try {
            doWork();
            reportOutcome(execution, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reportOutcome(execution, "Worker interrupted");
        } catch (RuntimeException e) {
            log.warn("Execution of job {} threw: {}", job.jobId(), e.getMessage());
            reportOutcome(execution, e.getMessage());
        } finally {
            inFlight.remove(job.executionId());
        }
    }

    /**
     * Reports success (null reason) or failure, unless this execution has
     * already been fenced - in which case another execution owns the job
     * and reporting would be trying to overwrite it.
     */
    private void reportOutcome(RunningExecution execution, String failureReason) {
        ClaimedJob job = execution.job();
        if (execution.isFenced()) {
            log.warn("Not reporting job {} (execution {}): this execution was fenced",
                    job.jobId(), job.executionId());
            return;
        }
        try {
            if (failureReason == null) {
                controlPlane.complete(job.jobId(), identity.workerId(), job.executionId());
                log.info("Completed job {} (execution {})", job.jobId(), job.executionId());
            } else {
                controlPlane.fail(job.jobId(), identity.workerId(), job.executionId(), failureReason);
                log.info("Reported failure for job {} (execution {}): {}",
                        job.jobId(), job.executionId(), failureReason);
            }
        } catch (StaleExecutionException e) {
            // We lost the lease at some point during execution. The job has
            // been reassigned, so this result is no longer ours to report.
            execution.markFenced();
            log.warn("Result for job {} rejected as stale (execution {}); another execution owns it now",
                    job.jobId(), job.executionId());
        } catch (RuntimeException e) {
            // Nothing is reported. The lease will lapse and the reaper will
            // requeue the job - at-least-once, by design.
            log.warn("Could not report outcome for job {}: {}", job.jobId(), e.getMessage());
        }
    }

    /** Stands in for real work. */
    private void doWork() throws InterruptedException {
        Thread.sleep(properties.jobDurationMs());
    }

    /**
     * Synchronized because two callers race here: {@link #onStartup()} on
     * the main thread and {@link #heartbeat()} on a scheduler thread, which
     * both register when the flag is still false. Without this they issue
     * two concurrent registrations for the same id, and the control plane's
     * find-then-insert upsert turns the loser into a duplicate-key 500.
     * The second caller re-checks the flag and finds the work already done.
     */
    private synchronized void tryRegister() {
        if (registered) {
            return;
        }
        try {
            controlPlane.register(new WorkerRegistrationRequest(
                    identity.workerId(), identity.hostname(), properties.capacity()));
            registered = true;
            log.info("Worker {} registered", identity.workerId());
        } catch (RuntimeException e) {
            log.warn("Registration failed for worker {}; will retry: {}", identity.workerId(), e.getMessage());
        }
    }

    /**
     * Graceful shutdown, as Kubernetes drives it: SIGTERM arrives, this runs,
     * and the pod is killed outright once {@code terminationGracePeriodSeconds}
     * elapses.
     * <p>
     * The order matters. Claiming stops first, so nothing new is taken on.
     * In-flight jobs are then given up to {@code shutdown-drain-ms} to finish
     * and report their own results - finishing honestly is better than
     * abandoning work that was nearly done, and a job that reports success
     * here never needs re-running. Only then does the worker deregister and
     * exit.
     * <p>
     * Draining deliberately does not renew leases. The drain window is kept
     * comfortably shorter than the 30s lease, so a job that started draining
     * with a freshly renewed lease still holds it throughout. Extending
     * leases while shutting down would only delay recovery of work this
     * process is about to abandon.
     * <p>
     * If a job cannot finish in the window it is simply left. Nothing is
     * reported for it, so fencing is untouched: its lease lapses, the reaper
     * recovers it, and another worker runs it under a new execution id. That
     * is the existing safety net and shutdown relies on it rather than trying
     * to do anything cleverer.
     */
    @PreDestroy
    public void onShutdown() {
        shuttingDown = true;
        log.info("Worker {} shutting down; no longer claiming work ({} in flight)",
                identity.workerId(), inFlight.size());

        // shutdown(), not shutdownNow(): let running jobs finish rather than
        // interrupting them mid-execution.
        executor.shutdown();
        try {
            if (!executor.awaitTermination(properties.shutdownDrainMs(), TimeUnit.MILLISECONDS)) {
                log.warn("Worker {} still had {} job(s) running after {}ms; abandoning them - their leases will "
                        + "expire and the control plane will reassign them",
                        identity.workerId(), inFlight.size(), properties.shutdownDrainMs());
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }

        if (!registered) {
            return;
        }
        try {
            controlPlane.deregister(identity.workerId());
            log.info("Worker {} deregistered on shutdown", identity.workerId());
        } catch (RuntimeException e) {
            log.warn("Deregistration failed for worker {}: {}", identity.workerId(), e.getMessage());
        }
    }
}
