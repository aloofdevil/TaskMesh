package com.taskmesh.worker;

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
 * The worker loop: register, heartbeat, poll for work.
 * <p>
 * Day 3 scope: this demonstrates the pull model end to end. A claimed job
 * is logged and the loop continues - the worker does not yet execute
 * payloads or report results, because the endpoints that accept a result
 * (and the fencing that makes accepting one safe) are Day 4. A job claimed
 * today therefore stays RUNNING, which is expected at this stage.
 */
@Component
@ConditionalOnProperty(prefix = "taskmesh.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class WorkerRuntime {

    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);

    private final ControlPlaneClient controlPlane;
    private final WorkerIdentity identity;
    private final WorkerProperties properties;

    /** Guards heartbeat/poll so they do nothing until registration has actually succeeded. */
    private volatile boolean registered;

    public WorkerRuntime(ControlPlaneClient controlPlane, WorkerIdentity identity, WorkerProperties properties) {
        this.controlPlane = controlPlane;
        this.identity = identity;
        this.properties = properties;
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

    @Scheduled(fixedDelayString = "${taskmesh.worker.poll-interval-ms}")
    public void pollForWork() {
        if (!registered) {
            return;
        }
        try {
            controlPlane.claim(identity.workerId()).ifPresent(this::onJobClaimed);
        } catch (RuntimeException e) {
            log.warn("Claim failed for worker {}: {}", identity.workerId(), e.getMessage());
        }
    }

    private void onJobClaimed(ClaimedJob job) {
        log.info("Claimed job {} (type={}, priority={}, attempt {}/{}, execution {})",
                job.jobId(), job.type(), job.priority(), job.attemptNumber(), job.maxAttempts(), job.executionId());
    }

    private void tryRegister() {
        try {
            controlPlane.register(new WorkerRegistrationRequest(
                    identity.workerId(), identity.hostname(), properties.capacity()));
            registered = true;
            log.info("Worker {} registered", identity.workerId());
        } catch (RuntimeException e) {
            log.warn("Registration failed for worker {}; will retry: {}", identity.workerId(), e.getMessage());
        }
    }

    @PreDestroy
    public void onShutdown() {
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
