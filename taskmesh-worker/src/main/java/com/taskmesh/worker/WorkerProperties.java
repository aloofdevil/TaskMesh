package com.taskmesh.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Worker configuration. Every value is environment-driven (see
 * {@code application.yml}) - notably the control plane URL, which is never
 * hardcoded so the same image runs under Docker Compose and Kubernetes.
 *
 * @param enabled              whether the register/heartbeat/poll loop runs at all; disabled in tests
 * @param controlPlaneUrl      base URL of the control plane
 * @param id                   this worker's id; generated from the hostname when blank
 * @param capacity             how many jobs this worker advertises it can run, and the size of its executor
 * @param pollIntervalMs       delay between claim attempts
 * @param heartbeatIntervalMs  delay between heartbeats
 * @param leaseRenewIntervalMs delay between lease renewals; must stay well below the control plane's
 *                             lease duration (10s against 30s) so a single failed renewal is survivable
 * @param jobDurationMs        how long the stand-in job execution sleeps for
 */
@ConfigurationProperties(prefix = "taskmesh.worker")
public record WorkerProperties(
        boolean enabled,
        String controlPlaneUrl,
        String id,
        int capacity,
        long pollIntervalMs,
        long heartbeatIntervalMs,
        long leaseRenewIntervalMs,
        long jobDurationMs) {
}
