package com.taskmesh.controlplane.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Lease and reaper tuning.
 * <p>
 * The relationship that matters is between {@code leaseDurationSeconds}
 * (30) and the worker's renewal interval (10s): a worker gets three
 * attempts to renew before its lease lapses, so a single slow request or
 * GC pause does not cost it the job. Shortening the lease detects crashes
 * sooner but makes spurious reassignment more likely; lengthening it does
 * the reverse.
 *
 * @param leaseDurationSeconds how long a claim or renewal holds the job
 * @param reaperIntervalMs     how often the reaper sweeps for lapsed leases
 * @param reaperBatchSize      maximum rows one reaper pass handles
 */
@ConfigurationProperties(prefix = "taskmesh.reliability")
public record ReliabilityProperties(
        int leaseDurationSeconds,
        long reaperIntervalMs,
        int reaperBatchSize) {
}
