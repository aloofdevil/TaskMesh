package com.taskmesh.controlplane.service;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Retry backoff tuning.
 *
 * @param initialBackoff delay after the first failed attempt; doubles per attempt
 * @param maxBackoff     ceiling for the doubling, so a job with a large attempt
 *                       budget cannot end up scheduled absurdly far in the future
 */
@ConfigurationProperties(prefix = "taskmesh.retry")
public record RetryProperties(Duration initialBackoff, Duration maxBackoff) {
}
