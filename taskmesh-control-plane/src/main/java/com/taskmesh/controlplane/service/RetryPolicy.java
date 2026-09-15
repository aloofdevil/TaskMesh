package com.taskmesh.controlplane.service;

import java.time.Duration;

import org.springframework.stereotype.Component;

/**
 * Decides whether a failed job gets another attempt, and how long to wait
 * before it does.
 * <p>
 * Kept as pure computation with no database access so that both failure
 * paths - a worker reporting failure and the reaper finding an abandoned
 * lease - reach the same verdict from the same rule, rather than each
 * growing its own copy of the policy.
 */
@Component
public class RetryPolicy {

    private final RetryProperties properties;

    public RetryPolicy(RetryProperties properties) {
        this.properties = properties;
    }

    /**
     * Whether a job that has just consumed {@code attemptCount} attempts may
     * have another.
     * <p>
     * {@code attempt_count} is incremented when a job is claimed, so it is
     * the number of executions actually started. With {@code maxAttempts}
     * of 3 the third failure leaves {@code attemptCount == 3}, which is not
     * less than the budget, so the job is dead-lettered - exactly three
     * executions, never a fourth.
     */
    public boolean hasAttemptsRemaining(int attemptCount, int maxAttempts) {
        return attemptCount < maxAttempts;
    }

    /**
     * Exponential backoff: the delay doubles with each attempt already
     * consumed, capped at {@code maxBackoff}. With the defaults (1s initial)
     * that is 1s after the first failure, 2s after the second, 4s after the
     * third, and so on.
     */
    public Duration backoffAfterAttempt(int attemptCount) {
        int exponent = Math.max(0, attemptCount - 1);
        // Cap the exponent before shifting so a large attempt budget cannot
        // overflow the multiplication on its way to being capped anyway.
        if (exponent >= 32) {
            return properties.maxBackoff();
        }
        long multiplier = 1L << exponent;
        Duration backoff = properties.initialBackoff().multipliedBy(multiplier);
        return backoff.compareTo(properties.maxBackoff()) > 0 ? properties.maxBackoff() : backoff;
    }
}
