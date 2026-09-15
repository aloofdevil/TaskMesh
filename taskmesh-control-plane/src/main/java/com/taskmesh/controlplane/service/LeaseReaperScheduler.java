package com.taskmesh.controlplane.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives {@link LeaseReaperService} on a timer. Kept as its own bean so
 * that the sweep logic can be exercised deterministically without a
 * background timer firing in the middle of a test.
 * <p>
 * An exception here must not kill the schedule, so failures are logged and
 * the next tick simply tries again - a reaper that silently stopped would
 * leave crashed workers' jobs stuck in RUNNING forever.
 */
@Component
@ConditionalOnProperty(prefix = "taskmesh.reliability", name = "reaper-enabled", havingValue = "true",
        matchIfMissing = true)
public class LeaseReaperScheduler {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaperScheduler.class);

    private final LeaseReaperService reaper;

    public LeaseReaperScheduler(LeaseReaperService reaper) {
        this.reaper = reaper;
    }

    @Scheduled(fixedDelayString = "${taskmesh.reliability.reaper-interval-ms}")
    public void sweep() {
        try {
            int recovered = reaper.reapExpiredLeases();
            if (recovered > 0) {
                log.info("Lease reaper recovered {} job(s)", recovered);
            }
        } catch (RuntimeException e) {
            log.error("Lease reaper sweep failed; will retry on next tick", e);
        }
    }
}
