package com.taskmesh.controlplane.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

/**
 * Drives {@link OutboxPublisher} on a timer. Split from the publisher for
 * the same reason as the lease reaper: so tests can publish at an exact
 * moment instead of racing a background timer.
 * <p>
 * {@code taskmesh.outbox.publisher-concurrency} controls how many
 * {@code publishPending()} passes run at once. At the default of 1 this
 * class behaves exactly as it always has: one call, on the scheduler
 * thread, per tick.
 * <p>
 * Running several passes concurrently is safe because the outbox already
 * carries the ownership mechanism it needs -
 * {@code JobEventRepository.lockUnpublishedBatch} selects
 * {@code FOR UPDATE SKIP LOCKED}, so concurrent passes take disjoint
 * batches rather than colliding, and {@code markPublished} is guarded on
 * {@code published_at IS NULL} so it stays idempotent. No schema change was
 * needed to allow this.
 * <p>
 * The property that weakens is ordering. A single pass publishes its batch
 * in {@code id} order, so no event can overtake an earlier one. With several
 * passes in flight two batches race, so in principle events for the same
 * aggregate could reach Kafka out of {@code id} order, and global ordering
 * across all events is no longer guaranteed.
 * <p>
 * Measured, that risk did not materialise: across concurrencies 1, 2, 4 and
 * 8, all 30,000 job events per run arrived with zero duplicates and zero
 * per-aggregate ordering violations (see {@code docs/day15-outbox-scalability.md}).
 * Events for one job are recorded seconds apart, so they land in different
 * batches that are still drained in id order. The guarantee is weaker than
 * at concurrency 1 even though this workload never exercised the difference.
 * Delivery remains at-least-once either way, which is what the architecture
 * already promises.
 */
@Component
@ConditionalOnProperty(prefix = "taskmesh.outbox", name = "publisher-enabled", havingValue = "true",
        matchIfMissing = true)
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final OutboxPublisher publisher;
    private final int concurrency;

    /** Null at concurrency 1, so the default configuration allocates no threads at all. */
    private final ExecutorService executor;

    public OutboxPublisherScheduler(OutboxPublisher publisher, OutboxProperties properties) {
        this.publisher = publisher;
        this.concurrency = Math.max(1, properties.publisherConcurrency());
        this.executor = this.concurrency == 1 ? null : createExecutor(this.concurrency);
        if (this.concurrency > 1) {
            log.info("Outbox publisher running with concurrency {}", this.concurrency);
        }
    }

    private static ExecutorService createExecutor(int concurrency) {
        AtomicInteger counter = new AtomicInteger();
        // A pool of its own rather than the shared scheduler pool: publisher
        // passes block on Kafka acknowledgements, and borrowing scheduler
        // threads for that would starve the lease reaper - the one task that
        // must keep running when other things are failing.
        return Executors.newFixedThreadPool(concurrency, runnable -> {
            Thread thread = new Thread(runnable, "outbox-publisher-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Scheduled(fixedDelayString = "${taskmesh.outbox.poll-interval-ms}")
    public void publish() {
        try {
            int published = concurrency == 1 ? publisher.publishPending() : publishConcurrently();
            if (published > 0) {
                log.debug("Published {} outbox event(s) to Kafka", published);
            }
        } catch (RuntimeException e) {
            // Never let a bad pass kill the schedule - unpublished events
            // would then sit in the outbox forever.
            log.error("Outbox publishing pass failed; will retry on next tick", e);
        }
    }

    /**
     * Runs {@code concurrency} passes and waits for all of them.
     * <p>
     * Waiting without a deadline is deliberate. Each pass is already bounded:
     * every send waits at most {@code taskmesh.outbox.send-timeout-ms} and the
     * pass stops at its first failure, so a pass cannot run away. Abandoning a
     * pass mid-flight would be worse than waiting - its transaction would
     * still hold row locks that the next tick would then skip over.
     */
    private int publishConcurrently() {
        List<Future<Integer>> futures = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            // Each pass runs on its own thread, so it gets its own
            // transaction and its own connection rather than sharing one.
            futures.add(executor.submit(publisher::publishPending));
        }

        int total = 0;
        for (Future<Integer> future : futures) {
            try {
                total += future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return total;
            } catch (ExecutionException e) {
                log.warn("An outbox publishing pass failed; its events stay unpublished", e.getCause());
            }
        }
        return total;
    }

    @PreDestroy
    public void shutdown() {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
