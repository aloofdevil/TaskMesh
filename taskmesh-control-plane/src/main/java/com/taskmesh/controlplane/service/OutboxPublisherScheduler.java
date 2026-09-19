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
        long tickStart = System.nanoTime();
        try {
            int published = concurrency == 1 ? publisher.publishPending() : publishConcurrently(tickStart);
            if (published > 0 && concurrency == 1) {
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
    private int publishConcurrently(long tickStart) {
        List<Future<PassResult>> futures = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            // Each pass runs on its own thread, so it gets its own
            // transaction and its own connection rather than sharing one.
            // The lambda only brackets the same call with two nanoTime
            // reads; it does not change what the pass does.
            futures.add(executor.submit(() -> {
                long passStart = System.nanoTime();
                int n = publisher.publishPending();
                long passEnd = System.nanoTime();
                long[] phases = publisher.lastPassPhases();
                return new PassResult(n, passStart, passEnd, phases[0], phases[1], phases[2]);
            }));
        }
        long submitEnd = System.nanoTime();

        // Day 19 instrumentation. How many passes have already finished by
        // the time the barrier is entered separates "the barrier waits" from
        // "the barrier is a formality": if most futures are already done, the
        // wait is not what costs.
        int doneAtEntry = 0;
        for (Future<PassResult> future : futures) {
            if (future.isDone()) {
                doneAtEntry++;
            }
        }
        long barrierStart = System.nanoTime();

        int total = 0;
        int completed = 0;
        long passSum = 0;
        long passMin = Long.MAX_VALUE;
        long passMax = 0;
        long lastEnd = 0;
        long claimSum = 0;
        long claimMax = 0;
        long sendSum = 0;
        long sendMax = 0;
        long markSum = 0;
        for (Future<PassResult> future : futures) {
            try {
                PassResult result = future.get();
                total += result.published();
                long duration = result.endNanos() - result.startNanos();
                passSum += duration;
                passMin = Math.min(passMin, duration);
                passMax = Math.max(passMax, duration);
                lastEnd = Math.max(lastEnd, result.endNanos());
                claimSum += result.claimNanos();
                claimMax = Math.max(claimMax, result.claimNanos());
                sendSum += result.sendNanos();
                sendMax = Math.max(sendMax, result.sendNanos());
                markSum += result.markNanos();
                completed++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return total;
            } catch (ExecutionException e) {
                log.warn("An outbox publishing pass failed; its events stay unpublished", e.getCause());
            }
        }
        long tickEnd = System.nanoTime();

        if (total > 0 && log.isDebugEnabled()) {
            // One aggregated record per tick - never one per event.
            log.debug("Published {} outbox event(s) to Kafka"
                            + " tick_us={} submit_us={} barrier_us={} passes={} completed={}"
                            + " done_at_entry={} pass_min_us={} pass_mean_us={} pass_max_us={}"
                            + " claim_mean_us={} claim_max_us={} send_mean_us={} send_max_us={}"
                            + " mark_mean_us={}"
                            + " straggler_us={} barrier_excess_us={}",
                    total,
                    (tickEnd - tickStart) / 1000,
                    (submitEnd - tickStart) / 1000,
                    (tickEnd - barrierStart) / 1000,
                    concurrency, completed, doneAtEntry,
                    passMin == Long.MAX_VALUE ? 0 : passMin / 1000,
                    completed == 0 ? 0 : (passSum / completed) / 1000,
                    passMax / 1000,
                    completed == 0 ? 0 : (claimSum / completed) / 1000,
                    claimMax / 1000,
                    completed == 0 ? 0 : (sendSum / completed) / 1000,
                    sendMax / 1000,
                    completed == 0 ? 0 : (markSum / completed) / 1000,
                    // Cost of waiting for the slowest pass rather than an
                    // average one - the straggler term the barrier imposes.
                    completed == 0 ? 0 : (passMax - passSum / completed) / 1000,
                    // Barrier time not explained by the slowest pass: thread
                    // hand-off, scheduling and any queueing.
                    lastEnd == 0 ? 0 : (tickEnd - lastEnd) / 1000);
        }
        return total;
    }

    /** What one publishing pass did and when, for the per-tick decomposition. */
    private record PassResult(int published, long startNanos, long endNanos,
            long claimNanos, long sendNanos, long markNanos) {
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
