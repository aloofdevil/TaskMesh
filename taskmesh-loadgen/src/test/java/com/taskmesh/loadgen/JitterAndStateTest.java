package com.taskmesh.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * Jitter, seeding and the lifecycle state machine - the three things that,
 * if wrong, would invalidate a benchmark without failing anything loudly.
 */
class JitterAndStateTest {

    private static LoadGenConfig config() {
        return LoadGenConfig.parse(new String[] {"--workers=1000", "--seed=4242"});
    }

    /**
     * The anti-thundering-herd property. With a 1s poll interval and 1,000
     * workers, first-poll times must be spread across the whole second
     * rather than landing on the same boundary - otherwise the benchmark
     * measures a once-per-second stampede instead of a steady rate.
     */
    @Test
    void phaseOffsetsAreSpreadAcrossTheInterval() {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        Random seeded = new Random(config().seed());

        int workers = 1000;
        int buckets = 10;
        int[] histogram = new int[buckets];
        long intervalNanos = Duration.ofSeconds(1).toNanos();

        for (int i = 0; i < workers; i++) {
            LogicalWorker worker = new LogicalWorker("w-" + i, "h", config(), controlPlane, metrics, seeded);
            long offset = pollOffsetOf(worker);
            assertThat(offset).isBetween(0L, intervalNanos - 1);
            histogram[(int) (offset * buckets / intervalNanos)]++;
        }

        // Every tenth of the interval should receive a meaningful share.
        // A perfectly uniform split would be 100 per bucket.
        for (int bucket = 0; bucket < buckets; bucket++) {
            assertThat(histogram[bucket])
                    .as("bucket %d of the poll interval should not be empty or dominant", bucket)
                    .isBetween(40, 200);
        }
    }

    @Test
    void sameSeedProducesTheSameOffsetsSoRunsAreReproducible() {
        List<Long> first = offsetsForSeed(777);
        List<Long> second = offsetsForSeed(777);
        List<Long> different = offsetsForSeed(778);

        assertThat(first).isEqualTo(second);
        assertThat(first).isNotEqualTo(different);
    }

    private static List<Long> offsetsForSeed(long seed) {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        LoadGenConfig config = LoadGenConfig.parse(new String[] {"--workers=50", "--seed=" + seed});
        Random random = new Random(seed);
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            offsets.add(pollOffsetOf(new LogicalWorker("w-" + i, "h", config, controlPlane, metrics, random)));
        }
        return offsets;
    }

    /** Reads the private poll offset; it has no accessor because nothing in production needs one. */
    private static long pollOffsetOf(LogicalWorker worker) {
        try {
            var field = LogicalWorker.class.getDeclaredField("pollOffsetNanos");
            field.setAccessible(true);
            return field.getLong(worker);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("offset field should exist", e);
        }
    }

    @Test
    void legalLifecyclePathIsAccepted() {
        assertThat(WorkerLifecycleState.CREATED.canTransitionTo(WorkerLifecycleState.REGISTERING)).isTrue();
        assertThat(WorkerLifecycleState.REGISTERING.canTransitionTo(WorkerLifecycleState.ACTIVE)).isTrue();
        assertThat(WorkerLifecycleState.ACTIVE.canTransitionTo(WorkerLifecycleState.DRAINING)).isTrue();
        assertThat(WorkerLifecycleState.DRAINING.canTransitionTo(WorkerLifecycleState.STOPPED)).isTrue();
        // Registration retries loop back to themselves.
        assertThat(WorkerLifecycleState.REGISTERING.canTransitionTo(WorkerLifecycleState.REGISTERING)).isTrue();
    }

    @Test
    void invalidLifecycleTransitionsAreRejected() {
        assertThat(WorkerLifecycleState.CREATED.canTransitionTo(WorkerLifecycleState.ACTIVE)).isFalse();
        assertThat(WorkerLifecycleState.ACTIVE.canTransitionTo(WorkerLifecycleState.REGISTERING)).isFalse();
        assertThat(WorkerLifecycleState.DRAINING.canTransitionTo(WorkerLifecycleState.ACTIVE)).isFalse();
        assertThat(WorkerLifecycleState.STOPPED.canTransitionTo(WorkerLifecycleState.ACTIVE)).isFalse();
        assertThat(WorkerLifecycleState.FAILED.canTransitionTo(WorkerLifecycleState.ACTIVE)).isFalse();
    }

    @Test
    void terminalStatesAllowNoFurtherTransitions() {
        assertThat(WorkerLifecycleState.STOPPED.isTerminal()).isTrue();
        assertThat(WorkerLifecycleState.FAILED.isTerminal()).isTrue();
        assertThat(WorkerLifecycleState.STOPPED.allowedNext()).isEmpty();
        assertThat(WorkerLifecycleState.FAILED.allowedNext()).isEmpty();
    }

    @Test
    void drivingAnIllegalTransitionThrowsRatherThanCorruptingTheRun() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LogicalWorker worker = new LogicalWorker("w-x", "h", config(), controlPlane,
                new LoadGenMetrics(), new Random(1));

        var transitionTo = LogicalWorker.class.getDeclaredMethod("transitionTo", WorkerLifecycleState.class);
        transitionTo.setAccessible(true);

        assertThatThrownBy(() -> transitionTo.invoke(worker, WorkerLifecycleState.ACTIVE))
                .hasCauseInstanceOf(IllegalWorkerTransitionException.class);
    }

    @Test
    void requestAccountingSeparatesClaimsFromEmptyPolls() {
        LoadGenMetrics metrics = new LoadGenMetrics();
        metrics.pollAttempt();
        metrics.emptyPoll(1_000_000);
        metrics.pollAttempt();
        metrics.claimed(2_000_000);
        metrics.httpStatus(204);
        metrics.httpStatus(200);
        metrics.httpStatus(409);

        assertThat(metrics.pollAttemptCount()).isEqualTo(2);
        assertThat(metrics.emptyPollCount()).isEqualTo(1);
        assertThat(metrics.claimCount()).isEqualTo(1);
        assertThat(metrics.httpRequestCount()).isEqualTo(3);
        assertThat(metrics.httpSuccessCount()).isEqualTo(2);
        assertThat(metrics.httpFailureCount()).isEqualTo(1);
        assertThat(metrics.statusCodeDistribution()).containsEntry(204, 1L).containsEntry(409, 1L);
    }

    @Test
    void inFlightCountNeverGoesNegative() {
        LoadGenMetrics metrics = new LoadGenMetrics();
        metrics.executionStarted();
        metrics.executionCompleted(1000);
        metrics.executionCompleted(1000);

        assertThat(metrics.executionsInFlightCount()).isZero();
    }

    @Test
    void jobOfferedByFakeControlPlaneIsFencedAfterReassignment() {
        FakeControlPlane controlPlane = new FakeControlPlane();
        UUID jobId = UUID.randomUUID();
        controlPlane.offerJob(jobId);
        var claimed = controlPlane.claim("w").orElseThrow();

        controlPlane.renewLease(jobId, "w", claimed.executionId());

        controlPlane.reassign(jobId);
        assertThatThrownBy(() -> controlPlane.renewLease(jobId, "w", claimed.executionId()))
                .isInstanceOf(StaleExecutionException.class);
    }
}
