package com.taskmesh.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class LatencyHistogramTest {

    @Test
    void percentilesTrackAKnownDistributionWithinBucketResolution() {
        LatencyHistogram histogram = new LatencyHistogram();
        // 1..1000 ms, uniform.
        for (int millis = 1; millis <= 1000; millis++) {
            histogram.recordMicros(millis * 1000L);
        }

        assertThat(histogram.count()).isEqualTo(1000);
        // Buckets grow at 2%, so allow a few percent of slack rather than
        // pretending the estimate is exact.
        assertThat(histogram.p50Millis()).isBetween(480.0, 520.0);
        assertThat(histogram.p95Millis()).isBetween(920.0, 980.0);
        assertThat(histogram.p99Millis()).isBetween(960.0, 1000.0);
        assertThat(histogram.maxMillis()).isEqualTo(1000.0);
    }

    @Test
    void tailIsNotHiddenByTheMean() {
        LatencyHistogram histogram = new LatencyHistogram();
        for (int i = 0; i < 999; i++) {
            histogram.recordMicros(1_000);
        }
        histogram.recordMicros(30_000_000);

        assertThat(histogram.p50Millis()).isLessThan(2.0);
        assertThat(histogram.maxMillis()).isEqualTo(30_000.0);
        // With 1000 samples, p99.9 still selects the 999th ordered sample -
        // the single outlier is the 1000th. A percentile above 99.9% is what
        // reaches it, which is exactly why max is reported alongside p99.
        assertThat(histogram.percentileMicros(0.999)).isLessThan(10_000L);
        assertThat(histogram.percentileMicros(0.9995)).isGreaterThan(1_000_000L);
    }

    @Test
    void emptyHistogramReportsZeroesRatherThanFailing() {
        LatencyHistogram histogram = new LatencyHistogram();

        assertThat(histogram.count()).isZero();
        assertThat(histogram.p99Millis()).isZero();
        assertThat(histogram.summary()).isEqualTo("no samples");
    }

    @Test
    void recordsFromManyThreadsWithoutLosingSamples() throws Exception {
        LatencyHistogram histogram = new LatencyHistogram();
        int threads = 64;
        int perThread = 1000;
        CountDownLatch done = new CountDownLatch(threads);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < perThread; i++) {
                            histogram.recordMicros(5_000);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(histogram.count()).isEqualTo((long) threads * perThread);
    }

    @Test
    void bucketsAreMonotonic() {
        long previous = -1;
        for (long micros : new long[] {0, 1, 10, 100, 1_000, 10_000, 1_000_000, 60_000_000}) {
            int bucket = LatencyHistogram.bucketFor(micros);
            assertThat((long) bucket).isGreaterThanOrEqualTo(previous);
            previous = bucket;
        }
    }
}
