package com.taskmesh.loadgen;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A lock-free latency recorder with logarithmic buckets.
 * <p>
 * Percentiles are the point of this benchmark, and a mean would hide
 * exactly the tail behaviour we care about. Storing every sample would cost
 * gigabytes at 25,000 logical workers, so samples land in exponentially
 * spaced buckets instead: bucket {@code i} covers values around
 * {@code 1.02^i} microseconds, giving roughly 2% relative resolution across
 * the whole range from a microsecond to a minute in 1024 buckets.
 * <p>
 * Every recording path is a single {@code incrementAndGet}, so thousands of
 * virtual threads can record concurrently without contending on a lock -
 * which matters, because the instrument must not become the bottleneck it
 * is trying to measure.
 */
public final class LatencyHistogram {

    private static final int BUCKETS = 1024;
    private static final double GROWTH = 1.02;
    private static final double LOG_GROWTH = Math.log(GROWTH);

    private final AtomicLongArray counts = new AtomicLongArray(BUCKETS);
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong sumMicros = new AtomicLong();
    private final AtomicLong maxMicros = new AtomicLong();

    public void recordNanos(long nanos) {
        recordMicros(nanos / 1_000);
    }

    public void recordMicros(long micros) {
        long clamped = Math.max(0, micros);
        counts.incrementAndGet(bucketFor(clamped));
        total.incrementAndGet();
        sumMicros.addAndGet(clamped);
        maxMicros.accumulateAndGet(clamped, Math::max);
    }

    static int bucketFor(long micros) {
        if (micros <= 0) {
            return 0;
        }
        int index = (int) (Math.log(micros) / LOG_GROWTH);
        return Math.min(BUCKETS - 1, Math.max(0, index));
    }

    static long valueForBucket(int bucket) {
        return bucket == 0 ? 0 : (long) Math.pow(GROWTH, bucket);
    }

    public long count() {
        return total.get();
    }

    public long maxMicros() {
        return maxMicros.get();
    }

    public double meanMicros() {
        long n = total.get();
        return n == 0 ? 0 : (double) sumMicros.get() / n;
    }

    /** @param percentile e.g. 0.99 for p99. Returns microseconds. */
    public long percentileMicros(double percentile) {
        long n = total.get();
        if (n == 0) {
            return 0;
        }
        long target = (long) Math.ceil(percentile * n);
        long cumulative = 0;
        for (int i = 0; i < BUCKETS; i++) {
            cumulative += counts.get(i);
            if (cumulative >= target) {
                return valueForBucket(i);
            }
        }
        return maxMicros.get();
    }

    public double p50Millis() {
        return percentileMicros(0.50) / 1000.0;
    }

    public double p95Millis() {
        return percentileMicros(0.95) / 1000.0;
    }

    public double p99Millis() {
        return percentileMicros(0.99) / 1000.0;
    }

    public double maxMillis() {
        return maxMicros() / 1000.0;
    }

    public String summary() {
        if (count() == 0) {
            return "no samples";
        }
        return "n=%d p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms"
                .formatted(count(), p50Millis(), p95Millis(), p99Millis(), maxMillis());
    }
}
