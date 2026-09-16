package com.taskmesh.loadgen;

import java.time.Duration;

/**
 * An independent measure of whether the <em>generator</em> can still
 * schedule work on time.
 * <p>
 * This exists because worker scheduling delay alone cannot tell you whose
 * fault lateness is. A logical worker's loop blocks while an HTTP call is
 * in flight, so when the control plane slows down, the loop's next deadline
 * slips too - and the delay looks identical to the generator running out of
 * capacity. Attributing that to the generator would wrongly exonerate the
 * control plane; attributing it to the control plane when the generator
 * really is starved would do the opposite. Either way the benchmark would
 * be worthless.
 * <p>
 * The probe is one virtual thread that does nothing but sleep for a fixed
 * interval and measure how late it wakes. It issues no requests, so its
 * lateness cannot be caused by the control plane. If the probe is on time
 * while workers are late, the generator has spare capacity and the lateness
 * is downstream latency. If the probe itself is late, the carrier pool is
 * genuinely starved and every latency figure in the run is suspect.
 */
public final class SchedulerProbe implements AutoCloseable {

    private static final Duration INTERVAL = Duration.ofMillis(100);

    private final LatencyHistogram wakeupLateness = new LatencyHistogram();
    private volatile boolean running = true;
    private Thread thread;

    public void start() {
        thread = Thread.ofVirtual().name("loadgen-scheduler-probe").start(this::loop);
    }

    private void loop() {
        long next = System.nanoTime() + INTERVAL.toNanos();
        while (running) {
            long now = System.nanoTime();
            long sleep = next - now;
            if (sleep > 0) {
                try {
                    Thread.sleep(Duration.ofNanos(sleep));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            wakeupLateness.recordNanos(System.nanoTime() - next);
            next += INTERVAL.toNanos();
            // If we fell far behind, resynchronise rather than spin through
            // a backlog of missed ticks.
            long behind = System.nanoTime() - next;
            if (behind > 0) {
                next += ((behind / INTERVAL.toNanos()) + 1) * INTERVAL.toNanos();
            }
        }
    }

    public LatencyHistogram wakeupLateness() {
        return wakeupLateness;
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }
}
