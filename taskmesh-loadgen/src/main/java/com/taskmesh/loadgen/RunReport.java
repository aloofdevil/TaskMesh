package com.taskmesh.loadgen;

import java.time.Duration;
import java.util.Map;

/**
 * The end-of-run summary.
 * <p>
 * Two rules shape this output. First, the categories the Day 8 audit
 * insisted on staying separate - logical workers, virtual threads, carrier
 * threads, OS processes, containers, job slots, actual concurrent jobs,
 * requests offered and requests achieved - are printed as distinct lines
 * and never collapsed into a single "worker count".
 * <p>
 * Second, it states plainly whether the <em>generator</em> ran out of
 * capacity. A latency number produced by a saturated generator says nothing
 * about the control plane, so the verdict is printed before the numbers it
 * qualifies rather than buried beneath them.
 */
public final class RunReport {

    /**
     * Scheduling delay above which the generator is considered to have
     * failed to keep up. A quarter of the poll interval: at that point
     * actions are landing visibly late and the offered rate no longer
     * matches the configured one.
     */
    private static final double SATURATION_P99_FRACTION_OF_POLL = 0.25;

    /** Probe lateness above which the generator's own scheduling is judged starved. */
    private static final double PROBE_SATURATION_P99_MILLIS = 100.0;

    private final LoadGenConfig config;
    private final LoadGenMetrics metrics;
    private final ResourceSampler resources;
    private final SchedulerProbe probe;

    public RunReport(LoadGenConfig config, LoadGenMetrics metrics, ResourceSampler resources,
            SchedulerProbe probe) {
        this.config = config;
        this.metrics = metrics;
        this.resources = resources;
        this.probe = probe;
    }

    /**
     * True only when the GENERATOR itself could not keep up.
     * <p>
     * Judged by the independent {@link SchedulerProbe}, which issues no
     * requests: if a thread that only sleeps cannot wake on time, carriers
     * are starved. Worker scheduling delay alone is not sufficient evidence,
     * because a slow control plane blocks each worker loop and delays its
     * next deadline through exactly the same mechanism.
     */
    public boolean generatorSaturated() {
        return probe.wakeupLateness().count() > 0
                && probe.wakeupLateness().p99Millis() > PROBE_SATURATION_P99_MILLIS;
    }

    /** Workers ran late while the generator had headroom - i.e. downstream latency. */
    public boolean workersLate() {
        double thresholdMillis = config.pollInterval().toMillis() * SATURATION_P99_FRACTION_OF_POLL;
        return metrics.schedulingDelayHistogram().count() > 0
                && metrics.schedulingDelayHistogram().p99Millis() > thresholdMillis;
    }

    /**
     * Requests the configuration implies per second. Calculated, not
     * measured - it is the yardstick the achieved rate is compared against.
     * <p>
     * Derived from PEAK active workers, because by report time every worker
     * has stopped and the live count is zero.
     */
    public double offeredRequestsPerSecond() {
        int active = Math.max(metrics.peakWorkersActiveCount(), 0);
        double heartbeats = active / (config.heartbeatInterval().toMillis() / 1000.0);
        double polls = active / (config.pollInterval().toMillis() / 1000.0);
        return heartbeats + polls;
    }

    public double achievedRequestsPerSecond(Duration elapsed) {
        double seconds = elapsed.toMillis() / 1000.0;
        return seconds <= 0 ? 0 : metrics.httpRequestCount() / seconds;
    }

    public String render(Duration elapsed) {
        StringBuilder out = new StringBuilder();
        out.append("\n============================================================\n");
        out.append("TaskMesh load generator - run report\n");
        out.append("============================================================\n\n");

        out.append(config.describeEffective()).append("\n\n");

        out.append("GENERATOR VERDICT\n");
        if (generatorSaturated()) {
            out.append("  GENERATOR-SATURATED - probe wake-up p99 = %.1fms exceeds %.1fms.\n"
                    .formatted(probe.wakeupLateness().p99Millis(), PROBE_SATURATION_P99_MILLIS));
            out.append("  The generator could not schedule its own work on time.\n");
            out.append("  Latency figures below are CONTAMINATED and must not be blamed on the control plane.\n");
        } else if (workersLate()) {
            out.append("  Generator had headroom (probe wake-up p99 = %.1fms) but worker schedules slipped\n"
                    .formatted(probe.wakeupLateness().p99Millis()));
            out.append("  (scheduling delay p99 = %.1fms). A worker loop blocks while its HTTP call is in\n"
                    .formatted(metrics.schedulingDelayHistogram().p99Millis()));
            out.append("  flight, so this is DOWNSTREAM latency pushing workers late, not generator saturation.\n");
        } else {
            out.append("  Generator kept up (probe wake-up p99 = %.1fms, scheduling delay p99 = %.1fms).\n"
                    .formatted(probe.wakeupLateness().p99Millis(),
                            metrics.schedulingDelayHistogram().p99Millis()));
            out.append("  Control-plane latencies below are attributable to the control plane.\n");
        }
        out.append("  probe wake-up late   : ").append(probe.wakeupLateness().summary())
                .append("   (sleeps only; issues no requests)\n");
        out.append("  worker sched delay   : ").append(metrics.schedulingDelayHistogram().summary())
                .append("   (capped at ~1s by design)\n");
        out.append('\n');

        out.append("COUNTING CATEGORIES (deliberately not collapsed)\n");
        out.append("  logical workers      : %d configured, %d started, %d registered, %d active at end\n"
                .formatted(metrics.workersConfiguredCount(), metrics.workersStartedCount(),
                        metrics.workersRegisteredCount(), metrics.workersActiveCount()));
        out.append("  virtual threads      : %d (one per logical worker)\n".formatted(metrics.workersStartedCount()));
        out.append("  carrier/platform     : ~%d carrier parallelism, %d platform threads in JVM\n"
                .formatted(resources.carrierParallelism(), resources.platformThreadCount()));
        out.append("  OS processes         : 1 (this generator)\n");
        out.append("  containers/pods      : 0 for logical workers (generator runs outside the stack)\n");
        out.append("  job slot ceiling     : %d  (capacity %d x %d workers - a CEILING, not a measurement)\n"
                .formatted(config.jobSlotCeiling(), config.capacity(), config.workers()));
        out.append(("  actual concurrent jobs: %d in flight at report time (generator view; "
                + "authoritative count is in PostgreSQL)%n").formatted(metrics.executionsInFlightCount()));
        out.append("  requests offered     : %.0f/sec (calculated from config and peak active workers)\n"
                .formatted(offeredRequestsPerSecond()));
        out.append("  requests achieved    : %.0f/sec (measured over %.1fs)\n"
                .formatted(achievedRequestsPerSecond(elapsed), elapsed.toMillis() / 1000.0));
        out.append('\n');

        out.append("WORKERS\n");
        out.append("  configured=%d started=%d registered=%d registrationFailures=%d active=%d failed=%d stopped=%d deregistered=%d\n"
                .formatted(metrics.workersConfiguredCount(), metrics.workersStartedCount(),
                        metrics.workersRegisteredCount(), metrics.registrationFailureCount(),
                        metrics.workersActiveCount(), metrics.workersFailedCount(),
                        metrics.workersStoppedCount(), metrics.workersDeregisteredCount()));
        out.append("  registration latency : ").append(metrics.registrationLatencyHistogram().summary()).append('\n');
        out.append('\n');

        out.append("HEARTBEATS\n");
        out.append("  attempts=%d successes=%d failures=%d\n".formatted(
                metrics.heartbeatAttemptCount(), metrics.heartbeatSuccessCount(), metrics.heartbeatFailureCount()));
        out.append("  latency              : ").append(metrics.heartbeatLatencyHistogram().summary()).append('\n');
        out.append('\n');

        out.append("POLLING\n");
        out.append("  attempts=%d claims=%d emptyPolls=%d failures=%d\n".formatted(
                metrics.pollAttemptCount(), metrics.claimCount(),
                metrics.emptyPollCount(), metrics.pollFailureCount()));
        out.append("  latency              : ").append(metrics.pollLatencyHistogram().summary()).append('\n');
        out.append('\n');

        out.append("EXECUTION\n");
        out.append("  started=%d completed=%d failed=%d abandonedFenced=%d inFlight=%d\n".formatted(
                metrics.executionsStartedCount(), metrics.executionsCompletedCount(),
                metrics.executionsFailedCount(), metrics.executionsAbandonedFencedCount(),
                metrics.executionsInFlightCount()));
        out.append("  completion latency   : ").append(metrics.completionLatencyHistogram().summary()).append('\n');
        out.append('\n');

        out.append("LEASES\n");
        out.append("  renewAttempts=%d successes=%d failures=%d staleExecutionResponses=%d\n".formatted(
                metrics.renewAttemptCount(), metrics.renewSuccessCount(),
                metrics.renewFailureCount(), metrics.staleExecutionCount()));
        out.append("  latency              : ").append(metrics.renewLatencyHistogram().summary()).append('\n');
        out.append('\n');

        out.append("HTTP\n");
        out.append("  total=%d successes=%d failures=%d\n".formatted(
                metrics.httpRequestCount(), metrics.httpSuccessCount(), metrics.httpFailureCount()));
        out.append("  status codes         : ").append(formatMap(metrics.statusCodeDistribution())).append('\n');
        Map<String, Long> transport = metrics.transportErrorDistribution();
        out.append("  transport errors     : ")
                .append(transport.isEmpty() ? "none" : transport.toString()).append('\n');
        out.append('\n');

        out.append("GENERATOR RESOURCES\n");
        out.append("  process CPU load     : ").append(ResourceSampler.formatCpu(resources.processCpuLoad()))
                .append(" (of %d available processors)\n".formatted(resources.availableProcessors()));
        out.append("  JVM heap used        : ").append(ResourceSampler.formatBytes(resources.heapUsedBytes()))
                .append('\n');
        out.append("  JVM non-heap used    : ").append(ResourceSampler.formatBytes(resources.nonHeapUsedBytes()))
                .append('\n');
        out.append("  process RSS          : NOT MEASURED (not observable from inside the JVM)\n");
        out.append("============================================================\n");
        return out.toString();
    }

    private static String formatMap(Map<Integer, Long> map) {
        return map.isEmpty() ? "none" : map.toString();
    }
}
