package com.taskmesh.loadgen;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Run configuration, parsed from {@code --key=value} arguments.
 * <p>
 * The timing defaults are deliberately the <em>production</em> values from
 * {@code taskmesh-worker/src/main/resources/application.yml}, so a run with
 * no flags reproduces real worker behaviour rather than something tuned to
 * flatter the benchmark. Overriding them is allowed but never silent:
 * {@link #describeEffective()} prints what was actually used and is
 * recorded with every run.
 *
 * @param workers            logical workers to simulate (NOT threads, processes or containers)
 * @param capacity           in-flight job slots per logical worker
 * @param heartbeatInterval  production default 5s
 * @param pollInterval       production default 1s
 * @param leaseRenewInterval production default 10s
 * @param jobDuration        simulated execution time; production stand-in is 2s
 * @param rampUpPerSecond    logical workers started per second (0 = all at once)
 * @param controlPlaneUrl    base URL of the control plane
 * @param seed               RNG seed, so jitter and therefore the run are reproducible
 * @param runDuration        how long to run after ramp-up completes
 * @param requestTimeout     per-HTTP-request timeout
 * @param workerIdPrefix     prefix for generated worker ids
 */
public record LoadGenConfig(
        int workers,
        int capacity,
        Duration heartbeatInterval,
        Duration pollInterval,
        Duration leaseRenewInterval,
        Duration jobDuration,
        int rampUpPerSecond,
        String controlPlaneUrl,
        long seed,
        Duration runDuration,
        Duration requestTimeout,
        String workerIdPrefix) {

    public static final int DEFAULT_WORKERS = 100;
    public static final int DEFAULT_CAPACITY = 2;
    public static final Duration DEFAULT_HEARTBEAT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_POLL = Duration.ofSeconds(1);
    public static final Duration DEFAULT_LEASE_RENEW = Duration.ofSeconds(10);
    public static final Duration DEFAULT_JOB_DURATION = Duration.ofSeconds(2);
    public static final int DEFAULT_RAMP_UP_PER_SECOND = 100;
    public static final String DEFAULT_URL = "http://localhost:8080";
    public static final Duration DEFAULT_RUN_DURATION = Duration.ofSeconds(60);
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);

    public LoadGenConfig {
        if (workers <= 0) {
            throw new IllegalArgumentException("--workers must be positive, got " + workers);
        }
        if (capacity < 1 || capacity > 64) {
            // Mirrors the control plane's own validation on WorkerRegistrationRequest.
            throw new IllegalArgumentException("--capacity must be between 1 and 64, got " + capacity);
        }
        if (rampUpPerSecond < 0) {
            throw new IllegalArgumentException("--ramp-up must be >= 0, got " + rampUpPerSecond);
        }
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
            throw new IllegalArgumentException("--heartbeat-interval must be positive");
        }
        if (pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("--poll-interval must be positive");
        }
        if (leaseRenewInterval.isNegative() || leaseRenewInterval.isZero()) {
            throw new IllegalArgumentException("--lease-renew-interval must be positive");
        }
        if (jobDuration.isNegative()) {
            throw new IllegalArgumentException("--job-duration must be >= 0");
        }
    }

    public static LoadGenConfig defaults() {
        return parse(new String[0]);
    }

    public static LoadGenConfig parse(String[] args) {
        Map<String, String> values = new LinkedHashMap<>();
        List<String> unknown = new ArrayList<>();

        for (String arg : args) {
            if (arg == null || arg.isBlank()) {
                continue;
            }
            if (!arg.startsWith("--")) {
                unknown.add(arg);
                continue;
            }
            String stripped = arg.substring(2);
            int eq = stripped.indexOf('=');
            if (eq < 0) {
                // Flag form without a value is ambiguous here; every option takes one.
                throw new IllegalArgumentException("Option --" + stripped + " requires a value (use --key=value)");
            }
            values.put(stripped.substring(0, eq).trim(), stripped.substring(eq + 1).trim());
        }

        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Unrecognised argument(s): " + unknown + ". Options use --key=value");
        }

        LoadGenConfig config = new LoadGenConfig(
                intValue(values, "workers", DEFAULT_WORKERS),
                intValue(values, "capacity", DEFAULT_CAPACITY),
                durationValue(values, "heartbeat-interval", DEFAULT_HEARTBEAT),
                durationValue(values, "poll-interval", DEFAULT_POLL),
                durationValue(values, "lease-renew-interval", DEFAULT_LEASE_RENEW),
                durationValue(values, "job-duration", DEFAULT_JOB_DURATION),
                intValue(values, "ramp-up", DEFAULT_RAMP_UP_PER_SECOND),
                stringValue(values, "control-plane-url", DEFAULT_URL),
                longValue(values, "seed", System.nanoTime()),
                durationValue(values, "run-duration", DEFAULT_RUN_DURATION),
                durationValue(values, "request-timeout", DEFAULT_REQUEST_TIMEOUT),
                stringValue(values, "worker-id-prefix", "lw"));

        Map<String, String> leftovers = new LinkedHashMap<>(values);
        KNOWN_KEYS.forEach(leftovers::remove);
        if (!leftovers.isEmpty()) {
            throw new IllegalArgumentException("Unknown option(s): " + leftovers.keySet() + ". Known: " + KNOWN_KEYS);
        }
        return config;
    }

    private static final List<String> KNOWN_KEYS = List.of(
            "workers", "capacity", "heartbeat-interval", "poll-interval", "lease-renew-interval",
            "job-duration", "ramp-up", "control-plane-url", "seed", "run-duration",
            "request-timeout", "worker-id-prefix");

    private static String stringValue(Map<String, String> values, String key, String fallback) {
        String raw = values.get(key);
        return raw == null || raw.isBlank() ? fallback : raw;
    }

    private static int intValue(Map<String, String> values, String key, int fallback) {
        String raw = values.get(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.replace("_", ""));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " must be an integer, got '" + raw + "'");
        }
    }

    private static long longValue(Map<String, String> values, String key, long fallback) {
        String raw = values.get(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.replace("_", ""));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " must be an integer, got '" + raw + "'");
        }
    }

    /** Accepts bare milliseconds ("1500") or a suffix form ("1500ms", "5s", "2m"). */
    static Duration durationValue(Map<String, String> values, String key, Duration fallback) {
        String raw = values.get(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return parseDuration(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " must be a duration (e.g. 500ms, 5s, 2m), got '" + raw + "'");
        }
    }

    static Duration parseDuration(String raw) {
        String text = raw.trim().toLowerCase();
        if (text.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2).trim()));
        }
        if (text.endsWith("s")) {
            return Duration.ofMillis(Math.round(Double.parseDouble(text.substring(0, text.length() - 1).trim()) * 1000));
        }
        if (text.endsWith("m")) {
            return Duration.ofSeconds(Math.round(Double.parseDouble(text.substring(0, text.length() - 1).trim()) * 60));
        }
        return Duration.ofMillis(Long.parseLong(text));
    }

    /** Total in-flight slots: a CEILING on concurrent jobs, never a measurement of them. */
    public int jobSlotCeiling() {
        return workers * capacity;
    }

    public String describeEffective() {
        return """
                Effective configuration
                  logical workers      : %d
                  capacity per worker  : %d   (job slot ceiling: %d - NOT actual concurrent jobs)
                  heartbeat interval   : %s
                  poll interval        : %s
                  lease renew interval : %s
                  simulated job duration: %s
                  ramp-up              : %s
                  control plane        : %s
                  seed                 : %d
                  run duration         : %s
                  request timeout      : %s"""
                .formatted(workers, capacity, jobSlotCeiling(),
                        format(heartbeatInterval), format(pollInterval), format(leaseRenewInterval),
                        format(jobDuration),
                        rampUpPerSecond == 0 ? "all at once" : rampUpPerSecond + " workers/sec",
                        controlPlaneUrl, seed, format(runDuration), format(requestTimeout));
    }

    private static String format(Duration d) {
        return d.toMillis() + "ms";
    }

    public static String usage() {
        return """
                TaskMesh logical worker load generator

                Usage: java -jar taskmesh-loadgen.jar [--key=value ...]

                  --workers=N               logical workers to simulate (default %d)
                  --capacity=N              job slots per logical worker (default %d)
                  --heartbeat-interval=D    default %s (production value)
                  --poll-interval=D         default %s (production value)
                  --lease-renew-interval=D  default %s (production value)
                  --job-duration=D          simulated execution time (default %s)
                  --ramp-up=N               logical workers started per second, 0 = all at once (default %d)
                  --control-plane-url=URL   default %s
                  --seed=N                  RNG seed for reproducible jitter (default: nanoTime)
                  --run-duration=D          how long to run after ramp-up (default %s)
                  --request-timeout=D       per-request HTTP timeout (default %s)
                  --worker-id-prefix=S      worker id prefix (default lw)

                Durations accept 500ms, 5s, 2m or bare milliseconds.
                """.formatted(DEFAULT_WORKERS, DEFAULT_CAPACITY,
                        format(DEFAULT_HEARTBEAT), format(DEFAULT_POLL), format(DEFAULT_LEASE_RENEW),
                        format(DEFAULT_JOB_DURATION), DEFAULT_RAMP_UP_PER_SECOND, DEFAULT_URL,
                        format(DEFAULT_RUN_DURATION), format(DEFAULT_REQUEST_TIMEOUT));
    }
}
