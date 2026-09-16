package com.taskmesh.loadgen;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point: builds the logical worker fleet, ramps it up, runs it, then
 * shuts it down cleanly.
 * <p>
 * The whole fleet lives in <em>one</em> OS process and one JVM. N logical
 * workers cost N virtual threads, which are multiplexed onto a carrier pool
 * roughly the size of the CPU count - not N processes, containers or
 * platform threads. That is the entire reason this module exists.
 */
public final class LoadGenerator {

    private final LoadGenConfig config;
    private final LoadGenMetrics metrics = new LoadGenMetrics();
    private final ResourceSampler resources = new ResourceSampler();
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();

    private LoadGenerator(LoadGenConfig config) {
        this.config = config;
    }

    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                System.out.println(LoadGenConfig.usage());
                return;
            }
        }
        LoadGenConfig config;
        try {
            config = LoadGenConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();
            System.err.println(LoadGenConfig.usage());
            System.exit(2);
            return;
        }
        new LoadGenerator(config).run();
    }

    private void run() throws Exception {
        System.out.println(config.describeEffective());
        System.out.println();

        metrics.workersConfigured(config.workers());

        // One client for the whole generator. HTTP/1.1 explicitly: the
        // control plane is Tomcat, and leaving the client to negotiate would
        // add an upgrade attempt to the first request of every connection.
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        ControlPlane controlPlane = new HttpControlPlane(
                httpClient, config.controlPlaneUrl(), config.requestTimeout(), metrics);

        Random seeded = new Random(config.seed());
        List<LogicalWorker> workers = new ArrayList<>(config.workers());
        for (int i = 0; i < config.workers(); i++) {
            String id = "%s-%d-%06d".formatted(config.workerIdPrefix(), config.seed() & 0xFFFF, i);
            workers.add(new LogicalWorker(id, "loadgen", config, controlPlane, metrics, seeded));
        }

        CountDownLatch finished = new CountDownLatch(workers.size());
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> beginShutdown(workers)));

        SchedulerProbe probe = new SchedulerProbe();
        probe.start();

        long startNanos = System.nanoTime();
        rampUp(workers, executor, finished);
        System.out.printf("ramp-up complete: %d/%d logical workers registered (%d failures)%n",
                metrics.workersRegisteredCount(), config.workers(), metrics.registrationFailureCount());

        System.out.printf("running for %ds...%n", config.runDuration().toSeconds());
        Thread.sleep(config.runDuration());

        beginShutdown(workers);
        System.out.println("draining and deregistering...");
        if (!finished.await(60, TimeUnit.SECONDS)) {
            System.out.println("warning: some logical workers did not finish draining within 60s");
        }
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
        probe.close();
        System.out.println(new RunReport(config, metrics, resources, probe).render(elapsed));
        summariseLifecycleStates(workers);
    }

    /**
     * Starts logical workers at the configured rate. Ramping matters: 25,000
     * simultaneous registrations would measure a registration stampede rather
     * than steady-state behaviour, and the audit explicitly called for the
     * burst to be opt-in rather than accidental.
     */
    private void rampUp(List<LogicalWorker> workers, ExecutorService executor, CountDownLatch finished)
            throws InterruptedException {
        int perSecond = config.rampUpPerSecond();
        long batchStart = System.nanoTime();
        int startedThisSecond = 0;

        for (LogicalWorker worker : workers) {
            executor.submit(() -> {
                try {
                    worker.run();
                } finally {
                    finished.countDown();
                }
            });

            if (perSecond <= 0) {
                continue;
            }
            if (++startedThisSecond >= perSecond) {
                long elapsed = System.nanoTime() - batchStart;
                long remaining = Duration.ofSeconds(1).toNanos() - elapsed;
                if (remaining > 0) {
                    Thread.sleep(Duration.ofNanos(remaining));
                }
                batchStart = System.nanoTime();
                startedThisSecond = 0;
            }
        }
    }

    /** Idempotent: the shutdown hook and the normal path both call it. */
    private void beginShutdown(List<LogicalWorker> workers) {
        if (shutdownStarted.compareAndSet(false, true)) {
            workers.forEach(LogicalWorker::requestStop);
        }
    }

    private void summariseLifecycleStates(List<LogicalWorker> workers) {
        var byState = new java.util.EnumMap<WorkerLifecycleState, Integer>(WorkerLifecycleState.class);
        for (LogicalWorker worker : workers) {
            byState.merge(worker.state(), 1, Integer::sum);
        }
        System.out.println("final logical worker states: " + byState);
        int stillHolding = workers.stream().mapToInt(LogicalWorker::inFlightCount).sum();
        System.out.println("executions still held by generator at exit: " + stillHolding);
    }
}
