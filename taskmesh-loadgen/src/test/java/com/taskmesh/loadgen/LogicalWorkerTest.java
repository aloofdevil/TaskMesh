package com.taskmesh.loadgen;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/**
 * The logical worker's actual behaviour, driven against a control plane
 * that enforces fencing - not against a mock recording invocations.
 */
class LogicalWorkerTest {

    private static LoadGenConfig fastConfig(int capacity, Duration jobDuration) {
        return new LoadGenConfig(1, capacity,
                Duration.ofMillis(50), Duration.ofMillis(20), Duration.ofMillis(30),
                jobDuration, 0, "http://unused", 42L,
                Duration.ofSeconds(1), Duration.ofSeconds(1), "test");
    }

    private static Thread runWorker(LogicalWorker worker) {
        Thread thread = Thread.ofVirtual().start(worker);
        return thread;
    }

    @Test
    void registersThenBecomesActiveAndHeartbeats() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        LogicalWorker worker = new LogicalWorker("w-1", "test", fastConfig(2, Duration.ofMillis(10)),
                controlPlane, metrics, new Random(1));

        Thread thread = runWorker(worker);
        Thread.sleep(300);
        worker.requestStop();
        thread.join(Duration.ofSeconds(5));

        assertThat(controlPlane.registerCalls.get()).isEqualTo(1);
        assertThat(metrics.workersRegisteredCount()).isEqualTo(1);
        assertThat(controlPlane.heartbeatCalls.get()).isGreaterThan(0);
        assertThat(worker.state()).isEqualTo(WorkerLifecycleState.STOPPED);
        assertThat(controlPlane.deregisterCalls.get()).isEqualTo(1);
    }

    @Test
    void claimsExecutesAndCompletesAJob() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        UUID jobId = UUID.randomUUID();
        controlPlane.offerJob(jobId);

        LogicalWorker worker = new LogicalWorker("w-2", "test", fastConfig(2, Duration.ofMillis(50)),
                controlPlane, metrics, new Random(2));

        Thread thread = runWorker(worker);
        Thread.sleep(500);
        worker.requestStop();
        thread.join(Duration.ofSeconds(5));

        assertThat(metrics.claimCount()).isEqualTo(1);
        assertThat(metrics.executionsStartedCount()).isEqualTo(1);
        assertThat(metrics.executionsCompletedCount()).isEqualTo(1);
        assertThat(controlPlane.acceptedCompletions).containsExactly(jobId);
        assertThat(worker.inFlightCount()).isZero();
    }

    @Test
    void neverPollsBeyondItsCapacity() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        // Four jobs available but capacity is 1, and each takes a long time:
        // the worker must hold exactly one and stop asking for more.
        for (int i = 0; i < 4; i++) {
            controlPlane.offerJob(UUID.randomUUID());
        }

        LogicalWorker worker = new LogicalWorker("w-3", "test", fastConfig(1, Duration.ofSeconds(30)),
                controlPlane, metrics, new Random(3));

        Thread thread = runWorker(worker);
        Thread.sleep(400);
        int inFlightWhileBusy = worker.inFlightCount();
        long claimsWhileBusy = metrics.claimCount();
        worker.requestStop();
        thread.join(Duration.ofSeconds(10));

        assertThat(inFlightWhileBusy).isEqualTo(1);
        assertThat(claimsWhileBusy)
                .as("a worker at capacity must stop claiming")
                .isEqualTo(1);
    }

    /**
     * The invariant that matters most: once fenced, the worker must never
     * report a result. A simulator that completed a reassigned job would
     * manufacture exactly the corruption the benchmark is meant to detect.
     */
    @Test
    void abandonsAFencedExecutionWithoutReportingCompletion() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        UUID jobId = UUID.randomUUID();
        controlPlane.offerJob(jobId);

        // Renewal happens well before the job finishes, so the fence is
        // discovered during renewal rather than at completion.
        LoadGenConfig config = new LoadGenConfig(1, 2,
                Duration.ofMillis(50), Duration.ofMillis(20), Duration.ofMillis(40),
                Duration.ofMillis(600), 0, "http://unused", 7L,
                Duration.ofSeconds(1), Duration.ofSeconds(1), "test");

        LogicalWorker worker = new LogicalWorker("w-4", "test", config, controlPlane, metrics, new Random(4));
        Thread thread = runWorker(worker);

        // Let it claim, then reassign the job out from under it.
        Thread.sleep(150);
        controlPlane.reassign(jobId);

        Thread.sleep(700);
        worker.requestStop();
        thread.join(Duration.ofSeconds(5));

        assertThat(metrics.staleExecutionCount())
                .as("the fence must be observed")
                .isGreaterThan(0);
        assertThat(metrics.executionsAbandonedFencedCount()).isEqualTo(1);
        assertThat(controlPlane.acceptedCompletions)
                .as("a fenced execution must never be accepted as complete")
                .isEmpty();
        assertThat(metrics.executionsCompletedCount()).isZero();
    }

    @Test
    void failedRegistrationEndsInFailedState() throws Exception {
        ControlPlane alwaysFails = new UnreachableControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        LogicalWorker worker = new LogicalWorker("w-5", "test", fastConfig(2, Duration.ofMillis(10)),
                alwaysFails, metrics, new Random(5));

        Thread thread = runWorker(worker);
        thread.join(Duration.ofSeconds(20));

        assertThat(worker.state()).isEqualTo(WorkerLifecycleState.FAILED);
        assertThat(metrics.registrationFailureCount()).isGreaterThan(0);
        assertThat(metrics.workersRegisteredCount()).isZero();
    }

    @Test
    void shutdownDrainsInFlightWorkAndDeregisters() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        UUID jobId = UUID.randomUUID();
        controlPlane.offerJob(jobId);

        LogicalWorker worker = new LogicalWorker("w-6", "test", fastConfig(2, Duration.ofMillis(300)),
                controlPlane, metrics, new Random(6));

        Thread thread = runWorker(worker);
        Thread.sleep(120);
        // Stop while the job is still running: drain must let it finish.
        worker.requestStop();
        thread.join(Duration.ofSeconds(10));

        assertThat(metrics.executionsCompletedCount())
                .as("in-flight work should be drained, not abandoned")
                .isEqualTo(1);
        assertThat(controlPlane.acceptedCompletions).containsExactly(jobId);
        assertThat(controlPlane.deregisterCalls.get()).isEqualTo(1);
        assertThat(worker.state()).isEqualTo(WorkerLifecycleState.STOPPED);
    }

    @Test
    void recordsSchedulingDelaySoGeneratorSaturationIsVisible() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        LogicalWorker worker = new LogicalWorker("w-7", "test", fastConfig(2, Duration.ofMillis(10)),
                controlPlane, metrics, new Random(7));

        Thread thread = runWorker(worker);
        Thread.sleep(250);
        worker.requestStop();
        thread.join(Duration.ofSeconds(5));

        assertThat(metrics.actionsDueCount()).isGreaterThan(0);
        assertThat(metrics.schedulingDelayHistogram().count()).isGreaterThan(0);
    }

    @Test
    void doesNotRenewAfterTheExecutionCompletes() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        UUID jobId = UUID.randomUUID();
        controlPlane.offerJob(jobId);

        LogicalWorker worker = new LogicalWorker("w-8", "test", fastConfig(2, Duration.ofMillis(30)),
                controlPlane, metrics, new Random(8));

        Thread thread = runWorker(worker);
        Thread.sleep(200);
        AtomicReference<Integer> renewsAfterCompletion = new AtomicReference<>(controlPlane.renewCalls.get());
        Thread.sleep(200);
        worker.requestStop();
        thread.join(Duration.ofSeconds(5));

        assertThat(metrics.executionsCompletedCount()).isEqualTo(1);
        assertThat(controlPlane.renewCalls.get())
                .as("nothing is in flight, so no further renewals should be sent")
                .isEqualTo(renewsAfterCompletion.get());
    }

    @Test
    void oneVirtualThreadRunsOneLogicalWorker() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        AtomicReference<Boolean> wasVirtual = new AtomicReference<>();

        LogicalWorker worker = new LogicalWorker("w-9", "test", fastConfig(2, Duration.ofMillis(10)),
                controlPlane, metrics, new Random(9));
        Thread thread = Thread.ofVirtual().start(() -> {
            wasVirtual.set(Thread.currentThread().isVirtual());
            worker.run();
        });
        Thread.sleep(150);
        worker.requestStop();
        thread.join(Duration.ofSeconds(5));

        assertThat(wasVirtual.get()).isTrue();
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    void manyLogicalWorkersRunConcurrentlyInOneProcess() throws Exception {
        FakeControlPlane controlPlane = new FakeControlPlane();
        LoadGenMetrics metrics = new LoadGenMetrics();
        int count = 2_000;
        var workers = new java.util.ArrayList<LogicalWorker>(count);
        var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        var latch = new java.util.concurrent.CountDownLatch(count);

        Random seeded = new Random(99);
        for (int i = 0; i < count; i++) {
            LogicalWorker worker = new LogicalWorker("bulk-" + i, "test",
                    fastConfig(2, Duration.ofMillis(10)), controlPlane, metrics, seeded);
            workers.add(worker);
            executor.submit(() -> {
                try {
                    worker.run();
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread.sleep(400);
        workers.forEach(LogicalWorker::requestStop);
        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(metrics.workersRegisteredCount()).isEqualTo(count);
        assertThat(workers).allMatch(w -> w.state() == WorkerLifecycleState.STOPPED);
        assertThat(controlPlane.deregisterCalls.get()).isEqualTo(count);
    }
}
