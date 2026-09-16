package com.taskmesh.controlplane.service;

import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.taskmesh.controlplane.domain.WorkerStatus;
import com.taskmesh.controlplane.repository.JobEventRepository;
import com.taskmesh.controlplane.repository.WorkerRepository;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * The application's own metrics, kept in one place so the meter names stay
 * consistent and the services that record them do not each grow their own
 * registry wiring.
 * <p>
 * Every meter here is deliberately low cardinality. Job ids, execution ids
 * and payloads are never used as tags: they are unbounded, and a tag with
 * unbounded values multiplies into a new time series per value, which is
 * how a metrics backend gets taken down by its own instrumentation. The
 * only tag used is {@code outcome} on job failures, which has two values.
 */
@Component
public class TaskMeshMetrics {

    private final Counter jobsSubmitted;
    private final Counter jobsClaimed;
    private final Counter jobsCompleted;
    private final Counter jobsCancelled;
    private final Counter jobsRetried;
    private final Counter jobsDeadLettered;
    private final Counter leaseExpirations;
    private final Counter staleExecutionsRejected;
    private final Counter outboxPublished;
    private final Counter outboxPublishFailures;
    private final Timer claimLatency;

    public TaskMeshMetrics(MeterRegistry registry, WorkerRepository workerRepository,
            JobEventRepository jobEventRepository) {

        this.jobsSubmitted = Counter.builder("taskmesh.jobs.submitted")
                .description("Jobs accepted from clients").register(registry);
        this.jobsClaimed = Counter.builder("taskmesh.jobs.claimed")
                .description("Job executions handed to a worker").register(registry);
        this.jobsCompleted = Counter.builder("taskmesh.jobs.completed")
                .description("Jobs that finished successfully").register(registry);
        this.jobsCancelled = Counter.builder("taskmesh.jobs.cancelled")
                .description("Jobs cancelled while queued").register(registry);
        this.jobsRetried = Counter.builder("taskmesh.jobs.failed")
                .description("Failed executions, by what the retry policy decided")
                .tag("outcome", "retry").register(registry);
        this.jobsDeadLettered = Counter.builder("taskmesh.jobs.failed")
                .description("Failed executions, by what the retry policy decided")
                .tag("outcome", "dead_letter").register(registry);
        this.leaseExpirations = Counter.builder("taskmesh.leases.expired")
                .description("Executions reclaimed because their lease lapsed").register(registry);
        this.staleExecutionsRejected = Counter.builder("taskmesh.executions.stale_rejected")
                .description("Writes refused because the execution no longer owned the job").register(registry);
        this.outboxPublished = Counter.builder("taskmesh.outbox.published")
                .description("Outbox events acknowledged by Kafka").register(registry);
        this.outboxPublishFailures = Counter.builder("taskmesh.outbox.publish_failures")
                .description("Outbox events Kafka would not accept").register(registry);

        this.claimLatency = Timer.builder("taskmesh.jobs.claim")
                .description("Time to serve a worker's claim request, whether or not work was available")
                .register(registry);

        // Gauges read current state on scrape rather than being pushed to,
        // because they describe a level rather than a rate.
        Gauge.builder("taskmesh.workers.active", () -> workerRepository.countByStatus(WorkerStatus.ACTIVE))
                .description("Workers currently registered as ACTIVE")
                .register(registry);
        Gauge.builder("taskmesh.outbox.pending", jobEventRepository::countByPublishedAtIsNull)
                .description("Events written but not yet published to Kafka; grows during a Kafka outage")
                .register(registry);
    }

    public void jobSubmitted() {
        jobsSubmitted.increment();
    }

    public void jobClaimed() {
        jobsClaimed.increment();
    }

    public void jobCompleted() {
        jobsCompleted.increment();
    }

    public void jobCancelled() {
        jobsCancelled.increment();
    }

    public void jobRetryScheduled() {
        jobsRetried.increment();
    }

    public void jobDeadLettered() {
        jobsDeadLettered.increment();
    }

    public void leaseExpired() {
        leaseExpirations.increment();
    }

    public void staleExecutionRejected() {
        staleExecutionsRejected.increment();
    }

    public void outboxPublished(int count) {
        outboxPublished.increment(count);
    }

    public void outboxPublishFailed() {
        outboxPublishFailures.increment();
    }

    public void recordClaimLatency(long nanos) {
        claimLatency.record(nanos, TimeUnit.NANOSECONDS);
    }
}
