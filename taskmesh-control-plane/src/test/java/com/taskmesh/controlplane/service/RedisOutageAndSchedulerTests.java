package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.JobStatus;
import com.taskmesh.controlplane.repository.JobRepository;

/**
 * What a Redis outage actually does to the control plane, and whether one
 * blocked scheduled task can starve the others.
 * <p>
 * Both of these were real defects found during the Day 6 Kubernetes
 * verification, not hypotheticals:
 * <ul>
 * <li>Lettuce's default command timeout is 60 seconds, so although
 * {@link WorkerLivenessCache} caught every Redis failure, each call first
 * blocked for a minute. "Handled" is not the same as "fast".</li>
 * <li>Spring's scheduler pool defaults to one thread, so that minute-long
 * block also stopped every other scheduled task in the process.</li>
 * </ul>
 * This context gets its own Redis container, which is stopped before the
 * outage tests run - a real address with nothing behind it, which is what
 * an outage looks like. (Overriding {@code spring.data.redis.host} would
 * not work: {@code @ServiceConnection} supplies the address through a
 * connection-details bean that takes precedence over the property.)
 */
@SpringBootTest
@Import({TestcontainersConfiguration.DatabaseAndKafka.class, RedisOutageAndSchedulerTests.DedicatedRedis.class})
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false"
})
class RedisOutageAndSchedulerTests {

    private static final Logger log = LoggerFactory.getLogger(RedisOutageAndSchedulerTests.class);

    /** A Redis of this context's own, so stopping it affects no other test. */
    @TestConfiguration(proxyBeanMethods = false)
    static class DedicatedRedis {

        @Bean
        @ServiceConnection(name = "redis")
        GenericContainer<?> outageRedisContainer() {
            return new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);
        }
    }

    // By name, not just by type: PostgreSQLContainer and KafkaContainer are
    // also GenericContainer subclasses, so the type alone is ambiguous here.
    @Autowired
    @Qualifier("outageRedisContainer")
    private GenericContainer<?> redisContainer;

    @Autowired
    private JobService jobService;

    @Autowired
    private DispatchService dispatchService;

    @Autowired
    private ExecutionService executionService;

    @Autowired
    private WorkerService workerService;

    @Autowired
    private WorkerLivenessCache livenessCache;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private TaskScheduler taskScheduler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetState() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
    }

    private void stopRedis() {
        if (redisContainer.isRunning()) {
            redisContainer.stop();
        }
    }

    // ---------- scheduler isolation ----------

    /**
     * The fix for the starvation bug: one task blocking must not stop
     * another from running.
     * <p>
     * Deterministic rather than timing-sensitive - with a pool larger than
     * one, a second thread is free the instant the task is submitted, so
     * the quick task completes while the blocking one is still asleep. With
     * the old pool of one it could not run at all until the blocker
     * finished, and the latch would time out.
     */
    @Test
    void oneBlockedScheduledTaskDoesNotStopAnother() throws Exception {
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        CountDownLatch quickTaskRan = new CountDownLatch(1);

        taskScheduler.schedule(() -> {
            blockerStarted.countDown();
            try {
                releaseBlocker.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, java.time.Instant.now());

        assertThat(blockerStarted.await(10, TimeUnit.SECONDS))
                .as("the blocking task should start")
                .isTrue();

        taskScheduler.schedule(quickTaskRan::countDown, java.time.Instant.now());

        assertThat(quickTaskRan.await(10, TimeUnit.SECONDS))
                .as("a second scheduled task must run while the first is blocked")
                .isTrue();

        releaseBlocker.countDown();
    }

    @Test
    void schedulerPoolIsLargeEnoughForEveryScheduledTaskInThisProcess() {
        // The control plane schedules the lease reaper and the outbox
        // publisher. One thread each, so neither can delay the other.
        //
        // Core pool size, not getPoolSize(): the latter counts threads that
        // currently exist, and a ScheduledThreadPoolExecutor only creates
        // them as tasks arrive. This context disables both scheduled tasks
        // so it would under-report. What is being asserted is the capacity
        // configured in application.yml, which is the actual fix.
        assertThat(taskScheduler).isInstanceOf(ThreadPoolTaskScheduler.class);
        assertThat(((ThreadPoolTaskScheduler) taskScheduler).getScheduledThreadPoolExecutor().getCorePoolSize())
                .as("pool must exceed the 2 scheduled tasks in the control plane")
                .isGreaterThanOrEqualTo(3);
    }

    // ---------- Redis outage ----------

    /**
     * The timeout fix. A Redis call against a dead server must fail in
     * about the configured second, not the default minute.
     */
    @Test
    void redisOperationsFailFastWhenRedisIsDown() {
        stopRedis();

        long start = System.nanoTime();
        livenessCache.markAlive("worker-" + UUID.randomUUID());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        log.info("Redis call against a stopped Redis returned after {} ms", elapsed.toMillis());

        // Generous upper bound: the point is that it is nowhere near the
        // 60s Lettuce default, not that it hits 1s exactly.
        assertThat(elapsed).isLessThan(Duration.ofSeconds(15));
    }

    @Test
    void workerRegistrationSucceedsWithRedisDown() {
        stopRedis();
        String workerId = "worker-" + UUID.randomUUID();

        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));

        // PostgreSQL is authoritative: the worker exists regardless of Redis.
        assertThat(workerService.requireActive(workerId)).isNotNull();
        // ...and the liveness projection simply reports nothing rather than failing.
        assertThat(livenessCache.isAlive(workerId)).isFalse();
    }

    /**
     * The invariant that matters: Redis is a cache, so losing it must not
     * touch job state at all.
     */
    @Test
    void theFullJobLifecycleStillWorksWithRedisDown() {
        stopRedis();
        String workerId = "worker-" + UUID.randomUUID();
        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));

        UUID jobId = jobService.createJob(new CreateJobRequest(
                "redis-down-" + UUID.randomUUID(), "TEST_JOB", Map.of("n", 1), 5, 3, null)).job().getId();

        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();
        assertThat(claimed.jobId()).isEqualTo(jobId);
        // Leases are PostgreSQL state, unaffected by Redis.
        assertThat(claimed.leaseUntil()).isNotNull();

        executionService.renewLease(jobId, workerId, claimed.executionId());
        executionService.complete(jobId, workerId, claimed.executionId());

        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.COMPLETED);
    }

    @Test
    void jobStateSurvivesRedisGoingDownMidLifecycle() {
        String workerId = "worker-" + UUID.randomUUID();
        workerService.register(new WorkerRegistrationRequest(workerId, "test-host", 4));
        UUID jobId = jobService.createJob(new CreateJobRequest(
                "mid-outage-" + UUID.randomUUID(), "TEST_JOB", Map.of("n", 1), 5, 3, null)).job().getId();
        ClaimedJob claimed = dispatchService.claim(workerId).orElseThrow();

        // Redis dies while the job is RUNNING.
        stopRedis();

        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.RUNNING);
        executionService.complete(jobId, workerId, claimed.executionId());
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus()).isEqualTo(JobStatus.COMPLETED);
    }
}
