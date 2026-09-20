package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobEventRepository;

/**
 * Day 23: does restricting each publishing pass to a hash shard of the key
 * actually create an ownership boundary, or only appear to?
 * <p>
 * Days 21–22 located per-key ordering loss between the outbox claim and the
 * Kafka producer: every pass claims an id range, one key's events are spread
 * across the id space, so concurrent passes routinely hold the same key
 * (Day 22 measured 19.7–20.0 of 20 keys in every pass). These tests compare the
 * two claim models directly, with both claims <em>held at the same time</em>
 * rather than taken in sequence — holding the locks simultaneously is the whole
 * point, since the question is about concurrent ownership.
 * <p>
 * A passing test here does <strong>not</strong> establish production safety. It
 * establishes that the claim predicate partitions keys within one control-plane
 * instance. See the limitations in
 * docs/day23-key-aware-publisher-experiment.md.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "taskmesh.reliability.reaper-enabled=false",
        "taskmesh.outbox.publisher-enabled=false",
        // Its own topic: OutboxTests shares this broker and its consumer counts
        // every record on the topic before filtering by key (Day 22).
        "taskmesh.outbox.job-events-topic=taskmesh.job-events.key-ownership"
})
class OutboxKeyOwnershipTests {

    private static final int KEYS = 20;
    private static final int EVENTS_PER_KEY = 10;

    @Autowired
    private JobEventRepository jobEventRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private ExecutorService pool;
    private String run;

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, job_events, workers CASCADE");
        pool = Executors.newFixedThreadPool(4);
        run = UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    /**
     * Interleaved by (seq, key) so each key's events are spread across the id
     * space — the Day 22 workload shape, which is what makes id-range claims
     * span every key.
     */
    private void writeInterleavedEvents() {
        for (int seq = 1; seq <= EVENTS_PER_KEY; seq++) {
            for (int k = 0; k < KEYS; k++) {
                jobEventRepository.save(JobEvent.record(key(k), JobEventType.JOB_QUEUED,
                        Map.<String, Object>of("seq", seq)));
            }
        }
    }

    private String key(int k) {
        return "own-" + run + "-key" + k;
    }

    private int shardOf(String key, int shards) {
        Integer h = jdbcTemplate.queryForObject(
                "select ((hashtext(?) % ?) + ?) % ?", Integer.class, key, shards, shards, shards);
        return h == null ? -1 : h;
    }

    /**
     * Runs two claims concurrently, each holding its rows locked until both
     * have claimed, and returns the key set each one saw.
     */
    private List<Set<String>> claimConcurrently(int batchSize, Integer shards) throws Exception {
        CountDownLatch bothClaimed = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<Set<String>>> futures = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            final int shard = i;
            futures.add(pool.submit(() -> new TransactionTemplate(transactionManager).execute(status -> {
                List<JobEvent> claimed = shards == null
                        ? jobEventRepository.lockUnpublishedBatch(batchSize)
                        : jobEventRepository.lockUnpublishedBatchForShard(batchSize, shards, shard);
                Set<String> keys = new HashSet<>();
                claimed.forEach(e -> keys.add(e.getAggregateId()));
                bothClaimed.countDown();
                try {
                    // Hold the row locks until the other claim has also run, so
                    // the two claims genuinely overlap.
                    release.await(15, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return keys;
            })));
        }

        assertThat(bothClaimed.await(30, TimeUnit.SECONDS))
                .as("both claims should have run").isTrue();
        release.countDown();

        List<Set<String>> result = new ArrayList<>();
        for (Future<Set<String>> f : futures) {
            result.add(f.get(30, TimeUnit.SECONDS));
        }
        return result;
    }

    // ------------------------------------------------ the model comparison

    @Test
    void currentClaimLetsTwoConcurrentPassesHoldTheSameKeys() throws Exception {
        writeInterleavedEvents();

        List<Set<String>> claims = claimConcurrently(100, null);
        Set<String> shared = new HashSet<>(claims.get(0));
        shared.retainAll(claims.get(1));

        assertThat(claims.get(0)).isNotEmpty();
        assertThat(claims.get(1)).isNotEmpty();
        assertThat(shared)
                .as("this is the Day 21/22 root cause: id-range claims span every key, "
                        + "so two concurrent passes hold the same keys at the same time")
                .isNotEmpty();
    }

    @Test
    void shardedClaimGivesTwoConcurrentPassesDisjointKeys() throws Exception {
        writeInterleavedEvents();

        List<Set<String>> claims = claimConcurrently(100, 2);
        Set<String> shared = new HashSet<>(claims.get(0));
        shared.retainAll(claims.get(1));

        assertThat(claims.get(0)).isNotEmpty();
        assertThat(claims.get(1)).isNotEmpty();
        assertThat(shared)
                .as("no key may appear in two concurrently-held shards - this is the "
                        + "ownership invariant the prototype exists to provide")
                .isEmpty();
    }

    // ------------------------------------------------ mapping properties

    @Test
    void aKeyAlwaysMapsToTheSameShard() {
        for (int k = 0; k < KEYS; k++) {
            int first = shardOf(key(k), 4);
            assertThat(shardOf(key(k), 4))
                    .as("the shard of a key must be a pure function of the key")
                    .isEqualTo(first);
            assertThat(first).isBetween(0, 3);
        }
    }

    @Test
    void differentKeysCanMapToDifferentShards() {
        Set<Integer> shards = new HashSet<>();
        for (int k = 0; k < KEYS; k++) {
            shards.add(shardOf(key(k), 4));
        }
        assertThat(shards)
                .as("if every key landed on one shard the partitioning would be useless")
                .hasSizeGreaterThan(1);
    }

    @Test
    void everyEventOfAKeyIsVisibleToExactlyOneShard() {
        writeInterleavedEvents();

        // Union the claims of all 4 shards; each must contain only its own keys,
        // and together they must account for every event exactly once.
        Set<Long> seen = new HashSet<>();
        for (int shard = 0; shard < 4; shard++) {
            final int s = shard;
            List<JobEvent> claimed = new TransactionTemplate(transactionManager).execute(status ->
                    jobEventRepository.lockUnpublishedBatchForShard(KEYS * EVENTS_PER_KEY, 4, s));
            for (JobEvent e : claimed) {
                assertThat(shardOf(e.getAggregateId(), 4))
                        .as("shard %d claimed key %s which does not belong to it", s, e.getAggregateId())
                        .isEqualTo(s);
                assertThat(seen.add(e.getId()))
                        .as("event %d claimed by more than one shard", e.getId()).isTrue();
            }
        }
        assertThat(seen)
                .as("the shards must partition the outbox, not merely subdivide it")
                .hasSize(KEYS * EVENTS_PER_KEY);
    }

    // ------------------------------------------------ ordering within a shard

    @Test
    void publishingEveryShardPreservesPerKeyIdOrder() {
        writeInterleavedEvents();

        int shards = 4;
        for (int round = 0; round < 50 && jobEventRepository.countByPublishedAtIsNull() > 0; round++) {
            for (int shard = 0; shard < shards; shard++) {
                outboxPublisher.publishShard(shard, shards);
            }
        }
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();

        // Within each key, publication order must follow id order. published_at
        // is the pass identifier (Day 22): it is fixed at transaction start, so
        // a later event stamped with an earlier time means a pass overtook.
        Integer inversions = jdbcTemplate.queryForObject("""
                select count(*) from (
                  select published_at, lag(published_at) over (
                           partition by aggregate_id order by id) prev
                    from job_events) x
                 where prev is not null and published_at < prev""", Integer.class);
        assertThat(inversions)
                .as("no event may be published by a pass that started before the pass "
                        + "which published the preceding event for the same key")
                .isZero();
    }

    @Test
    void shardedPublishingStillPublishesEveryEventExactlyOnce() {
        writeInterleavedEvents();

        int published = 0;
        for (int round = 0; round < 50 && jobEventRepository.countByPublishedAtIsNull() > 0; round++) {
            for (int shard = 0; shard < 4; shard++) {
                published += outboxPublisher.publishShard(shard, 4);
            }
        }

        assertThat(published)
                .as("each event published once, no more and no fewer")
                .isEqualTo(KEYS * EVENTS_PER_KEY);
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "select count(*) from job_events where published_at is not null", Integer.class))
                .isEqualTo(KEYS * EVENTS_PER_KEY);
    }

    /**
     * The failure question: if the shard that owns a key never runs, what
     * happens to that key? This pins the answer, which is a real limitation of
     * the prototype rather than a property to be proud of.
     */
    @Test
    void aKeyWhoseShardNeverRunsStaysUnpublishedButIsNotLost() {
        writeInterleavedEvents();

        int shards = 4;
        int missing = shardOf(key(0), shards);
        for (int round = 0; round < 50; round++) {
            for (int shard = 0; shard < shards; shard++) {
                if (shard != missing) {
                    outboxPublisher.publishShard(shard, shards);
                }
            }
        }

        List<String> stuckKeys = jdbcTemplate.queryForList(
                "select distinct aggregate_id from job_events where published_at is null",
                String.class);
        assertThat(stuckKeys)
                .as("only the abandoned shard's keys are left behind")
                .isNotEmpty()
                .allSatisfy(k -> assertThat(shardOf(k, shards)).isEqualTo(missing));

        // Nothing is lost: another publisher covering that shard drains it, and
        // ordering is still intact afterwards.
        for (int round = 0; round < 50 && jobEventRepository.countByPublishedAtIsNull() > 0; round++) {
            outboxPublisher.publishShard(missing, shards);
        }
        assertThat(jobEventRepository.countByPublishedAtIsNull()).isZero();

        List<Integer> seqs = jdbcTemplate.queryForList(
                "select (payload->>'seq')::int from job_events where aggregate_id = ? order by published_at, id",
                Integer.class, key(0));
        assertThat(seqs)
                .as("the delayed shard still publishes its key in sequence order")
                .containsExactlyElementsOf(IntStream.rangeClosed(1, EVENTS_PER_KEY).boxed().toList());
    }
}
