package com.taskmesh.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

/**
 * Day 24: which claim path the scheduler actually dispatches to, per
 * concurrency, when key-aware sharding is enabled.
 * <p>
 * This exists because the Day 24 experiment found that the two-instance
 * {@code C=1} arm emitted no sharded claims at all. The cause is in
 * {@link OutboxPublisherScheduler#publish()}, which short-circuits:
 * <pre>
 *   concurrency == 1 ? publisher.publishPending() : publishConcurrently(...)
 * </pre>
 * At concurrency 1 that reaches {@code publishPending()} — the <em>unsharded</em>
 * claim — so {@code key-aware-sharding=true} has no effect whatsoever. Anyone
 * setting the flag on a single-threaded publisher would get none of the
 * behaviour it advertises, and nothing would say so.
 * <p>
 * Pinned here so the limitation is a tested fact rather than a footnote, and so
 * a future fix to the short-circuit is a deliberate change to a failing test
 * rather than a silent behavioural shift.
 * <p>
 * No Spring context, no broker: this is about dispatch, so a recording stub is
 * enough and keeps the test fast.
 */
class OutboxShardDispatchTests {

    /** Records which claim path the scheduler chose, without touching a database. */
    private static final class RecordingPublisher extends OutboxPublisher {

        private final List<String> calls = new CopyOnWriteArrayList<>();

        private RecordingPublisher() {
            super(null, null, null, null, null);
        }

        @Override
        public int publishPending() {
            calls.add("unsharded");
            return 0;
        }

        @Override
        public int publishShard(int shard, int shards) {
            calls.add("shard=" + shard + "/" + shards);
            return 0;
        }
    }

    private static OutboxProperties properties(int concurrency, boolean keyAware) {
        return new OutboxProperties(true, 100L, 100, concurrency, 5000L, false, keyAware,
                "taskmesh.job-events", "taskmesh.worker-events", 3);
    }

    @Test
    void atConcurrencyOneKeyAwareShardingIsNotAppliedAtAll() {
        RecordingPublisher publisher = new RecordingPublisher();
        OutboxPublisherScheduler scheduler =
                new OutboxPublisherScheduler(publisher, properties(1, true));

        scheduler.publish();

        assertThat(publisher.calls)
                .as("the concurrency==1 short-circuit reaches the unsharded claim, so "
                        + "key-aware-sharding=true is silently inert at concurrency 1")
                .containsExactly("unsharded");
        scheduler.shutdown();
    }

    @Test
    void aboveConcurrencyOneEachTaskClaimsItsOwnShard() {
        RecordingPublisher publisher = new RecordingPublisher();
        OutboxPublisherScheduler scheduler =
                new OutboxPublisherScheduler(publisher, properties(4, true));

        scheduler.publish();

        assertThat(publisher.calls)
                .as("one pass per shard, shard index matching task index, shard count "
                        + "equal to the configured concurrency")
                .containsExactlyInAnyOrder("shard=0/4", "shard=1/4", "shard=2/4", "shard=3/4");
        scheduler.shutdown();
    }

    @Test
    void shardingOffUsesTheUnshardedClaimAtEveryConcurrency() {
        for (int concurrency : new int[] {1, 2, 4}) {
            RecordingPublisher publisher = new RecordingPublisher();
            OutboxPublisherScheduler scheduler =
                    new OutboxPublisherScheduler(publisher, properties(concurrency, false));

            scheduler.publish();

            List<String> expected = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                expected.add("unsharded");
            }
            assertThat(publisher.calls)
                    .as("with the flag off, concurrency %d takes the original claim path", concurrency)
                    .containsExactlyElementsOf(expected);
            scheduler.shutdown();
        }
    }

    /**
     * The shard count is the instance's own concurrency, with no coordination —
     * which is exactly why two instances both cover shards 0..C-1 and the Day 24
     * experiment found every shard processed by both of them.
     */
    @Test
    void shardCountIsTheLocalConcurrencyWithNoExternalCoordination() {
        RecordingPublisher a = new RecordingPublisher();
        RecordingPublisher b = new RecordingPublisher();
        OutboxPublisherScheduler instanceA = new OutboxPublisherScheduler(a, properties(2, true));
        OutboxPublisherScheduler instanceB = new OutboxPublisherScheduler(b, properties(2, true));

        instanceA.publish();
        instanceB.publish();

        assertThat(a.calls).containsExactlyInAnyOrder("shard=0/2", "shard=1/2");
        assertThat(b.calls)
                .as("a second scheduler claims the identical shard set - nothing divides "
                        + "shards between instances")
                .containsExactlyInAnyOrderElementsOf(a.calls);
        instanceA.shutdown();
        instanceB.shutdown();
    }
}
