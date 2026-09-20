package com.taskmesh.controlplane.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;

public interface JobEventRepository extends JpaRepository<JobEvent, Long> {

    List<JobEvent> findByAggregateIdOrderByIdAsc(String aggregateId);

    List<JobEvent> findByEventTypeOrderByIdAsc(JobEventType eventType);

    /**
     * Claims a batch of unpublished events for this publisher.
     * <p>
     * Ordered by id so events reach Kafka in the order they were recorded,
     * and {@code FOR UPDATE SKIP LOCKED} so several control-plane instances
     * can publish concurrently, each taking a disjoint batch rather than
     * fighting over - or duplicating - the same rows.
     */
    @Query(value = """
            SELECT * FROM job_events
             WHERE published_at IS NULL
             ORDER BY id ASC
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<JobEvent> lockUnpublishedBatch(@Param("limit") int limit);

    /**
     * Day 23 experiment. Claims a batch restricted to one ownership shard:
     * only rows whose {@code aggregate_id} hashes to {@code shard} of
     * {@code shards} are visible to this caller.
     * <p>
     * The point is that the shard predicate is a pure function of the key, so
     * two callers with different {@code shard} values can never see the same
     * row - and therefore never hold events for the same key at the same time.
     * The plain {@link #lockUnpublishedBatch(int)} above has no such boundary:
     * it claims by id range, and since one key's events are spread across the
     * id space, concurrent passes routinely hold the same key (Day 22 measured
     * ~20 of 20 keys in every pass).
     * <p>
     * {@code hashtext} is a PostgreSQL internal function. It is deterministic
     * within a major version, which is all a single-instance prototype needs,
     * but it is not a documented stable hash across versions - see
     * docs/day23-key-aware-publisher-experiment.md. The double modulo keeps the
     * result non-negative, since {@code hashtext} returns a signed int4 and
     * PostgreSQL's {@code %} preserves the sign of the dividend.
     * <p>
     * Used only when {@code taskmesh.outbox.key-aware-sharding} is enabled,
     * which is off by default.
     */
    @Query(value = """
            SELECT * FROM job_events
             WHERE published_at IS NULL
               AND ((hashtext(aggregate_id) % :shards) + :shards) % :shards = :shard
             ORDER BY id ASC
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<JobEvent> lockUnpublishedBatchForShard(@Param("limit") int limit,
            @Param("shards") int shards, @Param("shard") int shard);

    /**
     * Marks events published, after Kafka has acknowledged them. Guarded on
     * {@code published_at IS NULL} so it stays idempotent.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE job_events
               SET published_at = now()
             WHERE id IN (:ids)
               AND published_at IS NULL
            """, nativeQuery = true)
    int markPublished(@Param("ids") Collection<Long> ids);

    long countByPublishedAtIsNull();
}
