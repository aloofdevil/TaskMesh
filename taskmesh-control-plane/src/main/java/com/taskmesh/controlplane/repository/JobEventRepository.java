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
