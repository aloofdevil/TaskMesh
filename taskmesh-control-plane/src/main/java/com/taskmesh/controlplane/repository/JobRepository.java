package com.taskmesh.controlplane.repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.taskmesh.controlplane.domain.Job;

public interface JobRepository extends JpaRepository<Job, UUID> {

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    /**
     * Cancels a job only if it is still QUEUED. This is the concurrency
     * guard for Day 2's only state transition: rather than reading the job,
     * checking its status in Java, and saving it back (which could race
     * with anything else touching the row), the WHERE clause makes the
     * transition itself the guard. The caller inspects the affected-row
     * count: 1 means the cancel happened, 0 means the job either does not
     * exist or was no longer QUEUED (the caller distinguishes those by a
     * follow-up lookup).
     * <p>
     * {@code clearAutomatically} drops the persistence context after this
     * bulk update so a subsequent {@code findById} in the same transaction
     * re-reads the row from the database instead of returning a stale
     * first-level-cache instance.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Job j
               set j.status = com.taskmesh.controlplane.domain.JobStatus.CANCELLED,
                   j.updatedAt = :now,
                   j.version = j.version + 1
             where j.id = :id
               and j.status = com.taskmesh.controlplane.domain.JobStatus.QUEUED
            """)
    int cancelIfQueued(@Param("id") UUID id, @Param("now") Instant now);
}
