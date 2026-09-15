package com.taskmesh.controlplane.repository;

import java.time.Instant;
import java.util.List;
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
     * The database's current time.
     * <p>
     * Claim eligibility is decided by PostgreSQL ({@code scheduled_at <= now()}),
     * so the default {@code scheduled_at} written at submission has to come
     * from the same clock. Stamping it from the JVM instead makes a job
     * ineligible for however far the application server's clock happens to
     * run ahead of the database's - a real effect, measured at ~35ms in
     * local Docker, and unbounded after a host suspends. One clock decides
     * scheduling, and it is this one.
     */
    @Query(value = "SELECT now()", nativeQuery = true)
    Instant databaseTime();

    /**
     * Selects and row-locks the next claimable jobs, highest priority
     * first. This is the core of the pull model.
     * <p>
     * {@code FOR UPDATE} takes a row lock on each selected row, held until
     * the surrounding transaction commits. {@code SKIP LOCKED} is what
     * makes concurrent claiming work: instead of blocking on a row another
     * transaction has already locked, this query steps over it and takes
     * the next eligible one. Two workers claiming simultaneously therefore
     * walk away with different jobs rather than queueing behind each other
     * for the same one - and the loser of a lock race is never handed a row
     * the winner is about to move out of QUEUED.
     * <p>
     * Eligibility is evaluated by PostgreSQL's {@code now()}, not by Java:
     * the database stays the single clock for scheduling decisions, so a
     * worker or control-plane instance with a skewed clock cannot make a
     * job eligible early.
     * <p>
     * Written as native SQL because JPQL cannot express
     * {@code FOR UPDATE SKIP LOCKED}. The caller must be inside a
     * transaction, otherwise the lock would be released immediately and the
     * guarantee would be lost.
     */
    @Query(value = """
            SELECT * FROM jobs
             WHERE status = 'QUEUED'
               AND scheduled_at <= now()
             ORDER BY priority DESC, scheduled_at ASC, id ASC
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Job> lockNextClaimableJobs(@Param("limit") int limit);

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
