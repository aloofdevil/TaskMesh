package com.taskmesh.controlplane.repository;

import java.time.Instant;
import java.util.Collection;
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

    // ------------------------------------------------------------------
    // Fenced execution writes.
    //
    // Each of the three statements below is a single conditional UPDATE
    // whose WHERE clause *is* the fence: the row changes only while this
    // worker's execution still owns it. There is deliberately no "read the
    // job, check ownership in Java, then write" anywhere - that pattern
    // has a window between the check and the write in which the reaper (or
    // another worker's claim) can take the job, and the write would then
    // land on an execution that no longer owns it. Here the check and the
    // write are the same statement, on a row PostgreSQL locks for its
    // duration, so an affected-row count of 0 is a definitive "you are
    // stale" and 1 is a definitive "you won".
    // ------------------------------------------------------------------

    /**
     * Extends the lease of the execution that currently owns the job.
     * <p>
     * {@code lease_until > now()} is part of the fence on purpose: a worker
     * whose lease has already lapsed must not be able to renew its way back
     * to life in the window before the reaper gets to the row, because the
     * job is, by then, already fair game for reassignment.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET lease_until = now() + (:leaseSeconds * interval '1 second'),
                   updated_at = now(),
                   version = version + 1
             WHERE id = :jobId
               AND status = 'RUNNING'
               AND assigned_worker_id = :workerId
               AND current_execution_id = :executionId
               AND lease_until > now()
            """, nativeQuery = true)
    int renewLease(@Param("jobId") UUID jobId, @Param("workerId") String workerId,
            @Param("executionId") UUID executionId, @Param("leaseSeconds") int leaseSeconds);

    /**
     * Marks the job COMPLETED on behalf of the owning execution.
     * <p>
     * Note there is no {@code lease_until > now()} condition here, unlike
     * renewal. If the lease has lapsed but the reaper has not yet requeued
     * the job, this execution is still the one that owns it and the work
     * genuinely was done - accepting the result is both safe and avoids
     * throwing away completed work. Once the reaper *has* acted,
     * {@code current_execution_id} no longer matches and this update
     * affects no rows, which is exactly the rejection we want.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'COMPLETED',
                   completed_at = now(),
                   lease_until = NULL,
                   updated_at = now(),
                   version = version + 1
             WHERE id = :jobId
               AND status = 'RUNNING'
               AND assigned_worker_id = :workerId
               AND current_execution_id = :executionId
            """, nativeQuery = true)
    int completeIfOwnedByExecution(@Param("jobId") UUID jobId, @Param("workerId") String workerId,
            @Param("executionId") UUID executionId);

    /**
     * Schedules a retry for a failed job on behalf of the owning execution.
     * <p>
     * The job goes to RETRYING with {@code scheduled_at} pushed into the
     * future by the backoff, which is what makes the delay real: the claim
     * query only sees QUEUED rows, and the promotion out of RETRYING is
     * itself gated on {@code scheduled_at <= now()}, so there is no way to
     * pick the job up early and no second queue to keep in step.
     * <p>
     * {@code attempt_count < max_attempts} is part of the WHERE clause so
     * the budget is re-checked by the database at the moment of the write,
     * not just in the Java that chose this branch.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'RETRYING',
                   assigned_worker_id = NULL,
                   current_execution_id = NULL,
                   lease_until = NULL,
                   last_failure_reason = :failureReason,
                   scheduled_at = now() + (:backoffSeconds * interval '1 second'),
                   updated_at = now(),
                   version = version + 1
             WHERE id = :jobId
               AND status = 'RUNNING'
               AND assigned_worker_id = :workerId
               AND current_execution_id = :executionId
               AND attempt_count < max_attempts
            """, nativeQuery = true)
    int scheduleRetryIfOwnedByExecution(@Param("jobId") UUID jobId, @Param("workerId") String workerId,
            @Param("executionId") UUID executionId, @Param("failureReason") String failureReason,
            @Param("backoffSeconds") long backoffSeconds);

    /** Dead-letters a job whose attempt budget is spent, on behalf of the owning execution. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'DEAD_LETTER',
                   assigned_worker_id = NULL,
                   current_execution_id = NULL,
                   lease_until = NULL,
                   last_failure_reason = :failureReason,
                   completed_at = now(),
                   updated_at = now(),
                   version = version + 1
             WHERE id = :jobId
               AND status = 'RUNNING'
               AND assigned_worker_id = :workerId
               AND current_execution_id = :executionId
               AND attempt_count >= max_attempts
            """, nativeQuery = true)
    int deadLetterIfOwnedByExecution(@Param("jobId") UUID jobId, @Param("workerId") String workerId,
            @Param("executionId") UUID executionId, @Param("failureReason") String failureReason);

    // ------------------------------------------------------------------
    // Lease-expiry variants. The worker is gone, so there is no execution
    // to match on; the fence is instead that the job is still RUNNING with
    // a lapsed lease, which is only true until something else touches it.
    // ------------------------------------------------------------------

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'RETRYING',
                   assigned_worker_id = NULL,
                   current_execution_id = NULL,
                   lease_until = NULL,
                   last_failure_reason = :failureReason,
                   scheduled_at = now() + (:backoffSeconds * interval '1 second'),
                   updated_at = now(),
                   version = version + 1
             WHERE id = :jobId
               AND status = 'RUNNING'
               AND lease_until < now()
               AND attempt_count < max_attempts
            """, nativeQuery = true)
    int scheduleRetryAfterLeaseExpiry(@Param("jobId") UUID jobId, @Param("failureReason") String failureReason,
            @Param("backoffSeconds") long backoffSeconds);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'DEAD_LETTER',
                   assigned_worker_id = NULL,
                   current_execution_id = NULL,
                   lease_until = NULL,
                   last_failure_reason = :failureReason,
                   completed_at = now(),
                   updated_at = now(),
                   version = version + 1
             WHERE id = :jobId
               AND status = 'RUNNING'
               AND lease_until < now()
               AND attempt_count >= max_attempts
            """, nativeQuery = true)
    int deadLetterAfterLeaseExpiry(@Param("jobId") UUID jobId, @Param("failureReason") String failureReason);

    // ------------------------------------------------------------------
    // Retry promotion: RETRYING -> QUEUED once the backoff has elapsed.
    // ------------------------------------------------------------------

    /**
     * Selects and row-locks jobs whose retry backoff has elapsed. The due
     * check uses PostgreSQL's {@code now()}, the same clock that wrote
     * {@code scheduled_at}, so a backoff means exactly what it said.
     */
    @Query(value = """
            SELECT * FROM jobs
             WHERE status = 'RETRYING'
               AND scheduled_at <= now()
             ORDER BY scheduled_at ASC
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Job> lockDueRetries(@Param("limit") int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'QUEUED',
                   updated_at = now(),
                   version = version + 1
             WHERE id IN (:jobIds)
               AND status = 'RETRYING'
               AND scheduled_at <= now()
            """, nativeQuery = true)
    int promoteDueRetries(@Param("jobIds") Collection<UUID> jobIds);

    // ------------------------------------------------------------------
    // Lease reaper.
    // ------------------------------------------------------------------

    /**
     * Selects and row-locks RUNNING jobs whose lease has lapsed, judged by
     * PostgreSQL's clock rather than any JVM's.
     * <p>
     * {@code SKIP LOCKED} makes the reaper safe to run in more than one
     * control-plane instance: concurrent reapers take disjoint sets of rows
     * instead of colliding, and a row already being completed by its worker
     * is skipped rather than fought over.
     */
    @Query(value = """
            SELECT * FROM jobs
             WHERE status = 'RUNNING'
               AND lease_until < now()
             ORDER BY lease_until ASC
             LIMIT :limit
             FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Job> lockExpiredLeases(@Param("limit") int limit);


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
