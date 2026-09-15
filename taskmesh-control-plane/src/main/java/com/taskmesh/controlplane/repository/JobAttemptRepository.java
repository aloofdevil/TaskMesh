package com.taskmesh.controlplane.repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.taskmesh.controlplane.domain.JobAttempt;

public interface JobAttemptRepository extends JpaRepository<JobAttempt, UUID> {

    List<JobAttempt> findByJobIdOrderByAttemptNumberAsc(UUID jobId);

    Optional<JobAttempt> findByExecutionId(UUID executionId);

    // ------------------------------------------------------------------
    // Attempts are closed out by execution_id, which is UNIQUE, so each of
    // these targets at most one row. The "status = 'RUNNING'" guard makes
    // every one of them idempotent: an attempt that has already reached a
    // terminal state is never re-closed, so a retried request or a second
    // reaper pass cannot rewrite history.
    // ------------------------------------------------------------------

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE job_attempts
               SET status = 'SUCCEEDED', completed_at = now()
             WHERE execution_id = :executionId
               AND status = 'RUNNING'
            """, nativeQuery = true)
    int markSucceeded(@Param("executionId") UUID executionId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE job_attempts
               SET status = 'FAILED', completed_at = now(), failure_reason = :failureReason
             WHERE execution_id = :executionId
               AND status = 'RUNNING'
            """, nativeQuery = true)
    int markFailed(@Param("executionId") UUID executionId, @Param("failureReason") String failureReason);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE job_attempts
               SET status = 'LEASE_EXPIRED', completed_at = now(),
                   failure_reason = 'Lease expired; worker stopped renewing'
             WHERE execution_id IN (:executionIds)
               AND status = 'RUNNING'
            """, nativeQuery = true)
    int markLeaseExpired(@Param("executionIds") Collection<UUID> executionIds);
}
