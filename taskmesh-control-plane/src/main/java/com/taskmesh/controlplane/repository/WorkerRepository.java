package com.taskmesh.controlplane.repository;

import java.time.Instant;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.taskmesh.controlplane.domain.Worker;

public interface WorkerRepository extends JpaRepository<Worker, String> {

    /**
     * Records a heartbeat for an ACTIVE worker. Scoped to ACTIVE so a
     * heartbeat cannot silently resurrect a worker that has deregistered;
     * the caller turns an affected-row count of 0 into either 404 (no such
     * worker) or 409 (worker is not ACTIVE).
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Worker w
               set w.lastHeartbeatAt = :now
             where w.id = :id
               and w.status = com.taskmesh.controlplane.domain.WorkerStatus.ACTIVE
            """)
    int recordHeartbeat(@Param("id") String id, @Param("now") Instant now);

    /**
     * Marks a worker DEREGISTERED. Not scoped to ACTIVE, so deregistering
     * twice is idempotent rather than an error.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Worker w
               set w.status = com.taskmesh.controlplane.domain.WorkerStatus.DEREGISTERED
             where w.id = :id
            """)
    int deregister(@Param("id") String id);
}
