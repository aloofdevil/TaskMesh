package com.taskmesh.controlplane.service;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.domain.Worker;
import com.taskmesh.controlplane.repository.WorkerRepository;

@Service
public class WorkerService {

    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final WorkerRepository workerRepository;
    private final WorkerLivenessCache livenessCache;
    private final JobEventRecorder eventRecorder;

    public WorkerService(WorkerRepository workerRepository, WorkerLivenessCache livenessCache,
            JobEventRecorder eventRecorder) {
        this.workerRepository = workerRepository;
        this.livenessCache = livenessCache;
        this.eventRecorder = eventRecorder;
    }

    /**
     * Registers a worker, or re-activates one that already exists under the
     * same id. Workers pick their own ids (pod name in Kubernetes), so a
     * restarted worker must come back as itself instead of leaking a second
     * row - which makes registration an upsert rather than an insert.
     */
    @Transactional
    public Worker register(WorkerRegistrationRequest request) {
        Worker worker = workerRepository.findById(request.workerId())
                .map(existing -> {
                    existing.reregister(request.hostname(), request.capacity());
                    return existing;
                })
                .orElseGet(() -> Worker.register(request.workerId(), request.hostname(), request.capacity()));

        Worker saved = workerRepository.save(worker);
        eventRecorder.recordWorkerEvent(JobEventType.WORKER_REGISTERED, saved);
        livenessCache.markAlive(saved.getId());
        log.info("Worker {} registered from {} with capacity {}", saved.getId(), saved.getHostname(),
                saved.getCapacity());
        return saved;
    }

    /**
     * Records a heartbeat. The conditional update is the guard: 0 affected
     * rows means the worker is either unknown (404) or no longer ACTIVE
     * (409), and a follow-up read tells those apart.
     */
    @Transactional
    public void heartbeat(String workerId) {
        int updated = workerRepository.recordHeartbeat(workerId, Instant.now());
        if (updated == 0) {
            Worker existing = workerRepository.findById(workerId)
                    .orElseThrow(() -> new WorkerNotFoundException(workerId));
            throw new WorkerNotActiveException(workerId, existing.getStatus());
        }
        livenessCache.markAlive(workerId);
    }

    /** Marks a worker DEREGISTERED. Idempotent; 404 only if the worker was never registered. */
    @Transactional
    public void deregister(String workerId) {
        int updated = workerRepository.deregister(workerId);
        if (updated == 0) {
            throw new WorkerNotFoundException(workerId);
        }
        workerRepository.findById(workerId)
                .ifPresent(worker -> eventRecorder.recordWorkerEvent(JobEventType.WORKER_DEREGISTERED, worker));
        livenessCache.clear(workerId);
        log.info("Worker {} deregistered", workerId);
    }

    @Transactional(readOnly = true)
    public Worker requireActive(String workerId) {
        Worker worker = workerRepository.findById(workerId)
                .orElseThrow(() -> new WorkerNotFoundException(workerId));
        if (!worker.isActive()) {
            throw new WorkerNotActiveException(workerId, worker.getStatus());
        }
        return worker;
    }
}
