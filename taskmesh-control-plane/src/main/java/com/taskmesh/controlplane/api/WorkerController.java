package com.taskmesh.controlplane.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.common.worker.WorkerResponse;
import com.taskmesh.controlplane.domain.Worker;
import com.taskmesh.controlplane.service.DispatchService;
import com.taskmesh.controlplane.service.WorkerService;

import jakarta.validation.Valid;

/**
 * The internal API workers talk to. Kept under {@code /internal} to mark it
 * as the worker-facing surface rather than the client-facing job API.
 */
@RestController
@RequestMapping("/internal/workers")
public class WorkerController {

    private final WorkerService workerService;
    private final DispatchService dispatchService;

    public WorkerController(WorkerService workerService, DispatchService dispatchService) {
        this.workerService = workerService;
        this.dispatchService = dispatchService;
    }

    @PostMapping("/register")
    public WorkerResponse register(@Valid @RequestBody WorkerRegistrationRequest request) {
        return toResponse(workerService.register(request));
    }

    @PostMapping("/{workerId}/heartbeat")
    public ResponseEntity<Void> heartbeat(@PathVariable String workerId) {
        workerService.heartbeat(workerId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{workerId}/deregister")
    public ResponseEntity<Void> deregister(@PathVariable String workerId) {
        workerService.deregister(workerId);
        return ResponseEntity.noContent().build();
    }

    /**
     * The pull endpoint: a worker asking for work. 204 means "nothing
     * eligible right now", which the worker treats as a signal to wait and
     * poll again rather than as an error.
     */
    @PostMapping("/{workerId}/claim")
    public ResponseEntity<ClaimedJob> claim(@PathVariable String workerId) {
        return dispatchService.claim(workerId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    private WorkerResponse toResponse(Worker worker) {
        return new WorkerResponse(
                worker.getId(),
                worker.getHostname(),
                worker.getCapacity(),
                worker.getStatus().name(),
                worker.getRegisteredAt(),
                worker.getLastHeartbeatAt());
    }
}
