package com.taskmesh.controlplane.api;

import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.taskmesh.common.worker.ExecutionReportRequest;
import com.taskmesh.common.worker.FailJobRequest;
import com.taskmesh.common.worker.LeaseResponse;
import com.taskmesh.controlplane.service.ExecutionService;

import jakarta.validation.Valid;

/**
 * The worker-facing endpoints for an execution in progress. Internal like
 * the rest of {@code /internal}: these carry an execution id that only the
 * worker holding the job knows, and they are not part of the public job
 * API.
 * <p>
 * Each returns 409 {@code STALE_EXECUTION} when the calling execution no
 * longer owns the job.
 */
@RestController
@RequestMapping("/internal/jobs")
public class JobExecutionController {

    private final ExecutionService executionService;

    public JobExecutionController(ExecutionService executionService) {
        this.executionService = executionService;
    }

    @PostMapping("/{jobId}/lease/renew")
    public LeaseResponse renewLease(@PathVariable UUID jobId, @Valid @RequestBody ExecutionReportRequest request) {
        return executionService.renewLease(jobId, request.workerId(), request.executionId());
    }

    @PostMapping("/{jobId}/complete")
    public ResponseEntity<Void> complete(@PathVariable UUID jobId, @Valid @RequestBody ExecutionReportRequest request) {
        executionService.complete(jobId, request.workerId(), request.executionId());
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{jobId}/fail")
    public ResponseEntity<Void> fail(@PathVariable UUID jobId, @Valid @RequestBody FailJobRequest request) {
        executionService.fail(jobId, request.workerId(), request.executionId(), request.failureReason());
        return ResponseEntity.noContent().build();
    }
}
