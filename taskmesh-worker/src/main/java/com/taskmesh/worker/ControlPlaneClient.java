package com.taskmesh.worker;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.ExecutionReportRequest;
import com.taskmesh.common.worker.FailJobRequest;
import com.taskmesh.common.worker.LeaseResponse;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.common.worker.WorkerResponse;

/**
 * The worker's only channel to the rest of the system. The worker talks
 * HTTP to the control plane and nothing else - it has no PostgreSQL, Redis
 * or Kafka client, because the control plane owns all state.
 */
@Component
public class ControlPlaneClient {

    /** Error code the control plane returns when this execution has been fenced. */
    private static final String STALE_EXECUTION = "STALE_EXECUTION";

    private final RestClient restClient;

    public ControlPlaneClient(RestClient.Builder builder, WorkerProperties properties) {
        this.restClient = builder.baseUrl(properties.controlPlaneUrl()).build();
    }

    public WorkerResponse register(WorkerRegistrationRequest request) {
        return restClient.post()
                .uri("/internal/workers/register")
                .body(request)
                .retrieve()
                .body(WorkerResponse.class);
    }

    public void heartbeat(String workerId) {
        restClient.post()
                .uri("/internal/workers/{workerId}/heartbeat", workerId)
                .retrieve()
                .toBodilessEntity();
    }

    public void deregister(String workerId) {
        restClient.post()
                .uri("/internal/workers/{workerId}/deregister", workerId)
                .retrieve()
                .toBodilessEntity();
    }

    /**
     * Asks for work. The control plane answers 204 when nothing is
     * eligible, which is a normal answer rather than an error - it becomes
     * an empty Optional and the worker waits before asking again.
     */
    public Optional<ClaimedJob> claim(String workerId) {
        ResponseEntity<ClaimedJob> response = restClient.post()
                .uri("/internal/workers/{workerId}/claim", workerId)
                .retrieve()
                .toEntity(ClaimedJob.class);

        if (response.getStatusCode() == HttpStatus.NO_CONTENT) {
            return Optional.empty();
        }
        return Optional.ofNullable(response.getBody());
    }

    public LeaseResponse renewLease(UUID jobId, String workerId, UUID executionId) {
        return execution(() -> restClient.post()
                .uri("/internal/jobs/{jobId}/lease/renew", jobId)
                .body(new ExecutionReportRequest(workerId, executionId))
                .retrieve()
                .body(LeaseResponse.class));
    }

    public void complete(UUID jobId, String workerId, UUID executionId) {
        execution(() -> restClient.post()
                .uri("/internal/jobs/{jobId}/complete", jobId)
                .body(new ExecutionReportRequest(workerId, executionId))
                .retrieve()
                .toBodilessEntity());
    }

    public void fail(UUID jobId, String workerId, UUID executionId, String failureReason) {
        execution(() -> restClient.post()
                .uri("/internal/jobs/{jobId}/fail", jobId)
                .body(new FailJobRequest(workerId, executionId, failureReason))
                .retrieve()
                .toBodilessEntity());
    }

    /**
     * Turns the control plane's fencing rejection into a typed exception.
     * A 409 carrying {@code STALE_EXECUTION} is not a transient error to
     * retry - it means another execution owns the job now, so the caller
     * must give up on this one rather than keep trying.
     */
    private <T> T execution(Supplier<T> call) {
        try {
            return call.get();
        } catch (HttpClientErrorException.Conflict e) {
            String body = e.getResponseBodyAsString();
            if (body != null && body.contains(STALE_EXECUTION)) {
                throw new StaleExecutionException(body);
            }
            throw e;
        }
    }
}
