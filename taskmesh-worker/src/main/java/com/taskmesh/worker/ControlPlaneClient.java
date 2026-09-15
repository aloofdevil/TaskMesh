package com.taskmesh.worker;

import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.common.worker.WorkerResponse;

/**
 * The worker's only channel to the rest of the system. The worker talks
 * HTTP to the control plane and nothing else - it has no PostgreSQL, Redis
 * or Kafka client, because the control plane owns all state.
 */
@Component
public class ControlPlaneClient {

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
}
