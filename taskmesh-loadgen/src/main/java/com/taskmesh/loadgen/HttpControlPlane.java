package com.taskmesh.loadgen;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import com.taskmesh.common.worker.ClaimedJob;
import com.taskmesh.common.worker.ExecutionReportRequest;
import com.taskmesh.common.worker.FailJobRequest;
import com.taskmesh.common.worker.WorkerRegistrationRequest;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * The real protocol, over one shared {@link HttpClient}.
 * <p>
 * <strong>One client for the entire generator</strong>, not one per logical
 * worker. The JDK client keeps its own connection pool and is designed to
 * be shared; giving 25,000 logical workers a client each would mean 25,000
 * independent pools and selector threads, which would make the generator
 * fail long before the control plane did.
 * <p>
 * Requests are sent with blocking {@code send()} on purpose. Each logical
 * worker runs on a virtual thread, so blocking parks that virtual thread
 * and releases its carrier rather than holding an OS thread - which is what
 * makes thousands of simultaneously-blocked workers affordable, and what
 * keeps the simulated sequencing identical to the real worker's.
 * <p>
 * Request bodies and paths mirror {@code ControlPlaneClient} and the
 * {@code WorkerController} / {@code JobExecutionController} contracts
 * exactly, using the same {@code taskmesh-common} records the real worker
 * serializes, so the simulator cannot drift from the production protocol.
 */
public final class HttpControlPlane implements ControlPlane {

    /** Matches the control plane's fencing error code. */
    private static final String STALE_EXECUTION = "STALE_EXECUTION";

    private static final int NO_CONTENT = 204;
    private static final int CONFLICT = 409;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final Duration requestTimeout;
    private final LoadGenMetrics metrics;

    public HttpControlPlane(HttpClient httpClient, String baseUrl, Duration requestTimeout, LoadGenMetrics metrics) {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.requestTimeout = requestTimeout;
        this.metrics = metrics;
        this.objectMapper = JsonMapper.builder()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    @Override
    public void register(String workerId, String hostname, int capacity) {
        post("/internal/workers/register", new WorkerRegistrationRequest(workerId, hostname, capacity), null);
    }

    @Override
    public void heartbeat(String workerId) {
        post("/internal/workers/" + workerId + "/heartbeat", null, null);
    }

    @Override
    public void deregister(String workerId) {
        post("/internal/workers/" + workerId + "/deregister", null, null);
    }

    @Override
    public Optional<ClaimedJob> claim(String workerId) {
        HttpResponse<String> response = post("/internal/workers/" + workerId + "/claim", null, null);
        if (response.statusCode() == NO_CONTENT) {
            return Optional.empty();
        }
        String body = response.body();
        if (body == null || body.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(objectMapper.readValue(body, ClaimedJob.class));
    }

    @Override
    public void renewLease(UUID jobId, String workerId, UUID executionId) {
        post("/internal/jobs/" + jobId + "/lease/renew", new ExecutionReportRequest(workerId, executionId), executionId);
    }

    @Override
    public void complete(UUID jobId, String workerId, UUID executionId) {
        post("/internal/jobs/" + jobId + "/complete", new ExecutionReportRequest(workerId, executionId), executionId);
    }

    @Override
    public void fail(UUID jobId, String workerId, UUID executionId, String failureReason) {
        post("/internal/jobs/" + jobId + "/fail",
                new FailJobRequest(workerId, executionId, failureReason), executionId);
    }

    /**
     * @param fencedExecutionId when non-null, a 409 carrying STALE_EXECUTION
     *                          is translated into {@link StaleExecutionException}
     *                          rather than a generic protocol error - the same
     *                          distinction the real {@code ControlPlaneClient} makes.
     */
    private HttpResponse<String> post(String path, Object body, UUID fencedExecutionId) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(requestTimeout);

        if (body == null) {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)));
        }

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            metrics.httpTransportError(e.getClass().getSimpleName());
            throw new ControlPlaneException("Transport failure calling " + path, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.httpTransportError("InterruptedException");
            throw new ControlPlaneException("Interrupted calling " + path, e);
        }

        metrics.httpStatus(response.statusCode());

        if (fencedExecutionId != null
                && response.statusCode() == CONFLICT
                && response.body() != null
                && response.body().contains(STALE_EXECUTION)) {
            throw new StaleExecutionException(fencedExecutionId, response.body());
        }
        if (response.statusCode() >= 400) {
            throw new ControlPlaneException(
                    "HTTP " + response.statusCode() + " from " + path + ": " + truncate(response.body()));
        }
        return response;
    }

    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 200 ? body : body.substring(0, 200) + "...";
    }
}
