package com.taskmesh.controlplane.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import tools.jackson.databind.ObjectMapper;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.api.dto.CreateJobRequest;
import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.repository.JobRepository;
import com.taskmesh.controlplane.service.JobService;

/**
 * End-to-end tests for the Day 2 job management API, run against a real
 * Postgres (Testcontainers) so that uniqueness, persistence, and the
 * conditional-cancel query are exercised for real rather than mocked.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class JobApiTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobService jobService;

    private static String uniqueKey(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private CreateJobRequest requestWith(String idempotencyKey, Map<String, Object> payload) {
        return new CreateJobRequest(idempotencyKey, "TEST_JOB", payload, 5, 3, null);
    }

    // ---------- create ----------

    @Test
    void createJob_isPersistedAsQueuedInPostgres() throws Exception {
        String key = uniqueKey("create");
        CreateJobRequest request = requestWith(key, Map.of("message", "hello"));

        String body = mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.type").value("TEST_JOB"))
                .andExpect(jsonPath("$.attemptCount").value(0))
                .andExpect(jsonPath("$.maxAttempts").value(3))
                .andReturn().getResponse().getContentAsString();

        UUID id = UUID.fromString(objectMapper.readTree(body).get("id").asText());

        Job persisted = jobRepository.findById(id).orElseThrow();
        assertThat(persisted.getIdempotencyKey()).isEqualTo(key);
        assertThat(persisted.getStatus().name()).isEqualTo("QUEUED");
        assertThat(persisted.getPayload()).containsEntry("message", "hello");
    }

    // ---------- idempotency ----------

    @Test
    void sameIdempotencyKeyAndPayload_returnsExistingJobInsteadOfCreatingAnother() throws Exception {
        String key = uniqueKey("idem-same");
        CreateJobRequest request = requestWith(key, Map.of("message", "hello"));

        String firstBody = mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String firstId = objectMapper.readTree(firstBody).get("id").asText();

        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(firstId));

        assertThat(jobRepository.findAll().stream()
                .filter(j -> key.equals(j.getIdempotencyKey())))
                .hasSize(1);
    }

    @Test
    void sameIdempotencyKeyDifferentPayload_returnsConflict() throws Exception {
        String key = uniqueKey("idem-diff");
        CreateJobRequest first = requestWith(key, Map.of("message", "hello"));
        CreateJobRequest second = requestWith(key, Map.of("message", "goodbye"));

        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(first)))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(second)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));

        assertThat(jobRepository.findAll().stream()
                .filter(j -> key.equals(j.getIdempotencyKey())))
                .hasSize(1);
    }

    @Test
    void concurrentSubmissionsWithSameKey_createExactlyOneJob() throws Exception {
        String key = uniqueKey("idem-race");
        CreateJobRequest request = requestWith(key, Map.of("message", "race"));
        int attempts = 8;

        ExecutorService pool = Executors.newFixedThreadPool(attempts);
        CountDownLatch ready = new CountDownLatch(attempts);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger created = new AtomicInteger();
        List<UUID> jobIds = new java.util.concurrent.CopyOnWriteArrayList<>();

        List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < attempts; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                JobService.JobCreationResult result = jobService.createJob(request);
                if (result.created()) {
                    created.incrementAndGet();
                }
                jobIds.add(result.job().getId());
            }));
        }

        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(created.get()).isEqualTo(1);
        assertThat(jobIds).hasSize(attempts).containsOnly(jobIds.get(0));
        assertThat(jobRepository.findAll().stream()
                .filter(j -> key.equals(j.getIdempotencyKey())))
                .hasSize(1);
    }

    // ---------- get ----------

    @Test
    void getExistingJob_returnsIt() throws Exception {
        String key = uniqueKey("get");
        CreateJobRequest request = requestWith(key, Map.of("message", "hello"));
        String body = mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andReturn().getResponse().getContentAsString();
        String id = objectMapper.readTree(body).get("id").asText();

        mockMvc.perform(get("/jobs/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void getUnknownJob_returns404() throws Exception {
        mockMvc.perform(get("/jobs/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    // ---------- cancel ----------

    @Test
    void cancelQueuedJob_transitionsToCancelled() throws Exception {
        String key = uniqueKey("cancel-ok");
        CreateJobRequest request = requestWith(key, Map.of("message", "hello"));
        String body = mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andReturn().getResponse().getContentAsString();
        String id = objectMapper.readTree(body).get("id").asText();

        mockMvc.perform(post("/jobs/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mockMvc.perform(get("/jobs/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void cancellingAnAlreadyCancelledJob_returnsConflict() throws Exception {
        String key = uniqueKey("cancel-twice");
        CreateJobRequest request = requestWith(key, Map.of("message", "hello"));
        String body = mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andReturn().getResponse().getContentAsString();
        String id = objectMapper.readTree(body).get("id").asText();

        mockMvc.perform(post("/jobs/{id}/cancel", id)).andExpect(status().isOk());

        mockMvc.perform(post("/jobs/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    void cancellingUnknownJob_returns404() throws Exception {
        mockMvc.perform(post("/jobs/{id}/cancel", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    // ---------- validation ----------

    @Test
    void missingIdempotencyKey_returns400() throws Exception {
        String json = """
                {"type":"TEST_JOB","payload":{"message":"hello"}}
                """;

        mockMvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("idempotencyKey")));
    }

    @Test
    void blankJobType_returns400() throws Exception {
        String json = """
                {"idempotencyKey":"%s","type":"   ","payload":{"message":"hello"}}
                """.formatted(uniqueKey("blank-type"));

        mockMvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("type")));
    }

    @Test
    void priorityOutOfRange_returns400() throws Exception {
        String json = """
                {"idempotencyKey":"%s","type":"TEST_JOB","payload":{"message":"hello"},"priority":99}
                """.formatted(uniqueKey("bad-priority"));

        mockMvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("priority")));
    }

    @Test
    void maxAttemptsBelowOne_returns400() throws Exception {
        String json = """
                {"idempotencyKey":"%s","type":"TEST_JOB","payload":{"message":"hello"},"maxAttempts":0}
                """.formatted(uniqueKey("bad-max-attempts"));

        mockMvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("maxAttempts")));
    }
}
