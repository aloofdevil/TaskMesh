package com.taskmesh.controlplane.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import com.taskmesh.common.worker.WorkerRegistrationRequest;
import com.taskmesh.controlplane.TestcontainersConfiguration;
import com.taskmesh.controlplane.domain.Worker;
import com.taskmesh.controlplane.domain.WorkerStatus;
import com.taskmesh.controlplane.repository.WorkerRepository;
import com.taskmesh.controlplane.service.WorkerLivenessCache;

import tools.jackson.databind.ObjectMapper;

/**
 * Worker registration / heartbeat / deregistration over HTTP, against a
 * real PostgreSQL and Redis (Testcontainers).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class WorkerApiTests {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private WorkerRepository workerRepository;

    @Autowired
    private WorkerLivenessCache livenessCache;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetState() {
        // The claim query is global ("give me the best eligible job"), so
        // these tests need a known-empty table rather than whatever earlier
        // test classes happened to leave behind.
        jdbcTemplate.execute("TRUNCATE jobs, job_attempts, workers CASCADE");
    }

    private static String uniqueWorkerId() {
        return "worker-" + UUID.randomUUID();
    }

    private void register(String workerId) throws Exception {
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(workerId, "test-host", 4);
        mockMvc.perform(post("/internal/workers/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk());
    }

    @Test
    void registrationCreatesAnActiveWorker() throws Exception {
        String workerId = uniqueWorkerId();
        WorkerRegistrationRequest request = new WorkerRegistrationRequest(workerId, "test-host", 4);

        mockMvc.perform(post("/internal/workers/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.workerId").value(workerId))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.capacity").value(4));

        Worker persisted = workerRepository.findById(workerId).orElseThrow();
        assertThat(persisted.getStatus()).isEqualTo(WorkerStatus.ACTIVE);
        assertThat(persisted.getHostname()).isEqualTo("test-host");
    }

    @Test
    void registrationIsAnUpsertSoARestartedWorkerReactivates() throws Exception {
        String workerId = uniqueWorkerId();
        register(workerId);

        mockMvc.perform(post("/internal/workers/{id}/deregister", workerId))
                .andExpect(status().isNoContent());
        assertThat(workerRepository.findById(workerId).orElseThrow().getStatus())
                .isEqualTo(WorkerStatus.DEREGISTERED);

        // Same worker comes back up: it must reactivate its own row, not create a second one.
        register(workerId);

        assertThat(workerRepository.findById(workerId).orElseThrow().getStatus()).isEqualTo(WorkerStatus.ACTIVE);
        assertThat(workerRepository.count()).isEqualTo(1);
    }

    @Test
    void registrationRecordsLivenessInRedis() throws Exception {
        String workerId = uniqueWorkerId();
        register(workerId);

        assertThat(livenessCache.isAlive(workerId)).isTrue();
    }

    @Test
    void heartbeatUpdatesLastHeartbeatAt() throws Exception {
        String workerId = uniqueWorkerId();
        register(workerId);
        var before = workerRepository.findById(workerId).orElseThrow().getLastHeartbeatAt();

        Thread.sleep(50);
        mockMvc.perform(post("/internal/workers/{id}/heartbeat", workerId))
                .andExpect(status().isNoContent());

        var after = workerRepository.findById(workerId).orElseThrow().getLastHeartbeatAt();
        assertThat(after).isAfter(before);
    }

    @Test
    void heartbeatForUnknownWorkerReturns404() throws Exception {
        mockMvc.perform(post("/internal/workers/{id}/heartbeat", uniqueWorkerId()))
                .andExpect(status().isNotFound());
    }

    @Test
    void heartbeatForDeregisteredWorkerReturnsConflict() throws Exception {
        String workerId = uniqueWorkerId();
        register(workerId);
        mockMvc.perform(post("/internal/workers/{id}/deregister", workerId))
                .andExpect(status().isNoContent());

        mockMvc.perform(post("/internal/workers/{id}/heartbeat", workerId))
                .andExpect(status().isConflict());
    }

    @Test
    void deregistrationMarksWorkerDeregisteredAndIsIdempotent() throws Exception {
        String workerId = uniqueWorkerId();
        register(workerId);

        mockMvc.perform(post("/internal/workers/{id}/deregister", workerId))
                .andExpect(status().isNoContent());
        mockMvc.perform(post("/internal/workers/{id}/deregister", workerId))
                .andExpect(status().isNoContent());

        assertThat(workerRepository.findById(workerId).orElseThrow().getStatus())
                .isEqualTo(WorkerStatus.DEREGISTERED);
        assertThat(livenessCache.isAlive(workerId)).isFalse();
    }

    @Test
    void deregisteringUnknownWorkerReturns404() throws Exception {
        mockMvc.perform(post("/internal/workers/{id}/deregister", uniqueWorkerId()))
                .andExpect(status().isNotFound());
    }

    @Test
    void registrationRejectsInvalidInput() throws Exception {
        String json = """
                {"workerId":"   ","hostname":"test-host","capacity":4}
                """;

        mockMvc.perform(post("/internal/workers/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("workerId")));
    }
}
