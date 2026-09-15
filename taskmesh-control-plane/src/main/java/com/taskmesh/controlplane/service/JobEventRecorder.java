package com.taskmesh.controlplane.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.taskmesh.controlplane.domain.Job;
import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.domain.Worker;
import com.taskmesh.controlplane.repository.JobEventRepository;

/**
 * Writes lifecycle events into the outbox.
 * <p>
 * Deliberately has no transaction management of its own and never talks to
 * Kafka. Every method here is called from inside a caller's transaction,
 * alongside the state change it describes, so the row is committed or
 * rolled back with that change and the two can never disagree. Handing the
 * event to Kafka is a separate, later, retryable step - see
 * {@link OutboxPublisher}.
 */
@Component
public class JobEventRecorder {

    private final JobEventRepository jobEventRepository;

    public JobEventRecorder(JobEventRepository jobEventRepository) {
        this.jobEventRepository = jobEventRepository;
    }

    /** Records an event describing a job, capturing its state at this moment. */
    public JobEvent recordJobEvent(JobEventType eventType, Job job) {
        return recordJobEvent(eventType, job, Map.of());
    }

    public JobEvent recordJobEvent(JobEventType eventType, Job job, Map<String, Object> extra) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("jobId", job.getId().toString());
        payload.put("type", job.getType());
        payload.put("status", job.getStatus().name());
        payload.put("priority", (int) job.getPriority());
        payload.put("attemptCount", job.getAttemptCount());
        payload.put("maxAttempts", job.getMaxAttempts());
        putIfPresent(payload, "scheduledAt", job.getScheduledAt());
        putIfPresent(payload, "assignedWorkerId", job.getAssignedWorkerId());
        putIfPresent(payload, "currentExecutionId", job.getCurrentExecutionId());
        putIfPresent(payload, "lastFailureReason", job.getLastFailureReason());
        payload.putAll(extra);

        return jobEventRepository.save(JobEvent.record(job.getId().toString(), eventType, payload));
    }

    public JobEvent recordWorkerEvent(JobEventType eventType, Worker worker) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("workerId", worker.getId());
        payload.put("hostname", worker.getHostname());
        payload.put("capacity", worker.getCapacity());
        payload.put("status", worker.getStatus().name());

        return jobEventRepository.save(JobEvent.record(worker.getId(), eventType, payload));
    }

    private static void putIfPresent(Map<String, Object> payload, String key, Object value) {
        if (value != null) {
            payload.put(key, value instanceof java.time.Instant || value instanceof java.util.UUID
                    ? value.toString()
                    : value);
        }
    }
}
