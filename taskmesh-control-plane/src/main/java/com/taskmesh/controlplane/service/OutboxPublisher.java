package com.taskmesh.controlplane.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskmesh.controlplane.domain.JobEvent;
import com.taskmesh.controlplane.domain.JobEventType;
import com.taskmesh.controlplane.repository.JobEventRepository;

import tools.jackson.databind.ObjectMapper;

/**
 * Forwards committed outbox events to Kafka.
 * <p>
 * This is the second half of the transactional outbox, and the only place
 * in the control plane that talks to Kafka. Nothing on a request path
 * publishes: state changes commit to PostgreSQL with their events, and this
 * runs afterwards, separately. That is what makes a Kafka outage survivable
 * - jobs keep being submitted, claimed, completed and retried while events
 * simply accumulate unpublished, and drain in order once Kafka returns.
 * <p>
 * <strong>Delivery semantics: at-least-once.</strong> An event is marked
 * published only after Kafka acknowledges it, so an event is never lost.
 * But if the process dies between Kafka's acknowledgement and the
 * {@code published_at} update, the next pass will send it again. The event
 * carries its stable {@code job_events.id} in the message, so a consumer
 * that cares can deduplicate on it. Exactly-once delivery is not
 * implemented and is not claimed.
 */
@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /**
     * Day 24 instrumentation. Identifies this JVM in the per-pass claim log, so
     * a multi-instance run can be attributed to instances after the fact. Fresh
     * per process start; deliberately not persisted anywhere, since nothing in
     * the production data model carries instance identity.
     */
    private static final String INSTANCE_ID = java.util.UUID.randomUUID().toString().substring(0, 8);

    private final JobEventRepository jobEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxProperties properties;
    private final TaskMeshMetrics metrics;

    /**
     * Day 19 instrumentation only. Phase timings of the pass this thread just
     * ran, in nanoseconds: {@code [claim, send, mark]}. Each pass runs on its
     * own pool thread and the scheduler reads this on that same thread
     * immediately after {@link #publishPending()} returns, so there is no
     * sharing between passes. It exists so the scheduler can log one
     * aggregated decomposition per tick without {@code publishPending()}
     * changing the value it returns.
     */
    private final ThreadLocal<long[]> lastPassPhases = ThreadLocal.withInitial(() -> new long[3]);

    long[] lastPassPhases() {
        return lastPassPhases.get();
    }

    public OutboxPublisher(JobEventRepository jobEventRepository, KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper, OutboxProperties properties, TaskMeshMetrics metrics) {
        this.jobEventRepository = jobEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Day 23 experiment. Publishes one batch restricted to an ownership shard:
     * only events whose key hashes to {@code shard} of {@code shards}.
     * <p>
     * Identical to {@link #publishPending()} in every respect except which rows
     * the claim can see. Called only when
     * {@code taskmesh.outbox.key-aware-sharding} is on, which is off by default,
     * so the shipped path is untouched.
     */
    @Transactional
    public int publishShard(int shard, int shards) {
        return publish(shard, shards);
    }

    /**
     * Publishes one batch and returns how many events were confirmed.
     * <p>
     * Events are marked published only after Kafka acknowledges them, and
     * only those that were acknowledged - so a partial failure leaves the
     * rest unpublished for a later pass rather than losing them. The whole
     * pass is one transaction holding row locks on the batch, which keeps
     * two control-plane instances from publishing the same rows.
     */
    @Transactional
    public int publishPending() {
        return publish(-1, -1);
    }

    /** {@code shards <= 0} means the original unsharded claim. */
    private int publish(int shard, int shards) {
        // Day 19 instrumentation. Every timer here is recorded only for a
        // pass that actually claimed rows. That is deliberate: a saturated
        // drain is followed by many passes that find an empty outbox and
        // return in microseconds, and Day 18 showed what including those
        // does to a mean - hikaricp.connections.usage fell from 355ms to
        // 83ms purely because faster polling added empty passes, which
        // looks like passes getting faster and is not. Timing only working
        // passes keeps these four meters comparable across concurrencies.
        long[] phases = lastPassPhases.get();
        phases[0] = 0;
        phases[1] = 0;
        phases[2] = 0;

        long passStart = System.nanoTime();
        List<JobEvent> pending = shards > 0
                ? jobEventRepository.lockUnpublishedBatchForShard(properties.batchSize(), shards, shard)
                : jobEventRepository.lockUnpublishedBatch(properties.batchSize());
        long claimNanos = System.nanoTime() - passStart;
        if (pending.isEmpty()) {
            return 0;
        }
        metrics.recordOutboxClaim(claimNanos);
        phases[0] = claimNanos;

        // Day 24 instrumentation, DEBUG-gated and sharded-claims only. One
        // aggregated record per pass - never per event - carrying who claimed
        // what: which JVM, which shard, and the exact event ids. Nothing else
        // in the system records instance identity, and without it there is no
        // way to tell whether two control planes processed the same shard.
        if (shards > 0 && log.isDebugEnabled()) {
            StringBuilder ids = new StringBuilder(pending.size() * 6);
            for (JobEvent event : pending) {
                if (ids.length() > 0) {
                    ids.append(',');
                }
                ids.append(event.getId());
            }
            log.debug("outbox-claim instance={} shard={}/{} count={} ids={}",
                    INSTANCE_ID, shard, shards, pending.size(), ids);
        }

        long sendStart = System.nanoTime();
        List<Long> published = properties.asyncSends() ? sendBatched(pending) : sendSequentially(pending);
        long sendNanos = System.nanoTime() - sendStart;
        metrics.recordOutboxSend(sendNanos);
        phases[1] = sendNanos;

        if (published.isEmpty()) {
            metrics.recordOutboxPass(System.nanoTime() - passStart);
            return 0;
        }
        long markStart = System.nanoTime();
        jobEventRepository.markPublished(published);
        long markNanos = System.nanoTime() - markStart;
        metrics.recordOutboxMark(markNanos);
        phases[2] = markNanos;
        metrics.outboxPublished(published.size());
        metrics.recordOutboxPass(System.nanoTime() - passStart);
        return published.size();
    }

    /**
     * The original behaviour, and still the default: send one event, wait for
     * its acknowledgement, then send the next.
     * <p>
     * Stopping at the first failure is deliberate. Kafka is likely down, and
     * continuing would just pile up timeouts; it also means the events this
     * pass marks published are always a <em>prefix</em> of the batch in id
     * order, so no event is published while an earlier one in the same batch
     * is not.
     */
    private List<Long> sendSequentially(List<JobEvent> pending) {
        List<Long> published = new ArrayList<>();
        for (JobEvent event : pending) {
            if (!send(event)) {
                break;
            }
            published.add(event.getId());
        }
        return published;
    }

    /**
     * Day 20: submit the whole batch to the producer first, then collect the
     * acknowledgements.
     * <p>
     * The reason is measured, not assumed. Day 19 found that 87-96% of a
     * publisher tick was this loop, and that the per-event cost matched Kafka's
     * own {@code record.queue.time.avg + request.latency.avg} to within 3% -
     * i.e. every event was paying its own {@code linger.ms} wait because the
     * pass blocked before the next record could join a batch. Submitting first
     * lets records accumulate into the same producer batch.
     * <p>
     * <strong>What is preserved:</strong> an event is marked published only if
     * its own send was acknowledged, so a failure still leaves its row
     * {@code published_at IS NULL} for a later pass. Delivery stays
     * at-least-once.
     * <p>
     * <strong>What changes:</strong> the prefix property above is lost. Every
     * event in the batch is attempted, so a later event can be acknowledged and
     * marked published while an earlier one failed and is retried on a
     * subsequent pass. Within a single pass, publication of one aggregate's
     * events can therefore be reordered across a failure - something the
     * sequential path could not do. Note the system already had no global
     * ordering guarantee whenever {@code publisher-concurrency > 1}, since
     * concurrent passes take disjoint batches and race (see Day 15); this
     * narrows the guarantee that remained inside a single pass.
     */
    private List<Long> sendBatched(List<JobEvent> pending) {
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(pending.size());
        List<JobEvent> submitted = new ArrayList<>(pending.size());
        for (JobEvent event : pending) {
            try {
                String topic = topicFor(event.getEventType());
                String message = objectMapper.writeValueAsString(envelope(event));
                futures.add(kafkaTemplate.send(topic, event.getAggregateId(), message));
                submitted.add(event);
            } catch (Exception e) {
                // Serialisation, buffer exhaustion, or a record the producer
                // rejects outright: this event never went in flight, so it is
                // simply left unpublished rather than being waited on.
                metrics.outboxPublishFailed();
                log.warn("Could not submit event {} ({}) to Kafka; it stays unpublished and will be retried: {}",
                        event.getId(), event.getEventType(), e.getMessage());
            }
        }

        // One deadline for the whole batch rather than the full send timeout
        // per event: the sends were all submitted at once, so they complete at
        // roughly the same time, and a per-event timeout would let one pass
        // block for batchSize x sendTimeoutMs in the worst case.
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(properties.sendTimeoutMs());
        List<Long> published = new ArrayList<>(submitted.size());
        for (int i = 0; i < futures.size(); i++) {
            JobEvent event = submitted.get(i);
            try {
                futures.get(i).get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                published.add(event.getId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted publishing event {}; it stays unpublished", event.getId());
                return published;
            } catch (Exception e) {
                metrics.outboxPublishFailed();
                log.warn("Could not publish event {} ({}) to Kafka; it stays unpublished and will be retried: {}",
                        event.getId(), event.getEventType(), e.getMessage());
            }
        }
        return published;
    }

    private boolean send(JobEvent event) {
        try {
            String topic = topicFor(event.getEventType());
            String message = objectMapper.writeValueAsString(envelope(event));
            // Block on the acknowledgement: "published" has to mean Kafka
            // actually took it, not merely that it was buffered.
            kafkaTemplate.send(topic, event.getAggregateId(), message)
                    .get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted publishing event {}; it stays unpublished", event.getId());
            return false;
        } catch (Exception e) {
            metrics.outboxPublishFailed();
            log.warn("Could not publish event {} ({}) to Kafka; it stays unpublished and will be retried: {}",
                    event.getId(), event.getEventType(), e.getMessage());
            return false;
        }
    }

    private String topicFor(JobEventType eventType) {
        return switch (eventType.aggregate()) {
            case JOB -> properties.jobEventsTopic();
            case WORKER -> properties.workerEventsTopic();
        };
    }

    /** The published message. {@code eventId} is stable across republishes, for consumer-side deduplication. */
    private Map<String, Object> envelope(JobEvent event) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", event.getId());
        envelope.put("eventType", event.getEventType().name());
        envelope.put("aggregateId", event.getAggregateId());
        envelope.put("occurredAt", event.getCreatedAt().toString());
        envelope.put("payload", event.getPayload());
        return envelope;
    }
}
