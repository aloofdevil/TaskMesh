# Day 16 — Kafka / Outbox Publisher Bottleneck Investigation

## 1. Objective

Determine experimentally what limits outbox publication throughput after Day 15's concurrency work — specifically whether Kafka acknowledgement latency, topic partition count, control-plane CPU, PostgreSQL, Hikari, or something else is the constraint at `publisher-concurrency = 8`.

This is an investigation. No optimisation was applied.

## 2. Starting Point from Day 15

Day 15 measured publication throughput against publisher concurrency: C=1 ≈ 68.5/s, C=2 ≈ 139/s, C=4 ≈ 243/s, C=8 ≈ 348/s, and noted producer request latency rising from ~0.7–1.7 ms at C≤4 to ~4.1 ms at C=8. That rise was flagged as the *suspected* next limit and explicitly not confirmed.

## 3. Hypotheses

- **H1** Kafka topic partition count limits publication parallelism.
- **H2** Kafka broker throughput/CPU is the constraint.
- **H3** Producer acknowledgement latency is the constraint.
- **H4** Control-plane CPU is the constraint.
- **H5** PostgreSQL / outbox row claiming is the constraint.
- **H6** Hikari connection contention is the constraint.
- **H7** Something else.

## 4. Existing Kafka Architecture

Single-node KRaft broker (`apache/kafka:4.2.1`) in Docker Compose, no ZooKeeper. Two topics, `taskmesh.job-events` and `taskmesh.worker-events`, created by `KafkaTopicsConfig` via Spring's `KafkaAdmin`. One shared `KafkaTemplate`/`KafkaProducer` in the control plane. Publication path is Day 15's: `C` concurrent `publishPending()` passes, each claiming a disjoint 100-row batch with `FOR UPDATE SKIP LOCKED` and sending each event with a blocking `send(...).get(5000ms)`.

## 5. Existing Producer Configuration

Captured from the control plane's own `ProducerConfig values` log line at runtime — **MEASURED**, not read from source:

| Setting | Value | Origin |
|---|---|---|
| `acks` | `-1` (all) | application.yml |
| `enable.idempotence` | `true` | auto-enabled |
| `linger.ms` | **`5`** | Kafka 4.x default |
| `batch.size` | `16384` | default |
| `compression.type` | `none` | default |
| `max.in.flight.requests.per.connection` | `5` | default |
| `retries` | `2147483647` | default |
| `buffer.memory` | `33554432` | default |
| `delivery.timeout.ms` | `10000` | application.yml |
| `request.timeout.ms` | `4000` | application.yml |
| `max.block.ms` | `5000` | application.yml |
| `partitioner.class` | `null` (default) | default |
| `partitioner.ignore.keys` | `false` | default |

Two details matter. `linger.ms` is **5, not 0** — the Kafka 4.x default, easy to assume otherwise. And with replication factor 1 the topics carry `min.insync.replicas=1`, so `acks=all` is satisfied by the leader alone; it is not paying a replication cost here.

## 6. Existing Topic Configuration

Verified with `kafka-topics.sh --describe` against the running broker rather than trusted from configuration:

```
Topic: taskmesh.job-events    PartitionCount: 3  ReplicationFactor: 1  Configs: min.insync.replicas=1
Topic: taskmesh.worker-events PartitionCount: 3  ReplicationFactor: 1  Configs: min.insync.replicas=1
```

## 7. Event Key / Partitioning Behaviour

Events are sent as `kafkaTemplate.send(topic, event.getAggregateId(), message)` — the key is the **aggregate id**, i.e. the job UUID for job events. With `partitioner.ignore.keys=false` and the default partitioner, the key's hash selects the partition.

The workload has 10,000 distinct job UUIDs, so key cardinality vastly exceeds any partition count tested. Distribution was **measured** per run from broker end-offsets:

| Partitions | Messages per partition | Imbalance |
|---:|---|---|
| 1 | 30,000 | n/a |
| 3 | 10,263 / 9,885 / 9,852 | 4.2% |
| 6 | 4,830 – 5,175 | 7.1% |
| 12 | 2,370 – 2,709 | 14.3% |

Distribution is near-uniform at every level. **The keying strategy does not prevent partition parallelism** — so if partition count mattered, this workload would reveal it.

## 8. Experimental Controls

Held constant: 5,000 logical workers, 10,000 jobs, seed 424242, ramp 500/sec, Hikari 40, `publisher-concurrency = 8`, 90 s run, all producer settings, all job/lease/retry/fencing semantics. **Partition count is the only deliberately varied variable.**

To vary it, the hardcoded `private static final int PARTITIONS = 3` in `KafkaTopicsConfig` was replaced with a `taskmesh.outbox.topic-partitions` property **defaulting to 3**, plumbed through `OutboxProperties`, `application.yml` and a `docker-compose.yml` pass-through — the same pattern used for Hikari (Day 11) and publisher concurrency (Day 15). Unset, behaviour is unchanged. Every run asserts the *actual* partition count from `kafka-topics.sh` and aborts on mismatch, and confirms the concurrency bean logged `concurrency 8`.

Each run began with `docker compose down -v --remove-orphans`, so topics were created fresh at the requested partition count (Kafka cannot reduce partitions on an existing topic).

## 9. Environment

Intel i5-12500H (12 physical / 16 logical), 15.7 GiB RAM; Docker Desktop 16 CPUs / 7.602 GiB, WSL2; PostgreSQL 16.15, Redis 7.4.11, Kafka 4.2.1; control plane `eclipse-temurin:21-jre` (OpenJDK 21.0.12); host JDK 21.0.2. Base commit `487318f`.

## 10. Baseline C=8 / P=3

Two replicates under the standard workload: overall publication rate **345/s and 345/s**, max backlog 11,314 / 12,680, drained to zero at 115 s / 116 s, all 40,000 events published, zero duplicates.

**This baseline revealed a problem with the experiment as designed.** The load generator finished at ~113 s and the backlog reached zero at 115–116 s — the publisher was finishing within ~2–3 s of the job load ending, with the backlog tracking production rather than plateauing. That means at C=8 the publisher was **not saturated**: 345/s is the rate at which the workload *produced* events, not the rate at which the publisher was *capable* of publishing them.

A partition effect cannot appear in a component that is not the constraint. Sections 11–14 report the under-load matrix as run; §21 reports a second, saturated experiment built to fix this.

## 11. P=1 Results

| Run | Overall rate | Max backlog | Drained at | Created | Published |
|---|---:|---:|---:|---:|---:|
| p1_r1 | 348/s | 13,303 | 115 s | 39,986 | 39,986 |
| p1_r2 | 348/s | 13,815 | 115 s | 40,000 | 40,000 |

## 12. P=3 Results

| Run | Overall rate | Max backlog | Drained at | Created | Published |
|---|---:|---:|---:|---:|---:|
| p3_r1 | 345/s | 11,314 | 115 s | 40,000 | 40,000 |
| p3_r2 | 345/s | 12,680 | 116 s | 39,998 | 39,998 |

## 13. P=6 Results

| Run | Overall rate | Max backlog | Drained at | Created | Published |
|---|---:|---:|---:|---:|---:|
| p6_r1 | 345/s | 13,869 | 116 s | 39,998 | 39,998 |
| p6_r2 | 348/s | 14,059 | 115 s | 40,000 | 40,000 |

## 14. P=12 Results

| Run | Overall rate | Max backlog | Drained at | Created | Published |
|---|---:|---:|---:|---:|---:|
| p12_r1 | 342/s | 14,726 | 117 s | 40,000 | 40,000 |
| p12_r2 | 343/s | 12,884 | 116 s | 39,774 | 39,774 |

**Under load, throughput across a 12× change in partition count spans 342–348/s — a 1.8% range, smaller than run-to-run variance.** Drain time spans 115–117 s.

## 15. Kafka Metrics

Under load (drain-phase means; see §23 for a sampling caveat):

| P | Record queue time | Request latency | Batch size | Records/request | Request rate | Broker CPU | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6.69 / 6.68 ms | 4.15 / 4.67 ms | 2550 / 2359 B | 6.17 / 6.06 | 55.3 / 60.3 /s | 246% / 125% | 0 |
| 3 | 6.03 / 6.02 ms | 4.13 / 4.10 ms | 1038 / 1021 B | 6.23 / 6.17 | 56.9 / 56.7 /s | 117% / 115% | 0 |
| 6 | 5.92 / 5.77 ms | 4.36 / 4.36 ms | 709 / 707 B | 6.05 / 6.07 | 60.0 / 60.2 /s | 158% / 194% | 0 |
| 12 | 5.72 / 5.69 ms | 5.22 / 4.96 ms | 581 / 578 B | 5.86 / 5.88 | 59.6 / 62.4 /s | 117% / 174% | 0 |

Batch size falls as partitions rise (2550 B → 578 B) because records spread across more accumulator batches — the expected mechanical consequence. Records-per-request stays ~6 and request rate stays ~55–62/s regardless. Broker CPU never exceeded 246% of the 1600% available. Zero producer errors at every level.

## 16. PostgreSQL Metrics

Under load, peak CPU 357–514%, consistent with Days 11–15 and with no trend across partition count. In the saturated experiment (§21) — where the only work is outbox publication — PostgreSQL CPU fell to **5.8–27%**, showing the outbox's own database work is a small fraction of the control plane's total database load.

## 17. Control-Plane Metrics

Under load, peak CPU 499–692%, no trend across partition count. In the saturated experiment, with no job traffic, control-plane CPU was **51–91%** (about 0.5–0.9 of one core) while publishing at ~730 events/s — the publisher is not CPU-bound.

## 18. Hikari Metrics

Under load: max active 40/40, max pending 163–168, acquire mean 68–75 ms — unchanged across partition counts and identical to Days 11–15. This contention is produced by the 5,000-worker request load, not by the publisher.

In the saturated experiment, with no job load, **Hikari acquire fell to 0.3–0.4 ms**. The publisher's own eight connections place no meaningful demand on a pool of 40.

## 19. Load-Generator Metrics

Achieved 1,869–2,088 req/s across runs; probe wake-up p99 22.5–38.4 ms against the 100 ms saturation threshold; **`GENERATOR-SATURATED` never triggered in any run**. The generator was not a limiting factor.

## 20. Correctness

All 14 runs (8 under-load + 6 saturated): **10,000/10,000 jobs COMPLETED**, duplicate claims **0**, execution-id collisions **0**, orphaned RUNNING **0**, over-budget **0**, dead-lettered **0**, workers left registered **0**. Every event created was published: final backlog **0** in every run.

Kafka-side: **30,000 messages, 30,000 unique event ids, 0 duplicates** at every partition count including P=12 (`p12_r2` showed 30,039/30,039 — a few extra events from 13 lease reassignments in that run, still zero duplicates).

## 21. Comparative Analysis

### The under-load matrix cannot answer the question

At C=8 the publisher keeps pace with production and drains 2–3 s after the job load stops. Its measured 345/s is therefore a **lower bound** on capacity, set by the workload, not by the publisher.

### Saturated-drain experiment

To measure capacity rather than production, a second experiment was run: build a full backlog with `publisher-enabled=false`, then restart only the control plane with the publisher on at C=8 and **zero job load**, and measure the pure drain. Same seed, workers, jobs and concurrency; partition count still the only variable.

| P | Backlog | Drain time | **Capacity** | Queue time | Request latency | Batch size | Records/req | Broker CPU | CP CPU | PG CPU | Hikari acquire |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 34,603 | 48 s | **721/s** | 5.22 ms | 1.13 ms | 2595 B | 7.79 | 251% | 91% | 7% | 0.3 ms |
| 1 | 34,300 | 46 s | **746/s** | 5.20 ms | 1.08 ms | 2566 B | 7.93 | 51% | 56% | 27% | 0.3 ms |
| 3 | 34,350 | 49 s | **701/s** | 5.14 ms | 1.17 ms | 1009 B | 7.94 | 253% | 55% | 18% | 0.3 ms |
| 3 | 34,261 | 47 s | **729/s** | 5.13 ms | 1.15 ms | 1017 B | 7.92 | 241% | 51% | 6% | 0.3 ms |
| 12 | 34,400 | 46 s | **748/s** | 5.11 ms | 1.21 ms | 562 B | 7.94 | 231% | 74% | 6% | 0.4 ms |
| 12 | 34,436 | 47 s | **733/s** | 5.11 ms | 1.32 ms | 563 B | 7.84 | 249% | 89% | 25% | 0.4 ms |

Means: **P=1 → 733/s, P=3 → 715/s, P=12 → 740/s.** Spread across a 12× partition change is 3.6%; within-condition replicate spread reaches 3.5%. **Not distinguishable.**

Two things follow. First, true publication capacity is **~700–750 events/s**, roughly double the 345/s observed under load — confirming the publisher was production-limited, not capacity-limited, in every Day 15 and Day 16 under-load run. Second, partition count does not affect it even when the publisher *is* saturated.

### Where the time actually goes

At ~733/s with 8 concurrent passes, the per-event budget per pass is `8 / 733 ≈ 10.9 ms`. Decomposing from measured producer metrics:

| Component | Measured | Share of 10.9 ms |
|---|---:|---:|
| Producer accumulator wait (`record.queue.time.avg`) | **5.1 ms** | 47% |
| Broker round trip (`request.latency.avg`) | **1.1 ms** | 10% |
| Everything else (JSON, DB batch claim/update amortised, thread handoff) | ~4.7 ms | 43% |

**CALCULATED.** The striking part: the broker answers in ~1.1 ms, while records wait ~5.1 ms in the producer's accumulator — almost exactly `linger.ms = 5`. Records-per-request is ~7.9, matching the 8 concurrent senders: the eight threads' records batch together, wait out the linger window, go in one request, and all eight unblock together.

So the Kafka-side cost is dominated by **batching delay the publisher's own send-and-wait pattern makes unavoidable**, not by broker service time.

## 22. Bottleneck Classification

**H1 — partition count: NOT SUPPORTED.** Throughput is flat within 1.8% under load across P=1→12, and within 3.6% at saturation. Key distribution is near-uniform, so the test was capable of detecting an effect.

**H2 — broker throughput/CPU: NOT SUPPORTED.** Broker CPU peaked at 253% of 1600% available, and one 746/s run recorded only 51%. Zero producer errors throughout.

**H3 — acknowledgement latency: NOT SUPPORTED as stated.** Broker round-trip is 1.1–1.3 ms at saturation, ~10% of the per-event budget. Day 15's observation that request latency rose to ~4.1 ms at C=8 reproduces here **under load** (4.1–5.2 ms), but at saturation with no competing job traffic it falls to ~1.1 ms — so that rise reflects host contention during the job workload, not a broker limit.

**H4 — control-plane CPU: NOT SUPPORTED for the publisher.** 51–91% (under one core) while publishing 730/s.

**H5 — PostgreSQL / row claiming: NOT SUPPORTED.** 6–27% CPU at saturation.

**H6 — Hikari contention: NOT SUPPORTED for the publisher.** Acquire 0.3–0.4 ms with no job load; the 68–75 ms seen under load is caused by the request workload.

**H7 — something else: SUPPORTED.** The limit is the **interaction between the synchronous send-and-wait publisher and producer batching latency**. Each pass blocks on `.get()` per record, so with C passes the achievable rate is bounded by roughly `C / (linger + round-trip)`. With C=8 and ~6.3 ms of measured Kafka path, that predicts ~1,270/s; measured 733/s, with the gap accounted for by the ~4.7 ms of non-Kafka per-event work. This is a **structural property of the publisher, not a capacity limit of any component**.

Classification summary — **MEASURED**: all throughput, latency, CPU, partition-distribution and correctness figures. **CALCULATED**: per-event budget decomposition and the `C / (linger + RTT)` bound. **INFERRED**: that the ~4.7 ms residual is JSON/DB/thread-handoff — its components were not individually profiled. **HYPOTHESIS**: that raising `C`, reducing `linger.ms`, or making sends asynchronous would raise capacity — none were tested.

## 23. Limitations

- **The under-load matrix (§§10–14) is inconclusive by construction**: the publisher was not saturated, so a partition effect could not have appeared. It is reported in full rather than discarded, because it is what shows the publisher now keeps pace with production.
- The under-load sampler was too slow (~15 s per sample), yielding only ~8 samples per 115 s run and just **2 post-loadgen samples**. The §15 Kafka means rest on those two samples per run; they are consistent across replicates but thinly supported. The saturated experiment used a faster single-query sampler.
- Two replicates per condition; differences under ~4% are not resolvable.
- The saturated experiment restarts only the control plane between phases, so its JVM is freshly started and cold-ish relative to a long-running one.
- Single-node broker, replication factor 1, `min.insync.replicas=1` — `acks=all` costs one local write. Conclusions about acknowledgement cost do not transfer to a replicated cluster.
- Single host with the generator co-resident, as in all previous days.
- The ~4.7 ms non-Kafka residual was derived by subtraction, not profiled directly.
- `record.queue.time.avg` and `request.latency.avg` are producer-reported averages over a 30 s metrics window, not per-record measurements.

## 24. Follow-up Experiment Candidates

Listed in order of how directly the evidence points at them. None were performed; each would be its own controlled experiment.

1. **Publisher concurrency beyond 8.** Capacity tracks `C / (linger + RTT)`, and no resource is near saturation at C=8 — broker 15% of host CPU, control plane under one core, PostgreSQL under 30%, Hikari 0.4 ms. C=16/32 would test whether the relationship continues.
2. **`linger.ms`.** The single largest measured component (5.1 ms, 47% of the per-event budget) is accumulator wait. Lowering it trades batching efficiency for latency, and with a blocking publisher the batching may be buying little.
3. **Asynchronous publication.** Removing the per-record `.get()` in favour of collecting futures would decouple throughput from `C × (linger + RTT)` entirely. This is the largest change and needs careful treatment of the ordering and at-least-once properties Day 15 documented.
4. **Profiling the 4.7 ms residual** with JFR, to attribute it between JSON serialisation, the batch claim/update transaction, and thread handoff.

**No optimisation is justified by this investigation on its own.** The measurements identify a structural property and rule out five candidate resources; choosing among the follow-ups above is a separate decision.
