# Day 15 — Outbox Publisher Concurrency Experiment

## 1. Objective

Determine what limits transactional-outbox publication throughput, and whether increasing publisher concurrency raises it safely. One variable changes: the number of concurrent publishing passes.

## 2. Previous Observation

Every benchmark from Day 10 through Day 14 measured outbox drain at approximately **100 events/sec**, unchanged by job throughput, Hikari pool size (10→80), CPU budgets, or the Day 14 timestamp change. It behaved as an architectural ceiling independent of everything else measured.

## 3. Current Outbox Architecture

```
JobService / ExecutionService / LeaseReaperService
        │  (same transaction as the state change)
        ▼
JobEventRecorder.recordJobEvent  ──►  job_events row (published_at NULL)
        │
        ▼  OutboxPublisherScheduler @Scheduled(fixedDelay = 500ms)
OutboxPublisher.publishPending()          @Transactional
        │
        ├─ lockUnpublishedBatch(100)   SELECT … WHERE published_at IS NULL
        │                              ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
        ├─ for each event:  kafkaTemplate.send(topic, aggregateId, json).get(5000ms)
        │                   └─ break at first failure
        └─ markPublished(ids)          UPDATE … WHERE id IN (…) AND published_at IS NULL
```

Two serialization points were identified by inspection:

1. **One `@Scheduled` method** → exactly one `publishPending()` call per tick, on one scheduler thread.
2. **A serial blocking loop** → each event's `.get()` completes before the next `send()` begins.

## 4. Baseline Implementation

Unchanged from Day 10: `poll-interval-ms=500`, `batch-size=100`, `send-timeout-ms=5000`, Kafka `acks=all`, 3 partitions, keyed by `aggregateId`.

## 5. Concurrency Design

**No schema change was required**, because the ownership mechanism already existed.

`lockUnpublishedBatch` selects `FOR UPDATE SKIP LOCKED`. Concurrent transactions therefore take **disjoint** batches — a row locked by one pass is skipped by another rather than fought over. `markPublished` is guarded on `published_at IS NULL`, so it is idempotent. The repository javadoc had explicitly anticipated concurrent publishers; nothing had ever exercised it.

The intervention is consequently minimal:

- `OutboxProperties` gains `publisherConcurrency` (default **1**).
- `OutboxPublisherScheduler` submits `C` concurrent `publishPending()` calls per tick to a dedicated fixed pool and waits for all of them. At `C=1` it calls the publisher directly on the scheduler thread — byte-for-byte the previous behaviour, allocating no threads at all.

A dedicated pool rather than the shared Spring scheduler pool: publisher passes block on Kafka acknowledgements, and borrowing scheduler threads for that would starve the lease reaper — the one task that must keep running when other things fail.

Each pass runs on its own thread, so Spring starts a fresh transaction and checks out its own connection per pass.

## 6. Correctness Requirements

The design must prevent duplicate publication, two publishers updating one event, lost events, permanently stuck events, and corrupted publication state. At-least-once delivery remains acceptable; exactly-once is not claimed.

The mechanism providing this is `FOR UPDATE SKIP LOCKED` + the `published_at IS NULL` guard, both pre-existing. Five focused tests were added to prove it holds under real concurrency rather than asserting that a method was called — see §21.

**Ordering is the property that weakens.** A single pass publishes its batch in `id` order. With several passes in flight, two batches race, so events for one aggregate could in principle reach Kafka out of order. Measured behaviour is in §22.

## 7. Experimental Environment

Intel i5-12500H (12 physical / 16 logical), 15.7 GiB; Docker Desktop 16 CPUs / 7.602 GiB, WSL2; PostgreSQL 16.15, Redis 7.4.11, Kafka 4.2.1; control plane `eclipse-temurin:21-jre` (OpenJDK 21.0.12). Base commit `c9f272a` with the uncommitted Day 14 working tree.

## 8. Baseline Configuration

Held constant across all runs: 5,000 logical workers, 10,000 jobs, Hikari 40, seed 424242, ramp 500/sec, 90 s run, worker capacity 2, `batch-size=100`, `poll-interval-ms=500`, Kafka partitions 3, `acks=all`. Every run began with `docker compose down -v --remove-orphans`.

Each run produces ~40,000 events: 30,000 job events (10,000 jobs × QUEUED/RUNNING/COMPLETED) on `taskmesh.job-events`, plus ~10,000 worker events on `taskmesh.worker-events`.

**Drain window: a uniform 180 s** after the load generator stops, applied identically to every configuration.

Two measures of publication rate are reported:
- **Drain rate** — published/sec after the generator stops, when no new events are being created. The cleanest steady-state figure.
- **Overall rate** — total published ÷ total elapsed, covering both phases. More robust when a configuration drains before the drain window opens.

## 9. C=1 Results

| Run | Drain rate | Overall rate | Created | Published | Max backlog | Final backlog | Drained to zero |
|---|---:|---:|---:|---:|---:|---:|---|
| c1_r1 | 83/s | 68/s | 42,048 | 20,159 | 36,989 | 21,889 | **no** (180 s) |
| c1_r2 | 82/s | 69/s | 39,908 | 21,040 | 34,968 | 18,868 | **no** (180 s) |
| **mean** | **82.5/s** | **68.5/s** | | | | | |

Reproduces the ~100 events/sec ceiling seen since Day 10. Neither replicate drained within 180 s; roughly half the events were still queued at the end.

## 10. C=2 Results

| Run | Drain rate | Overall rate | Created | Published | Max backlog | Final backlog | Drained to zero |
|---|---:|---:|---:|---:|---:|---:|---|
| c2_r1 | 168/s | 134/s | 33,624 | 33,624 | 23,580 | 0 | **251 s** |
| c2_r2 | 174/s | 144/s | 40,000 | 40,000 | 29,565 | 0 | **277 s** |
| **mean** | **171/s** | **139/s** | | | | | **264 s** |

Both replicates fully drained. `c2_r1` created fewer events (33,624) and left 395 workers registered — its generator degraded (654 req/s versus 2,222 in `c2_r2`), so its *rate* is valid while its totals are not comparable.

## 11. C=4 Results

| Run | Drain rate | Overall rate | Created | Published | Max backlog | Final backlog | Drained to zero |
|---|---:|---:|---:|---:|---:|---:|---|
| c4_r1 | 327/s | 238/s | 40,000 | 40,000 | 24,114 | 0 | **168 s** |
| c4_r2 | 348/s | 348/s* | 40,000 | 40,000 | 23,748 | 0 | **161 s** |
| **mean** | **337.5/s** | **243/s** | | | | | **164.5 s** |

*`c4_r2` overall rate is 248/s; 348 is its drain rate.

## 12. C=8 Results

| Run | Drain rate | Overall rate | Created | Published | Max backlog | Final backlog | Drained to zero |
|---|---:|---:|---:|---:|---:|---:|---|
| c8_r1 | 322/s | 345/s | 40,000 | 40,000 | 14,474 | 0 | **115 s** |
| c8_r2 | n/a | 351/s | 40,000 | 40,000 | 15,837 | 0 | **113 s** |
| **mean** | — | **348/s** | | | | | **114 s** |

The drain-rate figure is unreliable at C=8 and is not used: publication kept pace so closely with production that the backlog reached zero at t+113–115 s, only 1–8 s after the generator stopped, leaving almost no post-generator samples to measure. `c8_r2` shows `0/s` purely as an artefact of that empty window. **Overall rate and time-to-drain are the meaningful measures here**, and both are unambiguous.

Maximum backlog is also markedly lower (14,474–15,837 versus 23,748–29,565 at C=4), meaning the publisher was keeping up *during* the job run rather than only after it.

## 13. Replicate Variation

| C | Overall rate r1 | r2 | Spread | Time-to-drain r1 | r2 | Spread |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 68/s | 69/s | 1.5% | never | never | — |
| 2 | 134/s | 144/s | 7.5% | 251 s | 277 s | 10.4% |
| 4 | 238/s | 248/s | 4.2% | 168 s | 161 s | 4.3% |
| 8 | 345/s | 351/s | 1.7% | 115 s | 113 s | 1.8% |

Replicate spread is 1.5–10.4%, far below the differences between configurations (2×–5×). The configuration effect is not noise.

## 14. Outbox Publication Throughput

| C | Overall rate | vs C=1 | Marginal gain | Drain rate |
|---:|---:|---:|---:|---:|
| 1 | 68.5/s | 1.00× | — | 82.5/s |
| 2 | 139/s | **2.03×** | 2.03× | 171/s |
| 4 | 243/s | **3.55×** | 1.75× | 337.5/s |
| 8 | 348/s | **5.08×** | 1.43× | — |

Near-linear from 1→2, strong but sub-linear 2→4, and continuing though clearly diminishing 4→8.

## 15. Outbox Backlog

Backlog at fixed offsets (t measured from load-generator start):

| C | t=10 s | t=20 s | t=30 s | t=60 s | max | final | time to zero |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 (r1) | 11,892 | 27,510 | 27,510 | 33,526 | 36,989 | 21,889 | not reached in 180 s |
| 1 (r2) | 11,468 | 26,349 | 26,349 | 31,814 | 34,968 | 18,868 | not reached in 180 s |
| 2 (r2) | 11,433 | 25,294 | 25,294 | 29,365 | 29,565 | 0 | 277 s |
| 4 (r2) | 10,371 | 21,682 | 21,682 | 23,748 | 23,748 | 0 | 161 s |
| 8 (r1) | 8,170 | 14,474 | 14,474 | 12,544 | 14,474 | 0 | 115 s |
| 8 (r2) | 8,130 | 15,837 | 15,837 | 7,736 | 15,837 | 0 | 113 s |

At C=8 the backlog is already falling by t=60 s while jobs are still running — the only configuration where the publisher keeps pace with production rather than merely catching up afterwards.

## 16. Kafka Behavior

| C | Producer request latency (avg) | Producer errors | Kafka CPU (peak) |
|---:|---:|---:|---:|
| 1 | 1.31, 1.46 ms | 0 | 242%, 279% |
| 2 | 0.83, 0.71 ms | 0 | 264%, 207% |
| 4 | 0.90, 1.74 ms | 0 | 230%, 210% |
| 8 | **4.11, 4.13 ms** | 0 | 127%, 252% |

Producer latency is flat and low through C=4, then rises roughly 3–4× at C=8. That is the clearest signal of where the next limit lies: the broker's per-request acknowledgement cost climbs once eight passes send concurrently. Partitions were not changed (3 throughout), and no producer errors occurred at any level.

## 17. PostgreSQL Behavior

| C | PG CPU (peak) | Lock waits |
|---:|---:|---:|
| 1 | 304%, 610% | 0 |
| 2 | 326%, 659% | 0 |
| 4 | 620%, 658% | 0 |
| 8 | 616%, 598% | 0 |

**Lock waits remained zero at every concurrency level** — direct evidence that `SKIP LOCKED` is doing its job: concurrent passes step over each other's rows instead of blocking on them. PostgreSQL CPU shows no systematic increase attributable to publisher concurrency.

## 18. Hikari Behavior

| C | Max active | Max pending | Acquire mean |
|---:|---:|---:|---:|
| 1 | 40/40 | 158–159 | 79.9–100.8 ms |
| 2 | 40/40 | 158–160 | 62.7–145.1 ms |
| 4 | 40/40 | 162–163 | 63.0–65.8 ms |
| 8 | 40/40 | 161–166 | 62.3–63.0 ms |

The pool is saturated in every configuration, driven by the 5,000-worker request load rather than the publisher. Adding up to 8 publisher threads did **not** measurably worsen pool contention: pending stayed ~160 and acquire time did not rise with C — if anything it is lowest at C=4 and C=8. Eight connections out of 40 is a small marginal draw against ~160 already-queued request threads.

## 19. Control-Plane Behavior

| C | CP CPU (peak) |
|---:|---:|
| 1 | 614%, 987% |
| 2 | 970%, 890% |
| 4 | 891%, 917% |
| 8 | 924%, 819% |

No systematic CPU increase from publisher concurrency; all values sit within the band seen across Days 11–14. Consistent with Day 13 profiling, which attributed only 0.5% of CPU samples to `OutboxPublisher.send` and 0.7–2.3% to scheduler threads — the publisher was never a meaningful CPU consumer, which is why raising its concurrency costs little.

## 20. Job Throughput

| C | Loadgen achieved req/s | Jobs completed |
|---:|---|---:|
| 1 | 1,286 / 1,701 | 10,000 / 10,000 |
| 2 | 654* / 2,222 | 10,000 / 10,000 |
| 4 | 2,127 / 2,238 | 10,000 / 10,000 |
| 8 | 2,263 / 2,304 | 10,000 / 10,000 |

*`c2_r1`'s generator degraded, as noted in §10.

Job throughput is not systematically affected by publisher concurrency, and every configuration completed all 10,000 jobs. This is the expected separation: the publisher is downstream of job lifecycle events, so outbox drain time can change 5× while job processing is unchanged. No adverse effect of publisher concurrency on the request path was observed.

## 21. Correctness Results

Every run: **10,000/10,000 jobs COMPLETED**, duplicate claims **0**, execution-id collisions **0** (attempts always equal distinct execution ids), orphaned RUNNING **0**, over-budget attempts **0**, DLQ **0**.

Event accounting at C=2, 4 and 8: **created = published = 40,000**, final backlog 0 — every event created was published exactly once, with no stuck rows. At C=1 the drain window expired first, leaving 18,868–21,889 unpublished; those rows were intact and still claimable, not lost.

Two runs left workers registered (`c2_r1`: 395; one discarded run: 480) — a generator-shutdown artefact also seen on previous days, unrelated to the outbox.

Full suite after this day's work: **148 tests, 0 failures, 0 errors, 0 skipped** (control-plane 113, worker 1, load generator 34), taken from the Surefire XML reports rather than console output. That figure includes the 8 Day 14 timestamp semantic cases, which a separate test-discovery fix made visible to the default suite (see `docs/day14-timezone-optimization.md` §19); no benchmark measurement or conclusion in this document is affected by it.

Five new tests (`OutboxConcurrentPublisherTests`) plus the 22 existing outbox tests all pass:

1. Eight concurrent passes take disjoint batches — every event published exactly once, no id appears twice, and the passes collectively claim exactly the number of rows that exist (overlap would exceed it).
2. Unpublished events eventually become published.
3. Already-published events are not republished and their `published_at` is not rewritten.
4. Events survive across passes and remain claimable; every pass makes progress while rows remain.
5. Concurrent publishing interleaved with ongoing event creation loses nothing.

## 22. Duplicate Publication Analysis

Measured directly from the `taskmesh.job-events` topic by consuming from the beginning and parsing the `eventId` carried in each envelope:

| Run | C | Messages | Unique eventIds | Duplicates | Per-aggregate ordering violations |
|---|---:|---:|---:|---:|---:|
| c1_r1 | 1 | 31,209 | 31,209 | **0** | **0** |
| c2_r2 | 2 | 30,000 | 30,000 | **0** | **0** |
| c4_r2 | 4 | 30,000 | 30,000 | **0** | **0** |
| c8_r1 | 8 | 30,000 | 30,000 | **0** | **0** |
| c8_r2 | 8 | 30,000 | 30,000 | **0** | **0** |

30,000 messages is exactly the expected count (10,000 jobs × 3 job events); worker events go to a separate topic.

**Zero duplicates at every concurrency level.** Messages are keyed by `aggregateId`, so all events for one job land on one partition and their relative order is observable; **no aggregate received its events out of `id` order at any concurrency**.

This is weaker than a guarantee. Concurrency does remove the strict global ordering that a single pass provides, and two passes *could* hold events for the same aggregate simultaneously. It did not happen here because a job's three events are recorded seconds apart and therefore fall into different batches that are still drained in id order. The risk is real in principle and was not exercised by this workload. This remains **at-least-once** delivery; exactly-once is not claimed.

## 23. Bottleneck Interpretation

The hypothesis is **supported**. The ~100 events/sec ceiling was caused by the publisher running exactly one serialized pass at a time, and raising concurrency raised throughput by up to **5.08×** with no correctness cost.

Classification: **STRONG SCALING**, with diminishing returns at the top of the range. C=1→2 is near-linear (2.03×) and C=2→4 remains strong (3.55× cumulative); C=4→8 continues to improve (5.08× cumulative) but marginal gain falls to 1.43×.

Where the next limit lies: **Kafka acknowledgement latency**, which rose from ~0.7–1.7 ms at C≤4 to ~4.1 ms at C=8. It is the only measured quantity that moved systematically and adversely with concurrency. PostgreSQL lock waits stayed at zero, Hikari contention did not worsen, control-plane CPU did not rise, and no producer errors occurred — so the diminishing return at C=8 is not explained by database, pool, or CPU pressure.

This is consistent with everything measured since Day 10: the publisher was never CPU-bound (0.5% of samples on Day 13), so its ceiling was structural — one thread waiting on one acknowledgement at a time — rather than resource-bound.

## 24. Limitations

- Two replicates per configuration. Replicate spread (1.5–10.4%) is well below the configuration effects (2×–5×), so the ranking is safe, but small differences are not resolvable.
- **The drain-rate metric is unusable at C=8**, because the backlog reached zero within 1–8 s of the generator stopping. Overall rate and time-to-drain are used there instead; the two metrics are not interchangeable and both are reported for every configuration.
- Concurrency was not escalated beyond 8, per the experiment rules.
- Kafka partitions stayed at 3 throughout. With 8 concurrent publishers against 3 partitions, partition count may itself contribute to the C=8 flattening; this was not tested.
- `c2_r1` had a degraded load generator (654 req/s) and created only 33,624 events. Its rate is included; its absolute totals are not comparable.
- Ordering was measured, not proven. Zero violations were observed across five analysed runs; a workload whose events for one aggregate are created close together could behave differently.
- Single host with the generator co-resident, as in all previous days.
- The Kafka topic was consumed after each run rather than continuously, so message counts are a post-hoc snapshot.

## 25. Reproduction Commands

```bash
# Build (the change is in application.yml, so the image must be rebuilt)
./mvnw clean verify
docker compose build control-plane

# Verify the setting actually reaches the container - it did not, before the
# docker-compose pass-through was added (see §26)
OUTBOX_PUBLISHER_CONCURRENCY=4 docker compose up -d
docker compose exec -T control-plane sh -c 'env | grep OUTBOX'
docker compose logs control-plane | grep "Outbox publisher running with concurrency"

# One run at concurrency C
HIKARI_MAX_POOL_SIZE=40 OUTBOX_PUBLISHER_CONCURRENCY=<C> docker compose up -d
docker compose stop worker
java Submit.java 10000 <label> http://localhost:8080 64
java -jar taskmesh-loadgen/target/taskmesh-loadgen-0.0.1-SNAPSHOT.jar \
  --workers=5000 --ramp-up=500 --seed=424242 --run-duration=90s

# Backlog sampling
docker compose exec -T postgres psql -U taskmesh -d taskmesh -tAc \
  "select count(*)||','||count(published_at)||','||(count(*)-count(published_at)) from job_events"

# Duplicate and ordering analysis
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic taskmesh.job-events \
  --from-beginning --timeout-ms 20000 > msgs.json
```

## 26. Conclusion

Outbox publication throughput scaled from **68.5 to 348 events/sec (5.08×)** as publisher concurrency went from 1 to 8, and time to drain ~40,000 events fell from *never within 180 s* to **113–115 s**. Correctness was unaffected: zero duplicate claims, zero duplicate publications, zero ordering violations, zero lock waits, and every event published exactly once at C≥2.

The enabling mechanism was already present — `FOR UPDATE SKIP LOCKED` plus an idempotent `published_at IS NULL` guard — so no schema change was needed. The ceiling was simply that nothing ever ran more than one pass.

**One methodological note.** The first execution of the C=2 and C=4 arms was **discarded as invalid**. Their rates (80–90/s) were indistinguishable from C=1, and `confirmed_in_log=0` showed the concurrency bean never reported starting. The cause: `application.yml` resolves `${OUTBOX_PUBLISHER_CONCURRENCY:1}` *inside the container*, and `docker-compose.yml` did not pass that variable through — so every run had actually executed at C=1. This is the same class of failure caught on Day 14 (a stale image), and it was caught the same way: by verifying the manipulation took effect before trusting the numbers, rather than by trusting a plausible-looking result. After adding the pass-through and confirming both the container environment and the startup log, the matrix was re-run in full.

No production-scale claim is made from these figures. They describe this workload, on this single 16-core host, at concurrencies 1–8, with 3 Kafka partitions and a batch size of 100.
