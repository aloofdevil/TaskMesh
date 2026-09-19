# Day 20 — Asynchronous Kafka Sends Within an Outbox Publisher Pass

Every number is labelled **MEASURED** (read from a meter, a log, or a SQL
query), **CALCULATED** (arithmetic over measured values), **INFERRED** (a
conclusion the measurements support but do not prove), or **HYPOTHESIS**
(untested). Raw artefacts live outside the repository under the session
scratchpad `results20/<label>/`.

## 1. Objective

Day 19 decomposed the publisher tick and found the serial Kafka send loop was
87–96% of it. Day 20 tests one narrowly scoped change:

> Does submitting a whole batch to the Kafka producer before collecting
> acknowledgements — instead of awaiting each send in turn — materially improve
> outbox throughput, and at what cost to failure semantics?

This is a production-code experiment, not instrumentation. The change is
confined to the send/wait shape inside the existing pass. Nothing else moved.

## 2. Day 19 Findings

| Finding | Status |
|---|---|
| At C=32: tick ≈ 901 ms, send ≈ 789 ms, claim ≈ 39 ms, straggler ≈ 41 ms, barrier excess ≈ 0.19 ms | MEASURED |
| Send was 87.5–95.8% of the tick at every concurrency | MEASURED |
| Per-event send cost matched Kafka's own `record.queue.time.avg + request.latency.avg` within 3% | MEASURED |
| `future.get()` barrier classified **BARRIER NOT SUPPORTED** — 18.2% of tick growth vs 59.4% for send | MEASURED |
| Each event paid its own `linger.ms` because the pass blocked before the next record could join a batch | INFERRED |

Day 19's closing recommendation was exactly this experiment.

## 3. Existing Publisher Semantics

Read from the code, not assumed. `OutboxPublisher.publishPending()`:

1. **What happens when one Kafka send fails?** `send(event)` catches the
   exception, increments `taskmesh.outbox.publish_failures`, logs a warning and
   returns `false`. The event is not added to the `published` list.
2. **When is `published_at` written?** After the send loop, in
   `markPublished(published)` — a single `UPDATE ... SET published_at = now()
   WHERE id IN (:ids) AND published_at IS NULL`. Only acknowledged ids are in
   that list.
3. **Does it stop on the first failure?** **Yes** — `break`.
4. **Are later events in the batch attempted after a failure?** **No.** They are
   left untouched and stay unpublished.
5. **What transaction boundary exists?** The whole pass is one `@Transactional`
   unit. `lockUnpublishedBatch` takes `FOR UPDATE` row locks that are held for
   the entire pass, *including* all the Kafka I/O, and `markPublished` commits
   with them.
6. **What happens to outbox rows when publishing fails?** Nothing. No exception
   escapes, so the transaction commits normally; the rows simply keep
   `published_at IS NULL` and their locks are released.
7. **How are retries handled?** There is no separate retry path. The next tick's
   `lockUnpublishedBatch` re-selects any row still `published_at IS NULL`,
   ordered by id.
8. **What ordering guarantees exist?** See §5.
9. **Does submit-all-then-wait alter any of these?** Items 1, 2, 5, 6 and 7 are
   unchanged. Items 3 and 4 change, deliberately and visibly — see §5 and §25.

## 4. Existing Failure Behaviour

The sequential path gives a property worth naming, because the change removes
it: **what a pass marks published is always a prefix of its batch in id order.**
No event is ever published while an earlier event in the same batch is not.

That is a consequence of `break`, not a documented guarantee, and it holds only
within a single pass. It is pinned by a new test (§21).

Delivery is **at-least-once** and stays so: an event is marked published only
after Kafka acknowledges it, and a crash between acknowledgement and commit
re-sends it on the next pass. The envelope carries the stable `job_events.id`
as `eventId` for consumer-side deduplication.

## 5. Existing Ordering Behaviour

Determined from the code and tests, not invented:

- **Kafka key** — `kafkaTemplate.send(topic, event.getAggregateId(), message)`.
  Events for one job share a key, so they land on one partition.
  `OutboxTests` asserts this directly: *"keyed by job id for per-job ordering"*.
- **Batch claim order** — `lockUnpublishedBatch` is `ORDER BY id ASC`.
- **Tests** — the only ordering test, `eventsArePublishedInRecordedOrder`,
  asserts the **database rows** are in id order. **No test asserts Kafka
  delivery order.**
- **Existing documentation** — Day 15 already recorded that raising
  `publisher-concurrency` above 1 trades global id-ordering for throughput,
  because concurrent passes take disjoint batches and race.

So of the four candidates, the system promises **(A) ordering only within a
Kafka partition**, via keying. It does not promise (C) ordering of publication
completion, and (D) is too strong a denial because (A) genuinely holds.
(B) holds only as an implementation detail inside one pass.

**What the treatment changes:** all 100 records are handed to the producer
before any acknowledgement is awaited, so multiple records are in flight at
once. Records for one key are still enqueued in id order onto one partition.
Kafka preserves per-partition order for a single producer provided
`enable.idempotence=true` or `max.in.flight.requests.per.connection=1`.

**This was not verified.** The configuration sets `acks=all` and does not set
`enable.idempotence` explicitly; Kafka 4.x clients default it to `true` under
`acks=all`, which would preserve per-partition order at up to 5 in-flight
requests. That is **INFERRED from documented client defaults, not measured**,
and it is the first item in §27. What *is* measured is that no run produced a
duplicate event id (§20) — which shows no duplication, not ordering.

## 6. Optimization Design

```
CONTROL (unchanged default)          TREATMENT
for each event:                      for each event:
    f = send(event)                      f = send(event); keep f
    f.get()                          for each kept f:
                                          f.get()
```

Scope discipline: no new queue, no consumers, no schema change, no change to
job execution, leasing, retries, fencing, workers, Redis, or Kafka partitions.
Only the send/wait shape inside `publishPending()`.

## 7. Control Implementation

The original loop, extracted verbatim into `sendSequentially(List<JobEvent>)`.
Byte-for-byte the same behaviour: send, await, stop at first failure.

## 8. Treatment Implementation

`sendBatched(List<JobEvent>)`, selected by `taskmesh.outbox.async-sends`:

```java
for (JobEvent event : pending) {            // submit all
    futures.add(kafkaTemplate.send(topic, event.getAggregateId(), message));
    submitted.add(event);
}
long deadline = System.nanoTime() + MILLISECONDS.toNanos(properties.sendTimeoutMs());
for (int i = 0; i < futures.size(); i++) {  // then wait all
    try {
        futures.get(i).get(max(0, deadline - System.nanoTime()), NANOSECONDS);
        published.add(submitted.get(i).getId());   // per event, not per batch
    } catch (Exception e) { /* left unpublished, retried next pass */ }
}
```

Three points of care:

- **Per-event success.** `published` gains an id only when *that* future
  resolved. The obvious bug here — marking the batch because the batch was
  submitted — is what §21 tests against.
- **A submit-time failure** (serialisation, buffer exhaustion, a record the
  producer rejects outright) means the event never went in flight, so it is not
  waited on and simply stays unpublished.
- **One deadline for the whole batch**, not `sendTimeoutMs` per event. The sends
  were submitted together so they complete together; a per-event timeout would
  admit a worst case of `batchSize × sendTimeoutMs` in a single pass. A record
  that times out but later succeeds is re-sent on a later pass — at-least-once,
  unchanged.

### Configuration

| Property | Default | Effect |
|---|---|---|
| `taskmesh.outbox.async-sends` | **`false`** | `false` = original sequential send/wait; `true` = submit-all then wait-all |
| Env var | `OUTBOX_ASYNC_SENDS` | compose pass-through, defaults to `false` |

The default preserves existing behaviour exactly, and the switch affects
nothing outside the outbox send/wait path.

## 9. Experimental Controls

| Control | Value |
|---|---|
| Poll interval | 100 ms |
| HikariCP max pool | 40 |
| Kafka partitions | 3 |
| Outbox batch size | 100 |
| Kafka producer config | unchanged |
| Logical workers / jobs / ramp / seed | 5000 / 10000 / 500 / 424242 |
| Job load during measurement | zero |

Varied: `async-sends` ∈ {false, true} × C ∈ {8, 16, 24, 32}.

Asserted per run, aborting on mismatch (MEASURED, every reported run passed):
`concurrency_confirmed=1`, `env_concurrency`, `env_poll_ms=100`,
`env_hikari=40`, `partitions_actual=3`, and **`env_async_sends` matching the
arm** — so no run can silently measure the wrong mode.

Control and treatment were **interleaved at each C** (seq then async, same C,
back to back) so environmental drift lands on both arms.

## 10. Environment

Single Docker host, Windows 11, 16 logical CPUs, ~15.7 GiB RAM, 7.6 GiB to
containers. PostgreSQL 16-alpine, Kafka 4.2.1 single-node KRaft (RF=1), control
plane Spring Boot 4.1.1 / Java 21.

**A significant environmental finding:** a second, unrelated TaskMesh stack was
running throughout — six `k8s_*` containers (control-plane, two workers,
postgres, kafka, redis) up for two days, left from the Day 6 Kubernetes work.
Host free memory was 0.36–1.09 GiB throughout. This load was also present during
Days 17–19, so it is a constant rather than a new variable, but it is the most
likely cause of the two Phase-A stalls described in §26.

## 11. Measurement Methodology

Saturated-backlog drain (Days 16–19). Phase A builds ~34,500 unpublished events
with the publisher disabled and workers stopped; Phase B force-recreates only
the control plane with the publisher on and zero job load.

Two throughput channels:

- **Backlog method** — 200 ms samples on the PostgreSQL clock via one
  long-lived `psql \watch` session, T0/T1 anchored (§12).
- **Tick method** — `C × 100 / (tick_duration + 100 ms poll)`, from the Day 19
  per-tick instrumentation over saturated steady ticks.

The tick method agrees with the backlog method to within 3% on every control
run, where the backlog sampler has 33–163 in-window samples (MEASURED):

| run | backlog | tick-derived | diff |
|---|---|---|---|
| seq_c8_r1 | 988/s | 966/s | 2.2% |
| seq_c8_r2 | 976/s | 977/s | 0.1% |
| seq_c16_r1 | 1,826/s | 1,797/s | 1.6% |
| seq_c16_r2 | 1,898/s | 1,843/s | 2.9% |
| seq_c24_r1 | 2,547/s | 2,560/s | 0.5% |
| seq_c32_r1 | 3,518/s | 3,427/s | 2.6% |

That agreement is what licenses using the tick method where the backlog method
cannot reach: **the treatment drains ~34,500 events so fast that the backlog
sampler runs out of window.** In-window sample counts fall from 163 (seq C=8) to
22, 9, 5 and finally 0 (async C=32), where the entire drain finishes inside the
3 s warm-up guard. Treatment throughput is therefore reported from the tick
method, which still has 8–40 saturated ticks per run.

## 12. T0/T1 Methodology

Unchanged from Days 18–19. The 200 ms sampler starts **before** the readiness
wait so no drain happens off-camera; `t0` = last sample at the backlog peak plus
a 3 s guard; `t1` = last sample with `unpublished > 0`. Both endpoints come from
inside the sampled series, never from when the control script reached a line.
Because `unpublished = total − published` and both are sampled,
`published = (total₁ − total₀) + (u₀ − u₁)` is exact.

Recorded per run (MEASURED, all in §20): `t0`, `initial_unpublished`, `t1`,
`final_unpublished`, `drained`, `published`. No event drained before `t0` is
counted.

## 13. C = 8 Results

| run | ticks | tick | claim | send | mark | straggler | backlog rate | tick rate |
|---|---|---|---|---|---|---|---|---|
| seq r1 | 41 | 728 ms | 9 | 697 | 7 | 5 | 988/s | 966/s |
| seq r2 | 40 | 719 ms | 8 | 689 | 7 | 5 | 976/s | 977/s |
| **async r1** | 40 | **69 ms** | 12 | **23** | 9 | 12 | 4,965/s | 4,747/s |
| **async r2** | 40 | **57 ms** | 8 | **22** | 8 | 8 | 5,258/s | 5,093/s |

Mean: 972/s → **4,920/s = 5.06×**. Tick ratio 0.09, send ratio 0.03.

## 14. C = 16 Results

| run | ticks | tick | claim | send | mark | straggler | backlog rate | tick rate |
|---|---|---|---|---|---|---|---|---|
| seq r1 | 19 | 790 ms | 19 | 728 | 11 | 17 | 1,826/s | 1,797/s |
| seq r2 | 19 | 768 ms | 15 | 716 | 9 | 11 | 1,898/s | 1,843/s |
| **async r1** | 19 | **129 ms** | 26 | **48** | 11 | 27 | 8,026/s | 6,991/s |
| **async r2** | 19 | **88 ms** | 18 | **31** | 9 | 16 | 9,591/s | 8,496/s |

Mean: 1,820/s → **7,743/s = 4.25×**. Tick ratio 0.14, send ratio 0.05.

## 15. C = 24 Results

| run | ticks | tick | claim | send | mark | straggler | backlog rate | tick rate |
|---|---|---|---|---|---|---|---|---|
| seq r1 | 12 | 838 ms | 27 | 755 | 9 | 33 | 2,547/s | 2,560/s |
| **async r1** | 12 | **176 ms** | 40 | **57** | 13 | 45 | 12,247/s* | 8,711/s |

Mean: 2,560/s → **8,711/s = 3.40×**. Tick ratio 0.21, send ratio 0.08.

\* only 5 in-window backlog samples — not reliable; the tick figure is used.

**Single replicate per arm.** The `seq_c24_r2` run was invalidated and its
repeat could not complete (§26).

## 16. C = 32 Results

| run | ticks | tick | claim | send | mark | straggler | backlog rate | tick rate |
|---|---|---|---|---|---|---|---|---|
| seq r1 | 8 | 834 ms | 39 | 736 | 9 | 31 | 3,518/s | 3,427/s |
| **async r1** | 8 | **270 ms** | 58 | **63** | 17 | 85 | unavailable | 8,656/s |

Mean: 3,427/s → **8,656/s = 2.53×**. Tick ratio 0.32, send ratio 0.09.

The backlog method yielded **no window at all** for async C=32 — the drain
completed inside the 3 s guard. Reported as unavailable rather than estimated.

**Single replicate per arm** (§26).

## 17. Kafka Metrics

MEASURED, averaged over replicates:

| mode | C | `record.queue.time.avg` | `request.latency.avg` | `records.per.request.avg` | `request.total` | `record.send.total` | `record.error.total` |
|---|---|---|---|---|---|---|---|
| seq | 8 | 5.19 ms | 1.54 ms | 7.9 | 4,370 | 34,386 | **0** |
| **async** | 8 | **5.04 ms** | **12.36 ms** | **110.0** | **319** | 34,310 | **0** |
| seq | 16 | 5.20 ms | 1.93 ms | 15.1 | 2,288 | 34,398 | **0** |
| **async** | 16 | **14.73 ms** | **17.84 ms** | **116.7** | **302** | 34,486 | **0** |
| seq | 24 | 5.20 ms | 2.65 ms | 21.4 | 1,606 | 34,252 | **0** |
| **async** | 24 | **22.99 ms** | **23.00 ms** | **119.5** | **295** | 34,425 | **0** |
| seq | 32 | 5.12 ms | 2.42 ms | 28.2 | 1,219 | 34,206 | **0** |
| **async** | 32 | **28.27 ms** | **23.14 ms** | **115.7** | **303** | 34,236 | **0** |

### Linger is amortized, not eliminated

The prompt's caution applies directly, and the metrics settle it:
**`record.queue.time.avg` never drops below the 5 ms linger floor in any
treatment run, and it *rises* to 28.27 ms at C=32** — individual records queue
*longer*, not shorter. Linger is not eliminated. What changed is that queueing
now overlaps across records instead of being paid one record at a time.

Two arithmetic checks make that concrete (CALCULATED from measured inputs):

**Control** — send phase ≈ 100 × (queue + latency), i.e. each record pays its
own round trip:

| C | 100 × (queue + latency) | measured send | error |
|---|---|---|---|
| 8 | 673 ms | 693 ms | +3.0% |
| 16 | 713 ms | 722 ms | +1.3% |
| 24 | 785 ms | 755 ms | −3.8% |
| 32 | 754 ms | 736 ms | −2.4% |

**Treatment** — send phase ≈ 1 × (queue + latency), i.e. the batch pays roughly
one record's worth because the rest overlap:

| C | 1 × (queue + latency) | measured send | error |
|---|---|---|---|
| 8 | 17.4 ms | 22 ms | +26% |
| 16 | 32.6 ms | 39 ms | +20% |
| 24 | 46.0 ms | 57 ms | +24% |
| 32 | 51.4 ms | 63 ms | +23% |

Both models fit — the control within 4%, the treatment within ~25% (the excess
being the submit loop and serialisation, which the control amortises differently).
The factor of 100 between them is the whole effect.

Produce requests fell **4.0× to 13.7×** (4,370 → 319 at C=8; 1,219 → 303 at
C=32) while records per request rose to ~110–120. That is above the 100-event
batch size because records from *different concurrent passes* now merge into the
same producer batch.

`record.error.total` was **0 in every run of both arms** (MEASURED).

## 18. Publisher Timing

Phase means over saturated steady ticks (MEASURED, ms):

| mode | C | tick | claim | send | mark | straggler | pass_mean | tick p95 | tick max |
|---|---|---|---|---|---|---|---|---|---|
| seq | 8 | 723 | 9 | 693 | 7 | 5 | 718 | 864 | 915 |
| async | 8 | **63** | 10 | **22** | 9 | 10 | 52 | 138 | 198 |
| seq | 16 | 779 | 17 | 722 | 10 | 14 | 763 | 1,167 | 1,167 |
| async | 16 | **109** | 22 | **39** | 10 | 21 | 83 | 234 | 234 |
| seq | 24 | 838 | 27 | 755 | 9 | 33 | 800 | 1,073 | 1,073 |
| async | 24 | **176** | 40 | **57** | 13 | 45 | 126 | 344 | 344 |
| seq | 32 | 834 | 39 | 736 | 9 | 31 | 800 | 1,054 | 1,054 |
| async | 32 | **270** | 58 | **63** | 17 | 85 | 154 | 366 | 366 |

The tick's composition inverts. In the control, send is 83–93% of the tick. In
the treatment at C=32, send is 63 ms of a 270 ms tick (23%), while **claim
(58 ms) and straggler (85 ms) together exceed it**. The bottleneck Day 19
identified is gone, and the terms Day 19 ranked second and third are now the
largest.

Day 19's control-arm figures (tick 901 ms, send 789 ms at C=32) reproduce here
as 834 ms and 736 ms — 7% lower, consistent with ordinary cross-day drift on a
shared host.

## 19. Resource Metrics

| mode | C | Hikari active | **Hikari pending** | acquire | `usage_mean_ms` | PG lock waits | PG CPU | CP CPU | CP mem | threads |
|---|---|---|---|---|---|---|---|---|---|---|
| seq | 8 | 8 | **0** | 0.05 ms | 233 ms | 0 | 11% | 95% | 612 MiB | 38 |
| seq | 16 | 16 | **0** | 0.03 ms | 144 ms | 0 | 38% | 127% | 627 MiB | 46 |
| seq | 24 | 24 | **0** | 0.03 ms | 94 ms | 0 | 34% | 141% | 615 MiB | 54 |
| seq | 32 | 32 | **0** | 0.04 ms | 89 ms | 0 | 24% | 77% | 562 MiB | 62 |
| async | 8–32 | 0–12 | **0** | 0.01–0.04 ms | 22–24 ms | 0 | — | — | — | 38–62 |

Hikari **pending was 0 at every sample of every run in both arms**, and lock
waits were 0 throughout (MEASURED). `jvm.threads.live` is exactly `30 + C` in
both arms — the treatment allocates no threads.

`usage_mean_ms` is reported but **not interpreted as pass duration**: Days 18
and 19 established it is confounded by empty passes, and that confound is worse
here because the treatment finishes the drain sooner and then polls an empty
outbox for longer.

**Treatment CPU and host figures are largely missing** (shown as —). The
resource probes are throttled to every 2nd/3rd loop pass to avoid perturbing the
measurement, and the treatment's drains are short enough that the probes mostly
fire after the backlog is gone. Reported as missing rather than as zero; see
§26.

## 20. Correctness

**All 12 valid runs passed every gate** (MEASURED, SQL plus a Kafka console
consumer reading from the beginning):

| Check | Result, all 12 runs |
|---|---|
| Jobs submitted / completed | 10,000 / 10,000 |
| Jobs QUEUED / RUNNING / RETRYING / DEAD_LETTER | 0 / 0 / 0 / 0 |
| Duplicate claims | 0 |
| Execution-ID collisions | none (`job_attempts` = distinct `execution_id`) |
| Over-budget jobs | 0 |
| Outbox rows created | 40,000 |
| Final unpublished | **0** |
| Kafka messages | 30,000 |
| Duplicate Kafka event ids | **0** |
| `record.error.total` | 0 |
| CP error/exception log lines | 0 |

T0/T1 anchoring per run, control arm:

| run | window | u₀ | u₁ | drained | published |
|---|---|---|---|---|---|
| seq_c8_r1 | 32.4 s | 32,036 | 36 | 32,000 | 32,000 |
| seq_c8_r2 | 31.2 s | 31,136 | 689 | 30,447 | 30,447 |
| seq_c16_r1 | 15.8 s | 29,736 | 900 | 28,836 | 28,836 |
| seq_c16_r2 | 15.2 s | 29,459 | 600 | 28,859 | 28,859 |
| seq_c24_r1 | 10.4 s | 27,052 | 600 | 26,452 | 26,452 |
| seq_c32_r1 | 6.4 s | 24,606 | 2,094 | 22,512 | 22,512 |

Treatment arm (windows collapse as the drain accelerates):

| run | window | u₀ | u₁ | drained | published |
|---|---|---|---|---|---|
| async_c8_r1 | 4.2 s | 21,561 | 761 | 20,800 | 20,800 |
| async_c8_r2 | 3.8 s | 21,460 | 1,460 | 20,000 | 20,000 |
| async_c16_r1 | 1.6 s | 14,936 | 2,136 | 12,800 | 12,800 |
| async_c16_r2 | 1.0 s | 11,836 | 2,236 | 9,600 | 9,600 |
| async_c24_r1 | 0.8 s | 10,425 | 825 | 9,600 | 9,600 |
| async_c32_r1 | — | — | — | — | no window |

`published` equals `drained` in every row, confirming no events were created
inside any measurement window.

## 21. Failure-Path Validation

Seven new tests, using the repository's existing Testcontainers setup rather
than any new fault-injection framework. The failure injected is a **partial**
one, which `OutboxKafkaOutageTests` cannot produce (it stops the broker, so
everything fails): the broker is healthy and one record exceeds
`max.request.size=2048`, so the producer rejects that record and accepts its
neighbours. **All seven pass** (MEASURED).

`OutboxAsyncSendTests` (5 tests, `async-sends=true`):

| Test | Pins |
|---|---|
| `anEventWhoseSendFailsIsNotMarkedPublished` | the rejected event is not counted or marked published |
| `healthyEventsInTheSameBatchStillPublishAroundAFailedOne` | 2 of 3 published; the event *after* the failure still publishes — the documented semantic change |
| `aFailedEventStaysEligibleAndIsRetriedOnTheNextPass` | the row stays claimable, is re-attempted, and publishes once the cause is removed — with its id unchanged across the failure and the eventual success |
| `anAlreadyPublishedEventIsNeverRevertedByALaterFailureInTheSameBatch` | `published_at` is neither rolled back nor re-stamped |
| `everyEventInAHealthyBatchIsPublishedExactlyOnce` | no double-publication; a second pass has nothing to do |

`OutboxSequentialSendTests` (2 tests, `async-sends=false`) pins the other side:

| Test | Pins |
|---|---|
| `sequentialPublishingStopsAtTheFirstFailure` | only 1 of 3 published — the prefix property |
| `theEventsSkippedAfterAFailureAreNotLost` | skipped events publish on a later pass |

This makes the behavioural difference between the two modes a tested fact rather
than a claim in this document.

## 22. Performance Comparison

| C | control | treatment | speedup | improvement | tick ratio | send ratio | records/request | request count |
|---|---|---|---|---|---|---|---|---|
| 8 | 972/s | 4,920/s | **5.06×** | **+406%** | 0.09 | 0.03 | 7.9 → 110.0 | 4,370 → 319 |
| 16 | 1,820/s | 7,743/s | **4.25×** | **+325%** | 0.14 | 0.05 | 15.1 → 116.7 | 2,288 → 302 |
| 24 | 2,560/s | 8,711/s | **3.40×** | **+240%** | 0.21 | 0.08 | 21.4 → 119.5 | 1,606 → 295 |
| 32 | 3,427/s | 8,656/s | **2.53×** | **+153%** | 0.32 | 0.09 | 28.2 → 115.7 | 1,219 → 303 |

Kafka request latency rose in the treatment (1.54 → 12.36 ms at C=8; 2.42 →
23.14 ms at C=32) — expected, since each request now carries ~4–15× more
records. Request *rate* fell to ~7/s in every treatment run.

Two observations beyond the headline:

- **The speedup shrinks as C rises** (5.06× → 2.53×), because the control
  improves with concurrency while the treatment does not scale as far.
- **The treatment appears to plateau**: 8,711/s at C=24 and 8,656/s at C=32 are
  indistinguishable, whereas the control was still climbing. With one replicate
  at each of those points this is **suggestive, not established** — but it means
  the bottleneck has moved, and §18 shows where: claim and straggler now exceed
  send.

## 23. Statistical / Reproducibility Discussion

Replicate ranges, tick-derived (MEASURED):

| C | control range | treatment range | verdict |
|---|---|---|---|
| 8 | 966–977 | 4,747–5,093 | **disjoint by ~4.9×** |
| 16 | 1,797–1,843 | 6,991–8,496 | **disjoint by ~3.8×** |
| 24 | single replicate each | single replicate each | consistent with trend |
| 32 | single replicate each | single replicate each | consistent with trend |

At C=8 and C=16 — two replicates per arm — the ranges are separated by nearly a
factor of four with no overlap. The widest observed within-arm spread is 19%
(async C=16: 6,991 vs 8,496), an order of magnitude smaller than the 253–506%
effect.

C=24 and C=32 rest on **one replicate per arm**, so reproducibility there is
asserted only by consistency with the replicated levels, not demonstrated. The
effect sizes at those points (3.40× and 2.53×) remain far outside any variance
observed anywhere in the matrix, so the direction and rough magnitude are safe;
a precise value at those two points is not.

Two independent measurement channels agree within 3% on the control arm (§11),
and the phase decomposition independently reproduces both arms' send cost from
Kafka's own metrics within 4% and 25% respectively (§17). The result does not
rest on a single fast run.

## 24. Classification

### **ASYNC SEND SUPPORTED**

Against the four required criteria:

1. **Throughput improves materially** — 2.53× to 5.06× across four
   concurrencies, every one far outside replicate variance. ✔
2. **Publisher tick and send time decrease materially** — tick to 0.09–0.32× of
   control, send phase to 0.03–0.09×. ✔
3. **Correctness remains intact** — all 12 valid runs passed every gate with
   zero duplicates, zero losses, zero errors, and seven new tests pin the
   failure path including a partial failure the previous suite could not
   produce. ✔
4. **Reproducible across replicates** — demonstrated with disjoint ranges at
   C=8 and C=16; at C=24 and C=32 supported by one replicate per arm only. ✔
   with the §23 caveat.

**The careful statement of the cause**, per the required discipline: this
experiment does **not** show "Kafka was the bottleneck". Kafka absorbed 34,000
records in both arms with zero errors, and its per-record queue time got *worse*
in the treatment. What it shows is that **sequential synchronous waiting
prevented the producer from overlapping sends** — each record paid its own
`linger.ms` plus round trip because the pass blocked before the next record
could join a batch. Removing the blocking let ~116 records share a request
instead of ~8–28. The cost per record did not fall; it stopped being paid
serially.

## 25. Semantic Trade-offs

Stated plainly, because performance improving does not excuse hiding them.

**Preserved:** at-least-once delivery; an event marked published only if its own
send was acknowledged; failed events left `published_at IS NULL` and retried by
the existing mechanism; stable `eventId` for deduplication; the transaction
boundary; `FOR UPDATE SKIP LOCKED` batch disjointness; per-partition keying.

**Changed — the prefix property is lost.** The sequential path stops at the
first failure, so what a pass marks published is always a prefix of its batch in
id order. The batched path attempts every event, so event 5 can be published
while event 3 failed and waits for a later pass. Within a pass, publication of
one aggregate's events can therefore be reordered across a failure. This is
pinned by tests on both sides (§21).

**How much this matters:** the lost property held only *within a single pass*,
and the system already had no global ordering guarantee whenever
`publisher-concurrency > 1` — concurrent passes take disjoint batches and race,
as Day 15 documented and as every run here used (C ≥ 8). Per-partition ordering
via `aggregateId` keying is the guarantee the system actually promises, and that
is unchanged *provided* producer idempotence holds (§5 — **unverified**).

**Also changed:** the send timeout is now a single deadline for the batch rather
than per event. Under a slow broker this can time out records that would
eventually have been acknowledged, leaving them to be re-sent — more duplicate
deliveries, never a loss. At-least-once is unchanged.

**Not changed, and worth stating:** the default remains `false`. Nothing about
the shipped behaviour changes unless the switch is set.

## 26. Limitations

1. **Four runs of the planned 16 could not be completed.** `seq_c24_r2` stalled
   in Phase A — submission took 5,831 s at 2 jobs/sec with 280 failures, the
   same pathology as Day 19's invalidated run — and was quarantined. Its repeat
   stalled the same way, and the Docker daemon then became unresponsive to
   container lifecycle operations: `docker compose down -v` reported success
   while leaving containers up for three hours, and every `docker rm -f` timed
   out. The environment needs a Docker Desktop restart, which would also bounce
   the user's Kubernetes pods, so it was not done unilaterally. **Result: C=24
   and C=32 have one replicate per arm instead of two**, and the treatment arm
   has 6 valid runs against the 8 the plan called for.
2. **The leftover Kubernetes stack** (§10) is a standing confound and the most
   likely cause of the stalls. It was present for Days 17–19 as well.
3. **The backlog throughput method breaks down for the treatment.** In-window
   samples fall 163 → 22 → 9 → 5 → 0 as the drain accelerates. Treatment
   throughput therefore comes from the tick method, validated against the
   backlog method only on the control arm.
4. **Treatment resource metrics are mostly missing** (§19) because throttled
   probes miss short drains. Reported as missing, not zero.
5. **Producer idempotence was not verified** (§5), so per-partition ordering
   preservation is inferred from client defaults rather than measured.
6. **Few ticks at high C** — 8 steady ticks at C=32 in both arms, so p95 there
   is coarse.
7. **The treatment plateau at C=24–32 is suggestive only**, resting on one
   replicate per point.
8. **Single host, single broker, RF=1**, and `linger.ms=5` is a configuration
   choice. A replicated broker over real network hops would change the
   absolute numbers, though the 100× structural difference in §17 is not a
   tuning artefact.
9. **Day 19 instrumentation is active in both arms**, costing roughly 8%
   (Day 19 §6). It applies equally to both, so the ratio is unaffected.
10. **The full test suite could not be run to completion.** A targeted run of
    all five outbox test classes passed earlier with BUILD SUCCESS — 34 tests
    including the 7 new ones (§21) — but that was before the daemon wedged. Two
    subsequent attempts at `mvnw clean verify` both timed out, the first hanging
    for 24 minutes starting the Testcontainers Ryuk reaper, the second for
    28 minutes starting a Kafka container (with Ryuk disabled). **The expected
    total of 155 is therefore arithmetic, not a verified measurement**, and the
    suite must be re-run once the environment is repaired.

## 27. Recommended Next Experiment

1. **Verify producer idempotence and per-partition ordering** — the one open
   correctness question. Read the logged `ProducerConfig` for
   `enable.idempotence` and `max.in.flight.requests.per.connection`, then assert
   per-key delivery order on the consumed topic under the treatment at C=32.
   Cheap, and it closes §5. Do this before considering the switch for anything
   but experiments.
2. **Repeat C=24 and C=32 with two replicates** on a clean host (Docker
   restarted, Kubernetes stack stopped), to convert §23's single-replicate
   points and confirm or refute the plateau.
3. **Re-decompose the treatment tick.** §18 shows the composition inverted:
   claim (58 ms) and straggler (85 ms) now exceed send (63 ms) at C=32. The
   Day 19 instrumentation is already in place, so this is a re-analysis of a
   new matrix rather than new code. The `SKIP LOCKED` claim cost — which Day 19
   measured growing 4.4× with C and never explained — is now a first-order term
   and deserves the `EXPLAIN (ANALYZE, BUFFERS)` that Day 19 deferred.
4. **Sweep `linger.ms`** (5 → 1 → 0) under the treatment. §17 shows queue time
   rising to 28 ms at C=32; whether that is linger, in-flight limits, or
   accumulator contention is untested.
5. **Only then consider changing the default.** The performance case is strong,
   but items 1 and 2 are prerequisites, and the prefix-property loss in §25
   should be an explicit, accepted decision rather than a side effect.
