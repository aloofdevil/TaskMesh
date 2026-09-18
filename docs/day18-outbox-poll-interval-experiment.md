# Day 18 — Outbox Publisher Poll-Interval Experiment

Every number is labelled **MEASURED** (read from a meter, a log, or a SQL
query), **CALCULATED** (arithmetic over measured values), **INFERRED** (a
conclusion the measurements support but do not prove), or **HYPOTHESIS**
(untested). Raw artefacts for all eight runs live outside the repository under
the session scratchpad `results18/<label>/`.

## 1. Objective

Day 17 identified the fixed 500 ms outbox poll delay as the largest single
non-productive term in the publisher's tick, but never tested it. Day 18 asks:

> Does the 500 ms poll interval materially limit publisher throughput at high
> publisher concurrency, and does reducing it produce measurable additional
> drain throughput?

The poll interval is the **only** experimental variable. Publisher concurrency
is pinned at 32 throughout.

## 2. Day 17 Starting Point

| Day 17 finding | Status |
|---|---|
| Throughput rose at every step from C=8 to C=32 (650 → 2,056/s in-window) | MEASURED |
| Scaling efficiency fell to 79% at C=32; classification DIMINISHING RETURNS | MEASURED |
| No saturation observed through C=32 | MEASURED |
| Hikari pending 0 at every sample; CPU, PostgreSQL and Kafka all far from saturation | MEASURED |
| Kafka producer queue time pinned at ~5.1 ms (`linger.ms`) | MEASURED |
| Mean tick period grew 1,232 → 1,556 ms from C=8 to C=32 while mean pass duration stayed flat | CALCULATED |
| The fixed poll delay and the `future.get()` barrier were the two remaining candidates; **neither was proven** | INFERRED |

Day 17 could only calculate the tick period algebraically from throughput. Day
18 measures it directly, which also makes Day 17's calculation checkable.

## 3. Hypotheses

- **H1** — Reducing the poll interval significantly increases throughput.
- **H2** — Reducing the poll interval has little or no effect because the
  publisher pass itself dominates the tick.
- **H3** — Very low poll intervals add overhead and may *reduce* throughput.

None was assumed. All three are evaluated against the data in §21.

## 4. Existing Polling Implementation

No production code was changed. The interval was already a bound property:

```java
@Scheduled(fixedDelayString = "${taskmesh.outbox.poll-interval-ms}")
public void publish() {
    int published = concurrency == 1 ? publisher.publishPending() : publishConcurrently();
    if (published > 0) {
        log.debug("Published {} outbox event(s) to Kafka", published);
    }
}
```

`fixedDelay` (not `fixedRate`) means the delay starts when the previous
invocation *finishes*, so a tick costs `pass duration + poll interval` and the
two terms add rather than overlap. `publishConcurrently()` submits `C` passes
and waits for all of them via `future.get()` — the barrier, left untouched as
a controlled constant.

Two things made this experiment possible without writing any code:

1. `taskmesh.outbox.poll-interval-ms` is already wired to
   `OUTBOX_POLL_INTERVAL_MS`.
2. **The per-tick DEBUG line above already exists**, and it reports exactly how
   many events each tick published. This is precisely the tick instrumentation
   Day 17 listed as missing. Enabling it needs only a logger level, passed
   through the existing `CP_JAVA_OPTS` variable:
   `-Dlogging.level.com.taskmesh.controlplane.service.OutboxPublisherScheduler=DEBUG`.

## 5. Experimental Controls

Held constant across all eight runs and asserted per run (MEASURED):

| Control | Value |
|---|---|
| `publisher-concurrency` | **32** |
| HikariCP maximum pool size | 40 |
| Kafka topic partitions | 3 |
| Outbox batch size | 100 |
| Kafka producer config (`acks=all`, `linger.ms=5`, timeouts) | unchanged |
| `future.get()` barrier | unchanged |
| Logical workers / jobs / ramp / seed | 5000 / 10000 / 500 / 424242 |
| Load-generator duration | 90 s |
| Job load during measurement | **zero** |
| Git commit | `487318f` |

Varied: `OUTBOX_POLL_INTERVAL_MS` in {500, 250, 100, 50}, two clean replicates
each, `docker compose down -v` between every run.

Conditions were run **interleaved** (all `r1` in descending poll order, then
all `r2`) rather than grouped, so slow drift in host state spreads across
conditions instead of loading onto whichever ran last.

## 6. Measurement Methodology

Saturated-backlog drain, per Day 16/17:

**Phase A.** Clean volume, stack up with `OUTBOX_PUBLISHER_ENABLED=false`,
worker containers stopped, 10,000 jobs submitted, load generator for 90 s.
Events accumulate unpublished. Backlogs built were 34,400–35,621 events
(MEASURED).

**Phase B.** Force-recreate only the control plane with the publisher enabled
at C=32 and the poll interval under test, with **zero** job load. Drain the
backlog.

Two **independent** throughput measurements per run:

- **Backlog method.** A single long-lived `psql` session samples
  `select extract(epoch from clock_timestamp()), count(*)-count(published_at), count(*)`
  every 200 ms via `\watch i=0.2`. One session rather than one `docker exec`
  per sample is what makes 200 ms resolution possible — Day 17's per-sample
  exec cost produced 1–3 s gaps and only 9 samples at C=32. Day 18 has 32–75
  samples per window, on the PostgreSQL clock.
- **Tick method.** Sum of the per-tick published counts from the publisher's
  own DEBUG log, over the same window. This touches neither the database
  sampler nor the backlog, so agreement between the two is real corroboration.

### Exact publish count, not a bound

Because `unpublished(t) = total(t) - published(t)` and **both** `total` and
`unpublished` are sampled:

```
publishes(t0..t1) = (total(t1) - total(t0)) + (unpublished(t0) - unpublished(t1))
```

This is exact even if new events are created inside the window. Day 17 could
only argue its rate was a lower bound; here the correction term is computed.
It happened to be **0 in all eight runs** (MEASURED) — no events were created
during any measurement window — but it is now measured rather than assumed.

## 7. T0 Anchoring Method

Day 17's clock started after a readiness poll and two assertion commands, by
which point up to 9,700 events had already drained off the clock — inflating
high-throughput conditions ~3× more than low ones. Day 18 fixes this
structurally:

1. The 200 ms sampler is started **before** the readiness wait, so the entire
   drain is covered and no work happens off-camera.
2. `t0` and `t1` are both chosen *from inside the series*, never from when the
   control script happened to reach a line.
3. `t0` = last sample at the backlog peak, plus a 3 s guard. Anchoring on the
   **last** peak rather than the first sub-peak sample matters because the
   backlog can still rise while Phase-A stragglers land.
4. `t1` = the last sample with `unpublished > 0`, so the publisher is
   backlog-saturated across the whole window.

Both endpoints are measured values on one clock, and throughput is computed
strictly from work completed between them.

### Warm-up

Every run shows a monotonic decline over its first 2–3 ticks — JIT and pool
fill after the Phase-B restart. `p500_r1` began at 2,080 ms and settled near
1,400 ms (MEASURED). Because this warm-up differs between replicates it inflates
replicate spread, so results are reported two ways:

- **RAW** — the whole saturated window, warm-up included.
- **STEADY** — the last 6 ticks of the drain.

STEADY is used for the headline comparison; RAW is shown alongside so the size
of the effect is visible. No condition's conclusion changes between the two.

## 8. Environment

Single Docker host, Windows 11, 16 logical CPUs, 7.6 GiB available to
containers, all containers sharing the host. PostgreSQL 16-alpine, Redis 7,
Kafka 4.2.1 single-node KRaft (RF=1, `min.insync.replicas=1`), control plane on
Spring Boot 4.1.1 / Java 21. Container CPU is on Docker's scale where 1600% is
all sixteen cores.

## 9. 500 ms Results

| Replicate | ticks | RAW tick | STEADY tick | sd | STEADY rate | backlog | window samples |
|---|---|---|---|---|---|---|---|
| r1 | 12 | 1,614 ms | 1,450 ms | 123 | **2,207/s** | 35,621 | 75 |
| r2 | 11 | 1,328 ms | 1,245 ms | 89 | **2,570/s** | 34,501 | 53 |
| mean | | 1,471 ms | **1,348 ms** | | **2,388/s** | | |

Backlog-method rates for the raw window were 1,957/s and 2,535/s; the tick
method gave 2,082/s and 2,463/s — the two independent methods agree within 6%
and 3% (MEASURED).

This is the reference condition: it reproduces Day 17's C=32 point (Day 17
in-window 2,056/s) and, more usefully, **confirms Day 17's calculated tick
period of 1,556 ms with a directly measured 1,328–1,614 ms.** Day 17's
algebraic tick figure was sound.

## 10. 250 ms Results

| Replicate | ticks | RAW tick | STEADY tick | sd | STEADY rate |
|---|---|---|---|---|---|
| r1 | 11 | 1,156 ms | 1,066 ms | 92 | **3,002/s** |
| r2 | 11 | 1,095 ms | 1,012 ms | 61 | **3,162/s** |
| mean | | 1,126 ms | **1,039 ms** | | **3,082/s** |

## 11. 100 ms Results

| Replicate | ticks | RAW tick | STEADY tick | sd | STEADY rate |
|---|---|---|---|---|---|
| r1 | 11 | 1,015 ms | 910 ms | 64 | **3,515/s** |
| r2 | 11 | 952 ms | 872 ms | 71 | **3,670/s** |
| mean | | 984 ms | **891 ms** | | **3,593/s** |

## 12. 50 ms Results

| Replicate | ticks | RAW tick | STEADY tick | sd | STEADY rate |
|---|---|---|---|---|---|
| r1 | 11 | 940 ms | 869 ms | 61 | **3,682/s** |
| r2 | 11 | 968 ms | 910 ms | 83 | **3,517/s** |
| mean | | 954 ms | **889 ms** | | **3,600/s** |

Indistinguishable from 100 ms on every measure.

## 13. Publisher Metrics

| poll | STEADY tick | tick frequency | events/tick | poll share of tick | residual (tick − poll) |
|---|---|---|---|---|---|
| 500 ms | 1,348 ms | 0.74 /s | 3,200 | 37.1% | **848 ms** |
| 250 ms | 1,039 ms | 0.96 /s | 3,200 | 24.1% | **789 ms** |
| 100 ms | 891 ms | 1.12 /s | 3,200 | 11.2% | **791 ms** |
| 50 ms | 889 ms | 1.12 /s | 3,200 | 5.6% | **839 ms** |

Two MEASURED facts carry the whole analysis:

1. **Every tick in every run published exactly 3,200 events** = C × batch-size
   = 32 × 100. The publisher was fully saturated in all eight runs, so tick
   period is the only thing that can change throughput.
2. **The residual is flat at 789–848 ms** (mean 817 ms) with no trend in poll
   interval. The poll interval contributes almost exactly its nominal value to
   the tick and nothing else changes.

That gives a model with measured inputs (CALCULATED):

```
tick ≈ poll + 817 ms        throughput ≈ 3200 / (poll + 817 ms)
```

| poll | predicted | measured | error |
|---|---|---|---|
| 500 ms | 2,430/s | 2,388/s | +1.8% |
| 250 ms | 2,999/s | 3,082/s | −2.7% |
| 100 ms | 3,490/s | 3,593/s | −2.9% |
| 50 ms | 3,691/s | 3,600/s | +2.5% |

The model's asymptote as poll → 0 is `3200 / 0.817 s` = **3,917/s**
(CALCULATED). At 50 ms the publisher already achieves 92% of that, which is why
there is nothing left to win below 100 ms.

Number of polling cycles per run: 11–12 ticks published events in every
condition. Pass duration and barrier wait are **not separately instrumented** —
they are only available as the combined 817 ms residual. Separating them
requires the instrumentation in §24.

## 14. Kafka Metrics

Averaged over replicates (MEASURED):

| poll | `record.queue.time.avg` | `request.latency.avg` | `latency.max` | `records.per.request` | `request.rate` | `record.send.total` | `record.error.total` |
|---|---|---|---|---|---|---|---|
| 500 ms | 5.18 ms | 3.37 ms | 92 ms | 27.1 | 23.7 /s | 34,911 | **0** |
| 250 ms | 5.20 ms | 2.50 ms | 40 ms | 28.5 | 23.6 /s | 34,418 | **0** |
| 100 ms | 5.13 ms | 2.60 ms | 66 ms | 28.7 | 26.1 /s | 34,534 | **0** |
| 50 ms | 5.07 ms | 2.70 ms | 41 ms | 28.1 | 26.4 /s | 34,410 | **0** |

`record.queue.time.avg` stays pinned at ~5.1 ms at every poll interval — that
is `linger.ms=5`, unchanged, confirming the producer remains linger-bound and
that nothing about the poll interval altered broker behaviour. Request latency
does not degrade as throughput rises 51%; `records.per.request` is flat at ~28.
Kafka absorbed a 51% throughput increase without any measurable change.

**Day 17 limitation closed.** Day 17 recorded these `*.total` counters as empty
and noted it could not distinguish "nothing was lost" from "nothing failed".
The cause was the scraping statistic, not the registry: Micrometer files
counters under `COUNT`, not `VALUE`. Reading `COUNT` returns them, and
`record.error.total = 0` in all eight runs is now a **directly measured zero
send failures**, not an inference. `cp_log_error_lines = 0` in all eight runs
corroborates it.

## 15. HikariCP Metrics

| poll | active max | **pending max** | usage_count | `usage_mean_ms` | `acquire_mean_ms` |
|---|---|---|---|---|---|
| 500 ms | 32 | **0** | ~988 | 355 ms | 0.86 ms |
| 250 ms | 32 | **0** | ~1,639 | 195 ms | 0.12 ms |
| 100 ms | 32 | **0** | ~2,314 | 140 ms | 0.03 ms |
| 50 ms | 32 | **0** | ~4,043 | 83 ms | 0.02 ms |

`hikaricp.connections.pending` was **0 at every sample of every run** and
active peaked at exactly 32 against a pool of 40 (MEASURED). The pool is not
involved in this effect at all.

**A measurement trap worth recording.** `usage_mean_ms` appears to fall from
355 ms to 83 ms as the poll interval shrinks, which would suggest passes got
4× faster. They did not. `usage_count` rises from ~988 to ~4,043 over the same
runs, while only 11–12 ticks ever published anything. The extra usages are
passes that ran on an **empty** outbox — before the drain and, mostly, after it
— each taking a connection, finding nothing, and returning it almost instantly.
At 50 ms there are ten times as many of those per second, and since the metric
is a lifetime mean over all usages, they dominate it.

Multiplying back out, total connection-hold time is roughly **constant** across
conditions (~300–405 s; CALCULATED), which is what it should be if the real
work is unchanged. So `usage_mean_ms` is **confounded by the poll interval and
must not be read as pass duration across these conditions.** The tick residual
in §13 is the trustworthy figure.

This does not undermine Day 17, which compared `usage_mean_ms` across C at a
*fixed* 500 ms poll, where the confound is constant. It does mean the two days'
pass-duration figures are not directly comparable.

## 16. PostgreSQL Metrics

- `wait_event_type = 'Lock'` was **0 at every sample of every run** (MEASURED).
- Peak container CPU by condition: 77%, 42%, 36%, 38% of 1600% — under half a
  core at worst (MEASURED).
- `pg_stat_activity` active sessions peaked between 1 and 21 depending on
  sample timing; as in Day 17, connections are frequently held
  idle-in-transaction while blocked on Kafka acknowledgements, so this count
  reads far below the 32 leased connections.
- Outbox claim latency is **not separately instrumented**. `acquire_mean_ms`
  bounds connection acquisition (≤0.86 ms) but says nothing about the
  `SELECT ... FOR UPDATE SKIP LOCKED` itself. Not measured; see §24.

Shortening the poll interval 10× did not produce lock contention or any
measurable database pressure.

## 17. Control-Plane Metrics

| poll | CPU max (of 1600%) | container memory max | `jvm.threads.live` |
|---|---|---|---|
| 500 ms | 95% | 616 MiB | 62 |
| 250 ms | 68% | 604 MiB | 62 |
| 100 ms | 104% | 583 MiB | 62 |
| 50 ms | 99% | 624 MiB | 62 |

Thread count is exactly `30 + C = 62` in every run, independent of poll
interval (MEASURED) — the scheduler does not allocate per-tick threads, so a
10× faster tick does not create threads.

CPU peaked at 104% of 1600% — **under 1.1 of 16 cores**, at 50 ms just as at
500 ms. There is no CPU cost to polling ten times as often at this
concurrency. These are instantaneous `docker stats` samples (3–5 per run), so
they support "nothing approached saturation" and nothing finer.

`process.cpu.usage` again failed to parse in the scraping script (it recorded a
stray `e` from scientific notation). Control-plane CPU above is from
`docker stats`; the actuator gauge is not used.

## 18. Host Metrics

Host CPU was sampled via WMI on every fourth loop pass. Because the probe costs
~1 s it was deliberately throttled to avoid perturbing the measurement, and
because the drain is short this yielded samples only in the 500 ms and 250 ms
conditions, both reading **64%** of 16 cores (MEASURED). The 100 ms and 50 ms
runs completed before the probe fired, so **there is no host CPU sample for
them** — a real gap, not a zero. Container-level CPU (§16, §17) covers all four
conditions and shows no saturation anywhere.

Host memory: 7.602 GiB available to containers; peak container memory was
624 MiB control plane + ~400 MiB Kafka + ~165 MiB PostgreSQL, so memory was
never a constraint (MEASURED). The load generator was **not running** during
any measurement window, by design.

## 19. Correctness

Identical across all eight runs (MEASURED, from SQL and from a Kafka console
consumer reading the topic from the beginning):

| Check | Result |
|---|---|
| Jobs submitted / completed | 10,000 / 10,000 |
| Jobs left QUEUED / RUNNING / RETRYING / DEAD_LETTER | 0 / 0 / 0 / 0 |
| `dup_claims` (same job_id + attempt_number twice) | 0 |
| Execution-ID collisions (`job_attempts` vs distinct `execution_id`) | none — equal in every run |
| `over_budget` (attempt_count > max_attempts) | 0 |
| Final unpublished backlog | **0** |
| Messages on `taskmesh.job-events` | 30,000 (30,018 in `p500_r1`, which had 6 retries) |
| Unique event ids | equal to message count in every run |
| Duplicate event ids | **0** |
| `record.error.total` | **0** |
| Control-plane error/exception log lines | **0** |

No run violated correctness, so no run required separate classification.
Polling ten times as often did not produce duplicate publication, event loss,
or any job-level anomaly — `FOR UPDATE SKIP LOCKED` plus the
`published_at IS NULL` guard hold at 50 ms exactly as at 500 ms.

## 20. Comparative Analysis

STEADY-state, C=32 fixed (MEASURED rates, CALCULATED ratios):

| poll | tick | rate | replicate range | spread | vs 500 ms | marginal gain |
|---|---|---|---|---|---|---|
| 500 ms | 1,348 ms | 2,388/s | 2,207–2,570 | 15.2% | 100% | — |
| 250 ms | 1,039 ms | 3,082/s | 3,002–3,162 | 5.2% | **129%** | +29.0% |
| 100 ms | 891 ms | 3,593/s | 3,515–3,670 | 4.3% | **150%** | +16.6% |
| 50 ms | 889 ms | 3,600/s | 3,517–3,682 | 4.6% | **151%** | +0.2% |

Because replicate spread reaches 15% at 500 ms, differences are judged by
whether the replicate **ranges** overlap rather than by comparing means:

| step | change | ranges | verdict |
|---|---|---|---|
| 500 → 250 ms | +29.0% | [2207–2570] vs [3002–3162] | **DISTINGUISHABLE** — disjoint |
| 250 → 100 ms | +16.6% | [3002–3162] vs [3515–3670] | **DISTINGUISHABLE** — disjoint |
| 100 → 50 ms | +0.2% | [3515–3670] vs [3517–3682] | **NOT DISTINGUISHABLE** — nearly identical |

So the curve rises steeply from 500 ms to 100 ms and then stops flat. The
mechanism is unambiguous from §13: the poll interval is a pure additive term in
the tick, so removing it helps exactly until it is a small fraction of the
tick. At 500 ms it is 37% of the tick; at 50 ms it is 5.6%, and the remaining
817 ms residual sets the ceiling.

## 21. Hypothesis Evaluation

- **H1 — SUPPORTED, in the 500 ms → 100 ms range.** Throughput rose from
  2,388/s to 3,593/s, a **+50.5% increase**, with both intermediate steps
  distinguishable from replicate variance. This is a real, substantial effect,
  and it is the largest single throughput gain measured in TaskMesh since
  Day 15.
- **H2 — SUPPORTED below 100 ms, and it explains where H1 stops.** 100 → 50 ms
  gave +0.2% with overlapping ranges. Once the poll interval is a small
  fraction of the tick, the ~817 ms pass-plus-barrier residual dominates
  completely, exactly as H2 describes.
- **H3 — NOT SUPPORTED.** No throughput penalty at 50 ms (3,600/s, the highest
  measured mean). No rise in control-plane CPU (99% at 50 ms vs 95% at
  500 ms), no change in thread count (62 throughout), no lock waits, no Kafka
  degradation, no errors. The predicted overhead of aggressive polling did not
  appear at this concurrency — though note that a 10× increase in *empty*
  passes is visible in `usage_count` (§15), so the polling work is real; it is
  simply too cheap to matter here.

H1 and H2 are not in conflict: they describe the two ends of the same curve.

## 22. Bottleneck Interpretation

**The 500 ms poll interval does materially limit publisher throughput at C=32.**
It costs roughly a third of every tick and about 50% of achievable throughput.
That is the direct answer to the Day 18 question, and it is MEASURED.

**But the poll interval is not the cause of Day 17's diminishing returns.**
This distinction matters and the two facts are easy to conflate:

- The poll interval is a **constant additive tax**. The residual is flat at
  789–848 ms across a 10× change in poll interval, and every tick published a
  full 3,200 events in every condition (MEASURED).
- Day 17's diminishing returns were a **growth in that residual with
  concurrency** — tick period rose 1,232 → 1,556 ms from C=8 to C=32 while the
  poll interval was fixed at 500 ms. A constant term cannot produce a
  C-dependent slope.

So reducing the poll interval should **raise the whole throughput curve without
changing its shape**. That specific claim is **INFERRED**, not measured: it
combines Day 17's C-sweep at a fixed 500 ms poll with Day 18's poll-sweep at a
fixed C=32. The two-dimensional surface was never measured, and Day 18 cannot
rule out an interaction between the two. §24 proposes the experiment that would
settle it.

The `future.get()` barrier — left deliberately untouched as a controlled
constant — is now the **strongest remaining candidate** for the diminishing
returns, because it is the only identified term that both sits inside the
817 ms residual and scales with `C` (a tick ends only when the slowest of `C`
passes ends, and the maximum of `C` samples drifts above their mean as `C`
grows). That remains INFERRED. Day 18 measured the residual as a single
combined quantity and did not decompose it.

Cumulative bottleneck picture, all measured:

| Candidate | Verdict |
|---|---|
| Hikari pool of 10 (Day 11) | CONFIRMED limiting |
| PostgreSQL (Day 12) | NOT confirmed |
| Control-plane CPU (Day 12) | dominant sensitivity |
| Hibernate TimeZone contention (Day 14) | real but NOT SUPPORTED as a throughput constraint |
| Serial outbox publisher (Day 15) | CONFIRMED |
| Kafka partitions / broker (Day 16) | NOT SUPPORTED |
| Publisher concurrency ceiling (Day 17) | no saturation through C=32; sub-linear |
| **500 ms poll interval (Day 18)** | **CONFIRMED limiting: +50% from 500 → 100 ms** |
| `future.get()` barrier | not yet measured — strongest remaining candidate |

## 23. Limitations

1. **Replicate spread at 500 ms is 15.2%**, driven by uneven JIT warm-up
   (`p500_r1` started at 2,080 ms/tick, `p500_r2` at 1,522 ms). Excluding
   warm-up reduces but does not remove it. The 500 → 250 ms and 250 → 100 ms
   steps survive this because their ranges are disjoint; the 100 → 50 ms null
   result is likewise robust, since those two ranges nearly coincide. Only two
   replicates per condition, so no figure is resolved below ~5%.
2. **Short windows.** Backlog is capped at ~34,500 events by the fixed 10,000
   jobs, so the drain lasts 6–15 s and yields only 11–12 ticks. Day 17's
   limitation 5 (raise the backlog to ~200,000) still stands and now bites
   harder, because faster polling drains faster. The 200 ms sampler and the
   tick log compensate for resolution but cannot add ticks.
3. **Pass duration and barrier wait were not separately measured.** Only their
   817 ms sum is available. The §22 attribution to the barrier is inference.
4. **`hikari.usage_mean_ms` is confounded** by poll interval (§15) and cannot
   serve as pass duration here. This was caught during analysis; it would have
   produced a badly wrong conclusion if taken at face value.
5. **No host CPU samples at 100 ms or 50 ms** (§18) — the probe was throttled
   to avoid perturbing the run and the drains finished first.
6. **`process.cpu.usage` still not captured** (§17), so control-plane CPU rests
   on 3–5 instantaneous `docker stats` samples per run.
7. **One variable, one concurrency.** Poll interval was swept only at C=32.
   Whether the optimal interval depends on `C` is untested.
8. **Enabling DEBUG on the scheduler is a change from Day 17's conditions.** It
   is one log line per tick — 11–12 lines per run — and was applied uniformly
   to all four conditions, so it cannot bias the comparison. It does mean Day 18
   is not byte-identical in configuration to Day 17.
9. **Single host, single broker, RF=1.** With a replicated broker and real
   network hops, pass duration would grow and the poll interval's *share* of
   the tick would fall, which would reduce the benefit measured here.
10. **Outbox claim latency not instrumented** (§16).

## 24. Follow-up Experiments

All are **HYPOTHESIS** until run. No default was changed by this experiment.

1. **Decompose the 817 ms residual — the highest-value next step.** Instrument
   per-pass start/end and per-tick barrier-wait, then measure the *maximum*
   pass duration rather than the mean. This is the one measurement that would
   turn the §22 barrier attribution from INFERRED into MEASURED, and it is
   prerequisite to attacking the residual intelligently.
2. **Two-dimensional sweep: poll interval × concurrency.** C in {8, 16, 32} ×
   poll in {500, 100} would test the §22 inference that the poll interval is a
   constant tax rather than interacting with `C`. This is the specific claim
   Day 18 could not measure.
3. **Send asynchronously within a pass.** `publishPending()` blocks on
   `send().get()` per event, so a 100-event batch pays ~100 sequential
   round-trips each with a ~5 ms linger floor. This attacks the residual
   directly and is likely the largest remaining win, but it changes publisher
   architecture and must be a deliberate, separately-tested change.
4. **Raise the backlog to ~200,000 events** (needs more than 10,000 jobs) so
   drains last 60 s+ and yield 60+ ticks, removing limitation 2 before any
   further precision claims.
5. **Only then consider changing the default.** The data support 100 ms over
   500 ms at C=32 on this hardware, but the default also governs latency and
   idle cost for deployments that are *not* backlog-saturated — the case this
   experiment deliberately excluded by running with zero job load. Empty passes
   rose 4× at 50 ms (§15); harmless here, but not measured under concurrent job
   traffic. A default change needs that evidence first.
6. **Instrument outbox claim latency** so the `FOR UPDATE SKIP LOCKED` cost can
   be separated from connection acquisition.
