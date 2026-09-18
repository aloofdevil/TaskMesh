# Day 19 — Outbox Publisher Tick Decomposition

Every number is labelled **MEASURED** (read from a meter, a log, or a SQL
query), **CALCULATED** (arithmetic over measured values), **INFERRED** (a
conclusion the measurements support but do not prove), or **HYPOTHESIS**
(untested). Raw artefacts for all runs live outside the repository under the
session scratchpad `results19/<label>/`.

## 1. Objective

Days 15–18 narrowed the outbox publisher's cost to a ~817 ms non-poll
"residual" that grows with concurrency, without ever measuring what is inside
it. The remaining named suspect was the synchronous `future.get()` barrier.

> Does the barrier contribute increasingly to tick duration as `C` grows, and
> does that increase explain a meaningful portion of the tick growth?

This is instrumentation only. Nothing was optimised, `future.get()` was not
removed or replaced, and the publisher remains semantically identical.

## 2. Day 17 Evidence

| Finding | Status |
|---|---|
| C=8 → 650/s, C=12 → 990/s, C=16 → 1,212/s, C=24 → 1,656/s, C=32 → 2,056/s | MEASURED |
| Scaling efficiency fell to 79% at C=32 — DIMINISHING RETURNS | MEASURED |
| Tick period grew 1,232 → 1,556 ms from C=8 to C=32 | CALCULATED (algebraically, from throughput) |
| Mean pass duration appeared flat, so the growth was "somewhere else" | MEASURED, but see §18 |
| Barrier and poll delay named as candidates; neither proven | INFERRED |

## 3. Day 18 Evidence

| Finding | Status |
|---|---|
| Poll interval is a constant additive term: tick ≈ poll + 817 ms | MEASURED |
| 500 → 100 ms poll gave +50.5% throughput; 100 → 50 ms gave nothing | MEASURED |
| The 817 ms residual is where the C-dependence lives | INFERRED |
| `hikaricp.connections.usage` is confounded by empty polls and is not pass duration | MEASURED |

Day 18 fixed the poll interval at 100 ms as the sensible operating point, which
is what Day 19 uses so the poll term is small and constant.

## 4. Hypothesis

- **H-BARRIER** — `future.get()` makes each tick wait for the *slowest* of `C`
  passes. As `C` grows, the maximum of `C` samples drifts further above their
  mean, so the barrier's cost grows with `C` and explains the diminishing
  returns.

Competing explanations were given equal standing and measured, not assumed
away: claim cost, send cost, mark cost, and submission cost.

## 5. Instrumentation Design

Three files changed, all instrumentation. No semantics altered.

**`OutboxPublisherScheduler`** — each submitted task is wrapped so the same
call is bracketed by two `nanoTime()` reads:

```java
futures.add(executor.submit(() -> {
    long passStart = System.nanoTime();
    int n = publisher.publishPending();          // unchanged call
    long passEnd = System.nanoTime();
    long[] phases = publisher.lastPassPhases();
    return new PassResult(n, passStart, passEnd, phases[0], phases[1], phases[2]);
}));
```

The `future.get()` loop is structurally unchanged — same order, same
`InterruptedException` / `ExecutionException` handling. Added around it:
`submitEnd`, a `future.isDone()` scan at barrier entry (read-only, excluded
from the barrier timer), `barrierStart`, and `tickEnd`.

Derived per tick and emitted as **one aggregated DEBUG record per tick — never
one per event**:

| Field | Meaning |
|---|---|
| `tick_us` | whole `publish()` body, excluding the poll delay |
| `submit_us` | submitting `C` tasks to the pool |
| `barrier_us` | wall time inside the `future.get()` loop |
| `done_at_entry` | futures already complete when the barrier is entered |
| `pass_min/mean/max_us` | pass durations across the `C` passes |
| `claim_mean/max_us`, `send_mean/max_us`, `mark_mean_us` | phase timings across passes |
| `straggler_us` | `pass_max − pass_mean` — the cost of waiting for the slowest rather than an average pass |
| `barrier_excess_us` | `tickEnd − last pass end` — barrier time not explained by any pass |

**`OutboxPublisher`** — `publishPending()` brackets its three existing phases
(`lockUnpublishedBatch`, the serial send loop, `markPublished`). Its return
value and control flow are untouched. Phase timings are handed to the scheduler
through a `ThreadLocal<long[]>` read on the same pool thread immediately after
the call, so `publishPending()`'s signature — used by three test classes — did
not change.

**`TaskMeshMetrics`** — four new Micrometer timers (`taskmesh.outbox.claim`,
`.send`, `.mark`, `.pass`). This bean was **already injected** into
`OutboxPublisher`, so no constructor changed anywhere and no test wiring moved.

### The Day 18 lesson, applied

All four timers and all phase records are taken **only for passes that actually
claimed rows**. A saturated drain is followed by many passes that find an empty
outbox and return in microseconds; Day 18 showed that including those collapsed
`hikaricp.connections.usage` from 355 ms to 83 ms purely because faster polling
added empty passes. Timing only working passes keeps these meters comparable
across concurrencies.

### A Day 17 correction

Day 17 reported that `taskmesh.outbox.published` and
`taskmesh.outbox.publish_failures` were "not in the registry". **That was
wrong.** Both counters exist and always did. The scrape asked for statistic
`VALUE`; a Micrometer `Counter` exposes `COUNT`. Read correctly they return
data in every Day 19 run (§17), so Day 17's inability to distinguish "nothing
was lost" from "nothing failed" was a tooling defect, not a missing meter.

## 6. Instrumentation Overhead

A sanity run at C=32 / poll=100 ms was executed before the matrix and compared
with Day 18's uninstrumented `p100` runs at the same C and poll — the only
genuinely comparable pair available (Day 17's C=8 ran at poll=500, so it is not
a valid comparison and is not used as one).

| | Day 18 `p100` (uninstrumented) | Day 19 sanity (instrumented) |
|---|---|---|
| throughput | 3,593/s | 3,299/s |
| non-poll tick time | 791 ms | 879 ms |

That is roughly **8% slower, ~88 ms per tick** (CALCULATED). This is a
**qualitative** overhead check across two days and two builds, not a controlled
A/B, and it is not used to adjust any Day 19 number.

What matters for the conclusion is that **all eight matrix runs carry identical
instrumentation**, so comparisons *within* Day 19 are clean. Day 19 absolute
timings should not be compared to Day 17/18 absolutes without this caveat.

Overhead is also bounded from inside: `submit_us` — a pure-instrumentation
region — measured 0.06–0.26 ms per tick, and `barrier_excess_us` 0.13–0.20 ms,
both negligible against a 697–901 ms tick (MEASURED).

## 7. Experimental Controls

| Control | Value |
|---|---|
| Poll interval | **100 ms** (fixed; Day 18's operating point) |
| HikariCP max pool | 40 |
| Kafka partitions | 3 |
| Kafka producer config (`acks=all`, `linger.ms=5`) | unchanged |
| `future.get()` barrier | **unchanged** |
| Outbox batch size | 100 |
| Logical workers / jobs / ramp / seed | 5000 / 10000 / 500 / 424242 |
| Job load during measurement | **zero** |

Varied: `OUTBOX_PUBLISHER_CONCURRENCY` in {8, 16, 24, 32}, two clean replicates
each, `docker compose down -v` between runs, conditions interleaved.

Asserted per run and aborting on mismatch (MEASURED, all runs passed):
`concurrency_confirmed=1`, `env_concurrency=C`, `env_poll_ms=100`,
`env_hikari=40`, `partitions_actual=3`, `hikaricp.connections.max=40`.

## 8. Measurement Methodology

Saturated-backlog drain (Days 16–18). Phase A builds ~34,500 unpublished events
with the publisher disabled and workers stopped. Phase B force-recreates only
the control plane with the publisher on at concurrency `C`, zero job load, and
drains.

Two measurement channels:

- **Per-tick records** from the scheduler's DEBUG line — the decomposition.
- **Backlog series** sampled every 200 ms on the PostgreSQL clock through a
  single long-lived `psql \watch` session — the throughput.

### Tick selection

Only **saturated** ticks are analysed: those where every pass claimed a full
batch (`events == passes × 100`). End-of-drain ticks have passes that find few
or no rows, which collapses `pass_mean` and inflates `straggler_us` for reasons
that have nothing to do with concurrency. The first 2 saturated ticks are also
dropped as JIT warm-up (Day 18 measured that decline directly).

Retained per run: 41 steady ticks at C=8 down to 8 at C=32 (MEASURED).

All percentiles use nearest-rank on the actual per-tick samples. **No maximum
or percentile in this document is derived from a mean.**

## 9. T0 Anchoring

Day 17's clock started after a readiness poll and two assertion commands, by
which point up to 9,700 events had already drained off the clock. Day 19 keeps
Day 18's fix: the 200 ms sampler starts **before** the readiness wait, so no
part of the drain happens off-camera, and both endpoints are chosen from inside
the sampled series — `t0` = last sample at the backlog peak plus a 3 s guard,
`t1` = last sample with `unpublished > 0`, so the publisher is saturated across
the whole window.

Because `unpublished = total − published` and both are sampled,
`publishes = (total₁ − total₀) + (u₀ − u₁)` is exact even if events are created
inside the window. It was 0 in all eight runs (MEASURED).

Recorded per run: `t0`, `initial_unpublished`, `t1`, `final_unpublished`,
`drained = initial − final`, `throughput = drained / (t1 − t0)`.

Note the decomposition itself is immune to this class of error: it comes from
`nanoTime` deltas inside the publisher, not from backlog arithmetic.

## 10. Environment

Single Docker host, Windows 11, 16 logical CPUs, ~15.7 GiB host RAM, 7.6 GiB to
containers. PostgreSQL 16-alpine, Kafka 4.2.1 single-node KRaft (RF=1),
control plane Spring Boot 4.1.1 / Java 21. Container CPU on Docker's scale
where 1600% is all sixteen cores.

## 11. C = 8

| Replicate | steady ticks | rate | tick | barrier | pass_mean | pass_max | straggler | claim | send | mark |
|---|---|---|---|---|---|---|---|---|---|---|
| r1 | 41 | 989/s | 707 ms | 707 ms | 701 ms | 707 ms | 5.5 ms | 10.6 ms | 675 ms | 6.7 ms |
| r2 | 41 | 1,008/s | 686 ms | 686 ms | 683 ms | 686 ms | 2.5 ms | 7.2 ms | 659 ms | 6.9 ms |
| mean | | **999/s** | **697 ms** | 697 ms | 692 ms | 696 ms | **4.0 ms** | 8.9 ms | 667 ms | 6.8 ms |

`done_at_entry` = 0, `submit` = 0.06 ms, `barrier_excess` = 0.16 ms.

## 12. C = 16

| Replicate | steady ticks | rate | tick | barrier | pass_mean | pass_max | straggler | claim | send | mark |
|---|---|---|---|---|---|---|---|---|---|---|
| r1 | 19 | 1,977/s | 730 ms | 729 ms | 719 ms | 729 ms | 9.3 ms | 15.4 ms | 687 ms | 7.3 ms |
| r2 | 19 | 1,867/s | 742 ms | 742 ms | 728 ms | 741 ms | 12.2 ms | 15.1 ms | 693 ms | 10.3 ms |
| mean | | **1,922/s** | **736 ms** | 736 ms | 724 ms | 735 ms | **10.8 ms** | 15.3 ms | 690 ms | 8.8 ms |

`done_at_entry` = 0, `submit` = 0.11 ms, `barrier_excess` = 0.18 ms.

The first `c16_r2` attempt was **discarded as invalid and repeated** — see §25.

## 13. C = 24

| Replicate | steady ticks | rate | tick | barrier | pass_mean | pass_max | straggler | claim | send | mark |
|---|---|---|---|---|---|---|---|---|---|---|
| r1 | 12 | 2,757/s | 788 ms | 788 ms | 763 ms | 784 ms | 21.7 ms | 25.3 ms | 720 ms | 8.4 ms |
| r2 | 12 | 2,646/s | 816 ms | 816 ms | 792 ms | 812 ms | 20.0 ms | 30.0 ms | 736 ms | 11.3 ms |
| mean | | **2,702/s** | **802 ms** | 802 ms | 777 ms | 798 ms | **20.8 ms** | 27.6 ms | 728 ms | 9.8 ms |

`done_at_entry` = 0, `submit` = 0.15 ms, `barrier_excess` = 0.14 ms.

## 14. C = 32

| Replicate | steady ticks | rate | tick | barrier | pass_mean | pass_max | straggler | claim | send | mark |
|---|---|---|---|---|---|---|---|---|---|---|
| r1 | 8 | 3,028/s | 946 ms | 946 ms | 896 ms | 938 ms | 41.3 ms | 38.8 ms | 828 ms | 9.3 ms |
| r2 | 8 | 3,371/s | 856 ms | 856 ms | 810 ms | 852 ms | 41.3 ms | 39.8 ms | 749 ms | 8.6 ms |
| mean | | **3,199/s** | **901 ms** | 901 ms | 853 ms | 895 ms | **41.3 ms** | 39.3 ms | 789 ms | 9.0 ms |

`done_at_entry` = 0, `submit` = 0.26 ms, `barrier_excess` = 0.19 ms.

## 15. Tick Decomposition

All values MEASURED except the closure columns (CALCULATED):

| C | tick | claim | send | mark | in-pass unexplained | straggler | barrier excess | submit | sum | unexplained |
|---|---|---|---|---|---|---|---|---|---|---|
| 8 | 697 ms | 8.9 | 667.4 | 6.8 | 9.2 | 4.0 | 0.16 | 0.06 | 696.5 | **0.3 (0.0%)** |
| 16 | 736 ms | 15.3 | 690.0 | 8.8 | 9.9 | 10.8 | 0.18 | 0.11 | 735.1 | **0.9 (0.1%)** |
| 24 | 802 ms | 27.6 | 727.8 | 9.8 | 12.1 | 20.8 | 0.14 | 0.15 | 798.4 | **3.4 (0.4%)** |
| 32 | 901 ms | 39.3 | 788.8 | 9.0 | 16.3 | 41.3 | 0.19 | 0.26 | 895.2 | **6.0 (0.7%)** |

"In-pass unexplained" is the part of `pass_mean` not in claim/send/mark —
transaction begin and commit, Hibernate flush, connection acquisition. It is
labelled unexplained rather than attributed, because it was not instrumented.

**The decomposition closes to within 0.7% at worst.** Nothing material is
hiding outside the measured phases.

Share of the tick (CALCULATED):

| C | claim | **send** | mark | in-pass | **barrier (straggler + excess)** | submit |
|---|---|---|---|---|---|---|
| 8 | 1.3% | **95.8%** | 1.0% | 1.3% | **0.6%** | 0.01% |
| 16 | 2.1% | **93.8%** | 1.2% | 1.3% | **1.5%** | 0.01% |
| 24 | 3.4% | **90.8%** | 1.2% | 1.5% | **2.6%** | 0.02% |
| 32 | 4.4% | **87.5%** | 1.0% | 1.8% | **4.6%** | 0.03% |

## 16. Barrier Wait

Two different quantities are both called "barrier wait" and conflating them
would invert the conclusion, so both are reported explicitly.

**(a) Wall time inside the `get()` loop** — `barrier_us` = 697, 736, 802,
901 ms for C = 8…32 (MEASURED). This is ~99.9% of the tick, because the barrier
is the window during which the passes do their work. It is **not** a cost the
barrier imposes: that time is the sends themselves.

**(b) Cost attributable to the barrier** — the time spent waiting *beyond what
an average pass needs*, because the tick ends only when the slowest pass ends:

| C | straggler mean | straggler p95 | straggler max | barrier excess mean | barrier excess max |
|---|---|---|---|---|---|
| 8 | 4.0 ms | 11 ms | 35 ms | 0.160 ms | 0.20 ms |
| 16 | 10.8 ms | 23 ms | 23 ms | 0.179 ms | 0.21 ms |
| 24 | 20.8 ms | 34 ms | 34 ms | 0.135 ms | 0.19 ms |
| 32 | 41.3 ms | 69 ms | 69 ms | 0.192 ms | 0.23 ms |

Tick and barrier tails (MEASURED, nearest-rank over steady ticks):

| C | tick mean | tick p95 | tick max | barrier mean | barrier p95 | barrier max |
|---|---|---|---|---|---|---|
| 8 | 697 ms | 819 ms | 886 ms | 697 ms | 819 ms | 886 ms |
| 16 | 736 ms | 923 ms | 923 ms | 736 ms | 923 ms | 923 ms |
| 24 | 802 ms | 1,059 ms | 1,059 ms | 802 ms | 1,059 ms | 1,059 ms |
| 32 | 901 ms | 1,126 ms | 1,126 ms | 901 ms | 1,126 ms | 1,126 ms |

**Futures already complete at barrier entry: 0, in every steady tick of every
run** (MEASURED). The barrier is never a formality — it always genuinely waits.
That is the strongest available evidence *for* the hypothesis, and it is why
the straggler figures above, not `done_at_entry`, decide the question.

`barrier_excess` — barrier time not explained by any pass — is **flat at
0.13–0.20 ms across a 4× change in C** (MEASURED). Thread hand-off and
scheduling in the barrier cost essentially nothing and do not scale.

## 17. Kafka Completion

MEASURED, end of drain, averaged over replicates:

| C | `record.queue.time.avg` | `request.latency.avg` | `records.per.request` | `request.rate` | `record.send.total` | `record.error.total` |
|---|---|---|---|---|---|---|
| 8 | 5.16 ms | 1.32 ms | 7.8 | 102 /s | 34,652 | **0** |
| 16 | 5.16 ms | 1.62 ms | 15.3 | 38 /s | 34,458 | **0** |
| 24 | 5.14 ms | 2.12 ms | 21.6 | 30 /s | 34,354 | **0** |
| 32 | 5.14 ms | 2.63 ms | 28.6 | 24 /s | 34,806 | **0** |

`taskmesh.outbox.published` matched the drained backlog and
`taskmesh.outbox.publish_failures = 0` in all eight runs (MEASURED — the
counters Day 17 wrongly reported as absent). Broker CPU peaked at 170% of 1600%
and was below 100% in seven of eight runs.

### Why send costs what it costs

Each pass sends its 100 events **serially**, blocking on `.get()` before
sending the next, so each event pays a full `linger.ms` wait plus one request
round-trip. Per-event send cost (CALCULATED as `send_mean / 100`) against
Kafka's own reported `record.queue.time.avg + request.latency.avg`:

| C | measured send/event | Kafka linger + latency | ratio |
|---|---|---|---|
| 8 | 6.67 ms | 6.48 ms | 1.03 |
| 16 | 6.90 ms | 6.78 ms | 1.02 |
| 24 | 7.28 ms | 7.26 ms | 1.00 |
| 32 | 7.89 ms | 7.77 ms | 1.02 |

**Agreement within 3% at every concurrency.** The send phase is fully accounted
for by 100 sequential Kafka round-trips each paying the 5 ms linger floor. The
growth with C is the rising `request.latency.avg` (1.32 → 2.63 ms), multiplied
by 100 events: +131 ms predicted against +121 ms measured (CALCULATED).

`records.per.request.avg` rises with C (7.8 → 28.6) because *other passes'*
events batch together during one pass's linger window — batching happens
across passes, never within one.

## 18. HikariCP

| C | active max | **pending max** | acquire mean | `usage_mean_ms` | usage count |
|---|---|---|---|---|---|
| 8 | 8–9 | **0** | 0.04 ms | 186–241 ms | 1,010–1,356 |
| 16 | 16 | **0** | 0.02 ms | 118–120 ms | 2,262–2,266 |
| 24 | 24–25 | **0** | 0.02 ms | 103–107 ms | 2,794–2,844 |
| 32 | 32 | **0** | 0.03 ms | 86–91 ms | 3,408–4,026 |

`hikaricp.connections.pending` was **0 at every sample of every run**, and
acquire time never exceeded 0.04 ms (MEASURED). The pool is not involved.

`usage_mean_ms` **falls** as C rises, which is the Day 18 confound again — more
concurrency means more empty passes per second, and the count rises from ~1,180
to ~3,700 accordingly. It is **not** pass duration and is not used as such. The
directly measured `pass_mean` (692 → 853 ms, §15) *rises* with C.

This also corrects a Day 17 reading: Day 17 inferred from flat `usage_mean_ms`
that pass duration was flat in C. Day 19's direct measurement shows pass
duration **grows 23% from C=8 to C=32**. The Day 17 inference rested on a
confounded metric.

## 19. PostgreSQL

| C | claim mean | claim max (timer) | CPU max | active sessions max | **lock waits** |
|---|---|---|---|---|---|
| 8 | 8.9 ms | 311 ms | 23% | 1 | **0** |
| 16 | 15.3 ms | 474 ms | 29% | 1 | **0** |
| 24 | 27.6 ms | 303 ms | 42% | 1 | **0** |
| 32 | 39.3 ms | 442 ms | 45% | 1 | **0** |

`wait_event_type = 'Lock'` was **0 at every sample of every run** (MEASURED) —
`SKIP LOCKED` is doing its job; passes skip rather than block.

But **claim cost grows 4.4× from C=8 to C=32** (8.9 → 39.3 ms), the fastest
relative growth of any phase. This is now directly measured rather than
inferred, via the new `taskmesh.outbox.claim` timer. The likely mechanism is
that `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100` must scan past rows locked
by the other C−1 in-flight passes — at C=32 up to ~3,100 of them — before it
can fill a batch. That mechanism is **INFERRED**: no `EXPLAIN ANALYZE` or
buffer-hit measurement was taken to confirm it.

PostgreSQL CPU rose 23% → 45% of 1600%, under half a core throughout.

## 20. Control-Plane CPU

| C | CPU max (of 1600%) | container memory max | heap used | `jvm.threads.live` |
|---|---|---|---|---|
| 8 | 86% | 592 MiB | 216–218 MiB | 38 |
| 16 | 56% | 629 MiB | 263–276 MiB | 46 |
| 24 | 117% | 604 MiB | 231–273 MiB | 54 |
| 32 | 74% | 625 MiB | 263–305 MiB | 62 |

Peak anywhere in the matrix: **117% of 1600%**, under 1.2 of 16 cores
(MEASURED). Threads are exactly `30 + C` in every run. Scheduler activity:
11–44 publishing ticks per run, `done_at_entry` 0 throughout, zero error or
exception lines in the control-plane log in all eight runs.

These are instantaneous `docker stats` samples (3–20 per run depending on drain
length), so they support "nothing approached saturation" and nothing finer.

## 21. Host Metrics

Host CPU averaged **39–61%** of 16 cores across runs, with free physical memory
**0.56–0.99 GiB** of 15.7 GiB total (MEASURED, sampled every third loop pass).
Host memory was tight throughout but did not vary systematically with C, and no
run showed swapping symptoms in its timings. The load generator was **not
running** during any measurement window, by design; `active_workers = 0` in
every valid run confirms it (MEASURED).

## 22. Correctness

Identical across all eight valid runs (MEASURED):

| Check | Result |
|---|---|
| Jobs submitted / completed | 10,000 / 10,000 |
| Jobs left QUEUED / RUNNING / RETRYING / DEAD_LETTER | 0 / 0 / 0 / 0 |
| Duplicate claims | 0 |
| Execution-ID collisions | none — `job_attempts` = distinct `execution_id` in every run |
| Over-budget jobs | 0 |
| Final unpublished backlog | **0** |
| Kafka messages on `taskmesh.job-events` | 30,000 (30,339 in `c32_r1`, which had 113 retries) |
| Duplicate event IDs | **0** |
| `taskmesh.outbox.publish_failures` | **0** |
| `record.error.total` | **0** |
| Control-plane error/exception log lines | **0** |

## 23. Comparative Analysis

Growth from C=8 to C=32 (CALCULATED from the measured phases):

| Component | C=8 | C=32 | delta | share of tick growth |
|---|---|---|---|---|
| **total tick** | 696.8 ms | 901.2 ms | **+204.4 ms** | 100% |
| **send** | 667.4 ms | 788.8 ms | **+121.4 ms** | **59.4%** |
| **straggler (barrier)** | 4.0 ms | 41.3 ms | **+37.3 ms** | **18.2%** |
| **claim** | 8.9 ms | 39.3 ms | **+30.4 ms** | **14.9%** |
| in-pass unexplained | 9.2 ms | 16.3 ms | +7.1 ms | 3.5% |
| mark | 6.8 ms | 9.0 ms | +2.2 ms | 1.1% |
| barrier excess | 0.16 ms | 0.19 ms | +0.03 ms | 0.0% |
| submit | 0.06 ms | 0.26 ms | +0.20 ms | 0.1% |

Answering the required comparisons directly:

- **C vs barrier wait** — straggler rises monotonically 4.0 → 10.8 → 20.8 →
  41.3 ms, close to linear in C (≈1.3 ms per unit of C). **It does increase.**
- **C vs total tick duration** — 697 → 736 → 802 → 901 ms.
- **C vs claim duration** — 8.9 → 15.3 → 27.6 → 39.3 ms; fastest relative
  growth (4.4×).
- **C vs send duration** — 667 → 690 → 728 → 789 ms; largest absolute growth.
- **C vs Kafka latency** — `request.latency.avg` 1.32 → 2.63 ms; queue time
  pinned at 5.1 ms throughout.
- **C vs Hikari pending** — 0 at every C. No relationship.
- **C vs CPU** — no trend; peak 117% of 1600% at C=24, not at C=32.

## 24. Barrier Classification

### **BARRIER NOT SUPPORTED**

Criterion 1 — *does barrier wait increase materially with C?* **Yes.** The
straggler term grows 10.3× (4.0 → 41.3 ms), monotonically, with p95 and max
growing likewise (11 → 69 ms). This is measured, not inferred, and the
hypothesis is right that a barrier exposes growing variance.

Criterion 2 — *does that increase explain a meaningful portion of the
C-dependent tick growth?* **No.** It accounts for **18.2%** of the +204 ms,
against **59.4%** for the serial send loop. And in level terms the entire
barrier cost — straggler plus excess — is **0.6% of the tick at C=8 and 4.6% at
C=32**.

The decisive framing: **eliminating the barrier completely would remove at most
41.5 ms of a 901 ms tick at C=32**, while the send phase is 789 ms. Both
criteria are required for BARRIER SUPPORTED; the second fails.

A further point against attributing even that 41 ms to the barrier: the
straggler is *variance in pass duration*, which the barrier exposes but does not
create. Removing the barrier would decouple ticks, not make the slow passes
fast. Whether the variance would then be hidden or simply shifted is
**untested**.

**What the tick actually is:** at every concurrency measured, 87–96% of the
publisher tick is one pass's serial loop of 100 blocking Kafka sends, each
paying the 5 ms `linger.ms` floor plus a request round-trip — confirmed by
per-event send cost matching Kafka's own reported latency within 3% (§17). The
second-fastest-growing term is the `SKIP LOCKED` claim, at 14.9%. The barrier
is third.

### Day 18 cross-check

Day 18 measured a non-poll residual of ~817 ms at C=32 (and 791 ms specifically
at poll=100 ms). Day 19 measures the same quantity directly as **901 ms**, of
which **895 ms (99.3%) is accounted for** by claim + send + mark + in-pass +
straggler + excess + submit, leaving **6 ms (0.7%) unexplained** and explicitly
labelled as such.

The 901 ms vs 791 ms gap is ~14%, larger than the ~8% overhead estimated in §6.
The difference is **not fully explained**: it may be additional instrumentation
cost, or run-to-run drift between two days on a shared host. It is not
reconciled here and should not be presented as if it were.

So Day 18's residual is now opened up, and the answer is that it was never
mostly barrier — it was mostly Kafka round-trips taken one at a time.

## 25. Limitations

1. **One run was invalid and was repeated.** The first `c16_r2` attempt had its
   Phase A disrupted: submission took 3,128 s at 3 jobs/sec (versus ~18 s at
   ~570/sec) with 61 connection failures, 96 logical workers ended FAILED, and
   95 workers were still ACTIVE during the drain — violating both the
   correctness gate (9,999/10,000 jobs) and the zero-competing-load control. It
   was quarantined, not deleted, and repeated cleanly. Its decomposition
   happened to agree with `c16_r1`, but it is excluded on principle.
2. **Instrumentation costs ~8%** (§6) and the Day 18 cross-check leaves a ~14%
   gap that is not fully reconciled. Day 19 absolutes are not interchangeable
   with Day 17/18 absolutes.
3. **Few ticks at high C.** 41 steady ticks at C=8 but only 8 at C=32, because
   the backlog is capped at ~34,500 by the fixed 10,000 jobs. p95 over 8 samples
   is coarse — which is why the classification rests on means and on the
   consistent monotonic trend across four concurrencies, not on any single
   percentile.
4. **Two replicates per condition**, with C=32 throughput spreading 3,028 vs
   3,371/s (11%). Phase-level means were far more stable than throughput.
5. **The claim-growth mechanism is inferred.** `SKIP LOCKED` scanning past
   locked rows is consistent with the 4.4× growth and zero lock waits, but no
   `EXPLAIN ANALYZE` or buffer-hit data was collected.
6. **"In-pass unexplained" (9–16 ms) was not instrumented** — transaction
   begin/commit, flush and connection acquisition are lumped together.
7. **Percentiles for claim/send/mark come from per-tick aggregates**, not from
   a full per-pass distribution: the actuator exposes only COUNT, TOTAL_TIME and
   MAX for these timers, and the Prometheus endpoint that would carry true
   percentiles is not exposed. Timer MAX values in §19 are true maxima.
8. **Single host, single broker, RF=1.** The 5 ms linger floor that dominates
   §17 is a configuration choice; a replicated broker over real network hops
   would change the send term's composition substantially.
9. **Host memory was tight** (0.56–0.99 GiB free of 15.7 GiB) throughout. No
   run showed timing symptoms of swapping, but this was not controlled.
10. **Only C was varied.** The interaction between concurrency and poll interval
    remains unmeasured (Day 18 limitation 7 still stands).

## 26. Follow-up Experiment

All **HYPOTHESIS** until run. Nothing was optimised in Day 19.

1. **Send asynchronously within a pass — now the clearly indicated next step.**
   Collect the `CompletableFuture`s from `kafkaTemplate.send(...)` and join once
   at the end of the batch, instead of `.get()` per event. §17 shows each event
   currently pays its own 5 ms linger plus round-trip; batching within a pass
   should let 100 events share those waits. This targets 87–96% of the tick,
   where every other candidate targets under 15%. It changes publisher
   semantics — partial-failure handling and the "stop at first failure" ordering
   guarantee both need redesign — so it must be a deliberate, separately tested
   change with its own correctness argument, not a tweak.
2. **Reduce the per-event linger cost without changing the send loop**, as a
   cheap control for (1): sweep `linger.ms` (5 → 1 → 0) at fixed C. If
   per-event send cost tracks it, that independently confirms §17's mechanism
   and bounds how much of the win in (1) comes from batching versus from linger.
3. **Confirm the claim-growth mechanism.** `EXPLAIN (ANALYZE, BUFFERS)` on
   `lockUnpublishedBatch` at C=8 and C=32, to test whether rows scanned grows
   with concurrent passes. Cheap, and it would turn §19's inference into a
   measurement. A partial index on `published_at IS NULL` is the obvious
   candidate fix if confirmed.
4. **Larger backlog (~200,000 events)** so C=32 yields 60+ steady ticks,
   removing limitation 3 before any percentile claim is tightened.
5. **Instrument the in-pass remainder** (transaction begin/commit, flush) to
   close the last 1–2% of §15.
6. **Only after (1):** re-run the Day 17 concurrency sweep to see whether
   removing the send serialisation changes the *shape* of the scaling curve, not
   just its level.
