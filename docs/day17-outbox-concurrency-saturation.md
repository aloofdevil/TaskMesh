# Day 17 — Outbox Publisher Concurrency Saturation Experiment

Every number in this document is labelled **MEASURED** (read from a meter, a
log, or a SQL query), **CALCULATED** (arithmetic over measured values),
**INFERRED** (a conclusion the measurements support but do not prove), or
**HYPOTHESIS** (untested). Raw artefacts for all ten runs are outside the
repository, under the session scratchpad `results17/<label>/`.

## 1. Objective

Day 15 stopped at publisher concurrency `C = 8` because that was the largest
value tested, not because anything had been shown to saturate. Day 17 asks a
single experimental question:

> As `C` increases beyond 8, where does the synchronous outbox publisher stop
> converting added concurrency into added drain throughput?

`C` is the **only** experimental variable. This is an investigation, not an
optimisation: no default is changed, and no code is modified to make a number
look better.

## 2. Starting Point from Day 15 and Day 16

| Day | Finding | Status |
|---|---|---|
| 15 | Serial outbox publisher is a real throughput constraint; C=1 to 8 gave 5.08x | CONFIRMED |
| 16 | Kafka partition count (P=1 to 12) does not move outbox drain throughput; broker CPU peaked at 253% of 1600% | NOT SUPPORTED as a bottleneck |
| 16 | Under-load measurement is invalid for capacity: the publisher kept pace with production, so the observed rate was the *production* rate | methodology correction |
| 16 | Saturated-drain methodology measures true capacity: ~700-750/s at C=8, P=3 | MEASURED |

Day 16 therefore left the question "what *is* limiting the publisher?" open,
having removed Kafka partitioning from the candidate list. Day 17 resumes from
the saturated-drain methodology Day 16 established.

## 3. Hypotheses

- **H1** — Throughput continues to rise roughly linearly past C=8; C=8 was
  simply the edge of the Day 15 grid. *(Prior: likely, since nothing measured
  on Day 16 was near saturation.)*
- **H2** — HikariCP becomes the binding constraint as C approaches the pool
  size, visible as non-zero `hikaricp.connections.pending`.
- **H3** — Kafka producer latency rises with C as more in-flight requests queue
  at the broker, flattening the curve.
- **H4** — Control-plane CPU saturates first.

## 4. Code Under Test — No Day 17 Changes

**No production code was written for Day 17.** `taskmesh.outbox.publisher-concurrency`
already exists, committed on Day 15 as `487318f`. Every run in this document was
executed against that commit; `git rev-parse --short HEAD` was recorded in each
`run.meta` and reads `487318f` for all ten runs (MEASURED).

The working tree does carry uncommitted **Day 16** changes (the topic-partition
pass-through). Those were held at their default of 3 partitions throughout, so
they are inert with respect to this experiment — and the effective partition
count was asserted per run rather than assumed (§9).

## 5. How Publisher Concurrency Works

`OutboxPublisherScheduler` runs on a fixed delay of `poll-interval-ms = 500`. At
`C = 1` it calls the publisher inline. Above 1 it submits `C` independent
`publishPending()` calls to a dedicated fixed pool and then **waits for all of
them** before the tick can complete:

```java
for (int i = 0; i < concurrency; i++) futures.add(executor.submit(publisher::publishPending));
for (Future<Integer> future : futures) total += future.get();   // barrier
```

Each `publishPending()` is one transaction that claims up to `batch-size = 100`
rows with `FOR UPDATE SKIP LOCKED`, then sends them to Kafka **serially**, each
`send().get()` blocking on the broker acknowledgement, then marks them
published. `SKIP LOCKED` is what makes the `C` passes take disjoint batches
without a schema change.

Two structural consequences matter for reading the results below, and both are
properties of the code, not of the measurements:

1. A tick cannot finish until the **slowest** of the `C` passes finishes (a
   barrier).
2. After the tick finishes, a **fixed 500 ms delay** elapses during which
   nothing is published.

## 6. Experimental Controls

Held constant across all ten runs, and recorded in every `run.meta` (MEASURED):

| Control | Value |
|---|---|
| Kafka topic partitions | 3 (asserted per run via `kafka-topics.sh --describe`) |
| HikariCP maximum pool size | 40 |
| Outbox batch size | 100 |
| Outbox poll interval | 500 ms |
| Kafka `acks` | `all`, RF=1, `min.insync.replicas=1` |
| Producer `linger.ms` | 5 (Kafka default) |
| Load-generator seed | 424242 |
| Logical workers / jobs / ramp | 5000 / 10000 / 500 |
| Job load during measurement | **zero** |
| Git commit | `487318f` |

Varied: `OUTBOX_PUBLISHER_CONCURRENCY` in {8, 12, 16, 24, 32}, two clean
replicates each.

## 7. Environment

Single Docker host, Windows 11, 16 logical CPUs, all containers sharing it.
PostgreSQL 16-alpine, Redis 7, Kafka 4.2.1 single-node KRaft, control plane on
Spring Boot 4.1.1 / Java 21. Container CPU percentages below are on Docker's
scale, where 1600% is all sixteen cores.

## 8. Methodology

Per run, adapted from Day 16's saturated-drain design:

**Phase A — build a static backlog.** `docker compose down -v` (clean volume),
bring the stack up with `OUTBOX_PUBLISHER_ENABLED=false`, stop the worker
containers, submit 10,000 jobs, run the load generator for 90 s. Events
accumulate unpublished. Snapshot the backlog with
`select count(*)-count(published_at) from job_events`.

**Phase B — drain it.** Force-recreate *only* the control plane with the
publisher enabled at concurrency `C`, with **zero** job load. Sample the
remaining backlog, Hikari gauges and `pg_stat_activity` on every tick until the
backlog reaches zero.

Measuring a *drain* rather than a steady state is what makes this a capacity
measurement: the publisher is never able to keep up with an empty queue, so the
rate observed is the rate the publisher can achieve, not the rate at which work
arrives.

## 9. Manipulation Verification

Day 14 and Day 15 were each invalidated once by a manipulation that never
reached the container, so every run asserts its own configuration before any
data is kept:

- `concurrency_confirmed` greps the control-plane log for
  `Outbox publisher running with concurrency <C>`; the script **aborts the run**
  on mismatch.
- `partitions_actual` reads the live partition count from
  `kafka-topics.sh --describe`.

All ten runs recorded `concurrency_confirmed=1` and `partitions_actual=3`
(MEASURED). Independently, `jvm.threads.live` came out at exactly `30 + C`
(38, 42, 46, 54, 62), which corroborates that `C` publisher threads were really
allocated (MEASURED).

## 10. Measurement Anchoring — A Correction Applied During Analysis

The first pass of analysis divided the Phase-A backlog by the sampled drain
duration. **That figure is inflated and is not used as the headline result.**

The sampling clock `T0` starts *after* the readiness poll, the log grep and the
`kafka-topics.sh` call. The publisher begins working when the Spring context
comes up, which is earlier. The first sample therefore already shows a reduced
backlog, and the gap is much larger at high `C` (MEASURED):

| C | backlog built | backlog at first sample | drained before the clock started |
|---|---|---|---|
| 8 | 34,956 / 34,636 | 31,656 / 31,336 | 3,300 / 3,300 |
| 12 | 34,236 / 34,660 | 30,536 / 30,860 | 3,700 / 3,800 |
| 16 | 34,261 / 34,736 | 29,361 / 29,836 | 4,900 / 4,900 |
| 24 | 34,647 / 34,536 | 24,847 / 24,836 | 9,800 / 9,700 |
| 32 | 34,412 / 34,336 | 24,712 / 24,636 | 9,700 / 9,700 |

Dividing the *full* backlog by a duration that excludes that work credits the
publisher with events it published off the clock, and does so about 3x more
generously at C=24/32 than at C=8 — which would manufacture super-linear
scaling out of a timing artefact.

Every rate reported below is therefore computed **entirely inside the sampled
window**: backlog at the first sample, divided by (last sample time − first
sample time). Both endpoints are measured. The unanchored figures are shown
alongside only to document the size of the correction.

A second-order note on the same quantity: total `job_events` rows at the end of
each run (~40,000) exceed the Phase-A backlog snapshot (~34,500), so some events
were created after the snapshot. Any such event created *during* the window was
also published during it, which means the in-window rate is a **lower bound**,
never an overstatement.

## 11. C = 8 Results

| Replicate | window | backlog in window | in-window rate | OLS slope | unanchored |
|---|---|---|---|---|---|
| r1 | 47 s | 31,656 | **674/s** | 691/s | 728/s |
| r2 | 50 s | 31,336 | **627/s** | 663/s | 693/s |
| mean | | | **650/s** | 677/s | 710/s |

Hikari `maxActive` 8, `maxPending` **0**. Mean connection acquire 0.36 ms, mean
connection usage 459 ms. `jvm.threads.live` 38. (All MEASURED.)

This reproduces Day 16's C=8 saturated baseline (~700-750/s unanchored) and
restates it on the corrected clock as ~650/s.

## 12. C = 12 Results

| Replicate | window | backlog in window | in-window rate | OLS slope | unanchored |
|---|---|---|---|---|---|
| r1 | 31 s | 30,536 | **985/s** | 984/s | 1,070/s |
| r2 | 31 s | 30,860 | **995/s** | 1,004/s | 1,118/s |
| mean | | | **990/s** | 994/s | 1,094/s |

Hikari `maxActive` 12-13, `maxPending` **0**, acquire 0.37 ms, usage 517 ms,
threads 42. (MEASURED.)

## 13. C = 16 Results

| Replicate | window | backlog in window | in-window rate | OLS slope | unanchored |
|---|---|---|---|---|---|
| r1 | 23 s | 29,361 | **1,277/s** | 1,316/s | 1,370/s |
| r2 | 26 s | 29,836 | **1,148/s** | 1,246/s | 1,287/s |
| mean | | | **1,212/s** | 1,281/s | 1,328/s |

Hikari `maxActive` 16, `maxPending` **0**, acquire 0.49 ms, usage 474 ms,
threads 46. (MEASURED.) This is the widest replicate spread in the matrix
(10.6%) and is the main reason no efficiency claim below is stated to better
than about 10%.

## 14. C = 24 Results

| Replicate | window | backlog in window | in-window rate | OLS slope | unanchored |
|---|---|---|---|---|---|
| r1 | 15 s | 24,847 | **1,656/s** | 1,755/s | 2,165/s |
| r2 | 15 s | 24,836 | **1,656/s** | 1,820/s | 2,302/s |
| mean | | | **1,656/s** | 1,787/s | 2,234/s |

Hikari `maxActive` 24, `maxPending` **0**, acquire 0.78 ms, usage 552 ms,
threads 54. (MEASURED.)

The identical 1,656/s in both replicates is a coincidence of the 1 s sampling
granularity (both windows were 15 s and both starting backlogs were ~24,840),
not a sign of unusual precision. It should be read as 1,656 +/- ~110/s.

## 15. C = 32 Results

| Replicate | window | backlog in window | in-window rate | OLS slope | unanchored |
|---|---|---|---|---|---|
| r1 | 12 s | 24,712 | **2,059/s** | 2,202/s | 2,458/s |
| r2 | 12 s | 24,636 | **2,053/s** | 2,149/s | 2,641/s |
| mean | | | **2,056/s** | 2,175/s | 2,550/s |

Hikari `maxActive` 32, `maxPending` **0**, acquire 0.64 ms, usage 449 ms,
threads 62. (MEASURED.)

At C=32 the whole sampled window is 12 s across 9 samples. This is the coarsest
point in the matrix; see §22.

## 16. Kafka Metrics

Producer metrics read from the actuator at the end of each run, averaged over
replicates (MEASURED):

| C | `record.queue.time.avg` ms | `request.latency.avg` ms | `records.per.request.avg` | `batch.size.avg` B | `request.rate` /s |
|---|---|---|---|---|---|
| 8 | 5.14 | 1.30 | 7.84 | 1,023 | 81.1 |
| 12 | 5.14 | 1.40 | 11.67 | 1,348 | 81.1 |
| 16 | 5.16 | 1.63 | 15.24 | 1,675 | 54.5 |
| 24 | 5.16 | 2.32 | 21.61 | 2,251 | 30.9 |
| 32 | 5.08 | 2.19 | 28.56 | 2,894 | 25.1 |

Three things stand out, all MEASURED:

- **`record.queue.time.avg` is pinned at ~5.1 ms at every C.** That is
  `linger.ms = 5`. The producer is waiting out its linger timer, not queueing
  behind a busy broker. Broker pressure would make this rise; it does not.
- **`records.per.request.avg` tracks C almost exactly** (0.98, 0.97, 0.95, 0.90,
  0.89 x C). More concurrent senders means proportionally fuller batches in the
  same 5 ms window, so per-request cost is amortised *better* at high C, not
  worse.
- **`request.rate` falls** from 81/s to 25/s while throughput triples: fewer,
  larger requests.

`request.latency.avg` does rise, 1.30 to 2.19 ms, but it stays well below the
5 ms linger floor and cannot account for a 21% throughput shortfall. **H3 is
NOT SUPPORTED.**

`kafka.request.total`, `record.send.total` and `record.error.total` returned
empty from the actuator in all ten runs — those names are not in the registry.
No claim in this document rests on them.

## 17. HikariCP and PostgreSQL Metrics

`hikaricp.connections.pending` was **0 at every sample of every run**, including
C=32 against a pool of 40 (MEASURED). No publishing pass ever waited for a
connection. Mean acquire time stayed between 0.36 ms and 0.78 ms. **H2 is NOT
SUPPORTED** at the concurrency levels tested.

Mean connection *usage* (hold) time is the more interesting figure: 459, 517,
474, 552, 449 ms for C = 8 to 32 — **flat, with no trend in C** (MEASURED). Each
individual publishing pass costs the same regardless of how many run alongside
it.

PostgreSQL container CPU peaked at 40.3% of 1600% across all runs, and
`pg_stat_activity` showed `wait_event_type = 'Lock'` at zero on all but one
sample of one run (value 2). Note that `pg_stat_activity.state = 'active'`
frequently read 1 while Hikari reported 8-32 connections leased: the connections
are held but idle-in-transaction, blocked on Kafka acknowledgements rather than
doing database work.

> The header row written into `results17/*/drain.csv` is mislabelled: the
> columns are actually `t, unpublished, pg_active, pg_lockwait, hik_active,
> hik_pending`. The analysis above uses the true column order.

## 18. Control-Plane Metrics

Container CPU was captured as instantaneous `docker stats --no-stream`
snapshots every fourth tick, not as an average. The spread between replicates at
the same C (for example 113.0% and 43.2% at C=8) shows these are point samples
and should not be read as a per-level CPU cost.

What they do establish (MEASURED): the **highest control-plane sample anywhere
in the matrix was 113.0% of 1600%** — under 1.2 of 16 cores. The highest
PostgreSQL sample was 40.3% and the highest Kafka sample 242.6%. **H4 is NOT
SUPPORTED**: nothing came close to CPU saturation at any C.

`process.cpu.usage` parsed as `0.0` in the metrics files, which is a scraping
artefact rather than a real reading; it is not used.

Heap stayed between 546 MiB and 637 MiB with no trend in C.

## 19. Correctness

Identical across all ten runs (MEASURED, from SQL and from a Kafka console
consumer reading the topic from the beginning):

| Check | Result |
|---|---|
| Jobs submitted / completed | 10,000 / 10,000 |
| Jobs left QUEUED / RUNNING / RETRYING / DEAD_LETTER | 0 / 0 / 0 / 0 |
| `dup_claims` (same job_id + attempt_number twice) | 0 |
| `over_budget` (attempt_count > max_attempts) | 0 |
| `job_attempts` vs distinct `execution_id` | equal in every run |
| Unpublished events at end | **0** |
| Messages on `taskmesh.job-events` | 30,000 (30,051 and 30,003 in two runs with retries) |
| Unique event ids on the topic | equal to message count |
| Duplicate event ids | **0** |

Raising publisher concurrency from 8 to 32 produced **no duplicate publication,
no lost event and no job-level anomaly**. This is the `FOR UPDATE SKIP LOCKED`
claim plus the `published_at IS NULL` guard on `markPublished` doing their job,
and it is the precondition for treating any of the throughput numbers as
meaningful.

One honest limitation: because the outbox failure counter was not captured
(§16), this data shows that **no event was left unpublished**, not that **no
send ever failed**. A transient failure followed by a successful retry would
look identical here.

## 20. Comparative Analysis

Mean in-window rate, and scaling efficiency relative to the C=8 baseline
(CALCULATED from the measured rates):

| C | rate | speed-up vs C=8 | ideal | cumulative efficiency | marginal efficiency vs previous step |
|---|---|---|---|---|---|
| 8 | 650/s | 1.00x | 1.00x | 100% | — |
| 12 | 990/s | 1.52x | 1.50x | 102% | 102% |
| 16 | 1,212/s | 1.86x | 2.00x | 93% | 92% |
| 24 | 1,656/s | 2.55x | 3.00x | 85% | 91% |
| 32 | 2,056/s | 3.16x | 4.00x | **79%** | 93% |

Throughput rises **monotonically and substantially at every step**. There is no
inflection, no flattening and no maximum inside the tested range. But each step
returns only ~91-93% of proportional, so the shortfall compounds: 4x the
concurrency buys 3.16x the throughput.

### Where the missing 21% goes

Events published per tick is `C x batch-size = C x 100`. Dividing the in-window
event count by that gives the number of ticks, and the window length by the tick
count gives the mean tick period (CALCULATED):

| C | mean tick period | vs C=8 | mean pass duration (MEASURED, Hikari usage) | residual |
|---|---|---|---|---|
| 8 | 1,232 ms | 100% | 459 ms | 773 ms |
| 12 | 1,212 ms | 98% | 517 ms | 695 ms |
| 16 | 1,324 ms | 107% | 474 ms | 850 ms |
| 24 | 1,449 ms | 118% | 552 ms | 897 ms |
| 32 | 1,556 ms | 126% | 449 ms | 1,107 ms |

The tick period grows 26% from C=8 to C=32, and `1 / 1.26 = 0.79` — exactly the
cumulative efficiency above. **This is not independent corroboration**: tick
period is algebraically `C x 100 / rate`, so it is the same measurement
restated, and it appears here only because it re-expresses the result in units
that point at a mechanism.

The genuinely independent measurement is the **mean pass duration**, read from
`hikaricp.connections.usage` rather than from the backlog: it is **flat at
~450-550 ms with no trend in C**. Individual passes are not getting slower. So
the lengthening tick must come from something other than per-pass work.

Two candidates, both properties of the code in §5:

- A **fixed 500 ms poll delay** in which nothing is published. Against a mean
  pass of ~490 ms, that is roughly 40% of every tick spent idle — and it is
  *constant*, so it depresses all levels rather than causing the decline.
- The **barrier**: the tick ends only when the slowest of `C` passes ends. As
  `C` grows, the maximum of `C` samples drifts above their mean even when the
  mean is unchanged. This scales with `C` in the right direction and the right
  rough magnitude.

Attributing the residual specifically to the barrier is **INFERRED**, not
measured: the mean pass duration was measured, the *maximum* was not, and no
per-tick timing was instrumented. The measurements establish that the cost is
not in per-pass work, not in Kafka, not in the connection pool and not in CPU;
the barrier is the remaining candidate consistent with the data, not a proven
cause.

## 21. Saturation Classification

**Classification: DIMINISHING RETURNS.**

- Not *CONTINUES SCALING*: cumulative efficiency falls to 79% at C=32, and the
  decline is monotonic from C=16 onward.
- Not *PLATEAU*: throughput rises at every single step, by 24% even for the last
  one (C=24 to C=32). Nothing flattens.
- Not *INVALID / CONFOUNDED*: concurrency was confirmed in-container on all ten
  runs, partitions were asserted at 3, all other controls were held, and
  correctness was perfect throughout.

**The direct answer to the Day 17 question: the publisher does not saturate
anywhere in the range C = 8 to 32.** C=8 was not a ceiling — it was the edge of
the Day 15 grid, and roughly 3x more drain throughput was available beyond it.
H1 is **SUPPORTED** in direction, but its "roughly linear" form is **PARTIALLY
SUPPORTED** only: scaling is sub-linear at ~91-93% per step.

H2, H3 and H4 are all **NOT SUPPORTED**. Nothing external to the publisher —
not the connection pool, not the broker, not CPU — was near its limit at C=32.
The constraint that produced the 21% shortfall is **internal to the publisher's
own tick structure** (§20), which is where Day 18 should look.

## 22. Limitations

1. **Sampling resolution degrades badly at high C.** Windows shrink from ~48 s /
   33 samples at C=8 to 12 s / 9 samples at C=32. At 1 s granularity that is
   about +/-2% at C=8 but +/-8% at C=32. The backlog should be scaled up (or the
   sampling interval cut) before the high-C numbers are quoted more precisely.
2. **Replicate spread reaches 10.6%** (C=16). With two replicates per level, no
   single marginal-efficiency figure in §20 is resolved beyond that band. The
   *cumulative* 79% at C=32 is a larger effect than the per-step noise and
   accumulates in a consistent direction, which is why the classification rests
   on it rather than on any individual step.
3. **C=48 was deliberately not run.** HikariCP is fixed at 40 for this
   experiment, so 48 concurrent passes could not each hold a connection; eight
   would block on the pool and `C` would stop being the only variable. Raising
   the pool as well would change two things at once. C=40 is the arithmetic
   maximum under the current pool, and even that leaves no headroom for the
   lease reaper's connection, so C=32 is the largest clean point available
   without redesigning the controls.
4. **Container CPU figures are point samples, not averages** (§18). They support
   "nothing was near saturation" and nothing finer.
5. **Several intended metrics were never captured** — the outbox published and
   failure counters, and the Kafka `*.total` counters — because those names are
   not in the actuator registry. The correctness argument in §19 rests on SQL
   and on the Kafka topic contents instead, but the publish-failure question
   remains unanswerable from this data.
6. **Single-host, single-broker, RF=1.** A replicated broker with `acks=all`
   across real network hops would change the Kafka latency terms materially, and
   the linger-bound behaviour in §16 might not survive that.
7. **`T0` anchoring was a real defect in the harness**, corrected in analysis
   (§10) rather than by re-running. Correcting it in the script and re-running
   would be cleaner than correcting it arithmetically.

## 23. Follow-up Experiment Candidates

Ordered by expected value per unit of effort. All are **HYPOTHESIS** until run.

1. **Lower `poll-interval-ms`.** §20 measures ~500 ms of every ~1,230-1,560 ms
   tick as configured idle delay. This is the largest single identified
   non-productive term, it is a one-line configuration change, and it is
   completely untested. Sweep 500 / 250 / 100 / 50 ms at fixed C=8, which also
   keeps the window long enough to sample properly.
2. **Instrument the tick directly.** Record per-pass start/end and per-tick
   duration, so the barrier hypothesis in §20 can be confirmed or dismissed by
   measuring the *maximum* pass duration rather than inferring it. This is the
   single measurement that would turn the §20 conclusion from INFERRED into
   MEASURED.
3. **Send asynchronously within a pass.** `publishPending()` blocks on
   `send().get()` per event, so a 100-event batch pays ~100 sequential linger
   waits. Collecting the futures and joining once at the end of the batch would
   attack the per-pass cost directly — which §17 shows is flat at ~490 ms and is
   the other half of the tick.
4. **Raise Hikari and retest C=40 to 64.** Only worth doing *after* (1)-(3), and
   only as a deliberate two-variable experiment with the pool treated as a
   controlled co-variable rather than ignored.
5. **Increase the backlog to ~200,000 events** so that even C=32 drains over
   ~60 s, removing limitation 1 before any further high-C claims are made.
6. **Capture the outbox failure counter** (or add one) so §19 can distinguish
   "nothing was lost" from "nothing failed".
