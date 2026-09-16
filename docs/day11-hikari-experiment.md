# Day 11 — HikariCP Controlled Experiment

## 1. Objective

Determine experimentally whether HikariCP's default maximum pool size of 10 limits TaskMesh throughput at 5,000 logical workers / 10,000 jobs, and if so, where the limit moves when the pool is widened.

Day 10 observed the pool pinned at 10/10 with ~190 pending threads and connection acquisition (74–107 ms) an order of magnitude longer than connection usage (5.7–7.2 ms). That was a strong correlation but not a controlled test. This is the controlled test.

## 2. Hypothesis

If the pool of 10 is the limiting resource, then increasing `maximumPoolSize` should reduce connection acquisition latency and increase achieved throughput. If the pool is not limiting, throughput should stay flat despite the pool being saturated.

Day 10 also raised the possibility that widening the pool relocates the limit to PostgreSQL rather than removing it.

## 3. Experimental Controls

Held constant across all runs: machine, Docker resource allocation, PostgreSQL/Redis/Kafka images, control-plane image and code, load-generator build, worker count (5,000), job count (10,000), job type and duration, worker capacity (2), heartbeat/poll/lease/renew timing, reaper and outbox configuration, ramp-up (500/sec), and **random seed (424242)** so jitter is identical.

**The only variable is `maximumPoolSize`.**

Every run started from `docker compose down -v` — fresh containers, fresh volumes, empty database. Pool size was read back from `hikaricp.connections.max` after startup and the run aborted on mismatch. The bundled `worker` container was stopped so the generator supplied all workers.

No production Java, SQL, schema, index, Redis, Kafka, outbox, Tomcat, polling, lease, or scheduler configuration was changed.

## 4. Environment

| Item | Value |
|---|---|
| Git commit | `f868e9c` |
| CPU | Intel i5-12500H — 12 physical / 16 logical |
| System RAM | 15.7 GiB |
| OS | Windows 11 Home 10.0.26200 |
| Docker Desktop | 16 CPUs, 7.602 GiB |
| Java / Maven | 21.0.2+13-LTS / 3.9.16 |
| Docker / Compose | 29.8.0 / v5.5.1 |
| PostgreSQL / Redis / Kafka | 16.15 / 7.4.11 / 4.2.1 |
| Control plane | `taskmesh-control-plane:latest`, 1 Compose instance |
| Load generator | `taskmesh-loadgen-0.0.1-SNAPSHOT` |

Generator, control plane and all datastores share the same 16 logical cores. This matters — see §11 and §13.

## 5. Configuration Matrix

| Workers | Jobs | Hikari Max | Seed | Replicates |
|---:|---:|---:|---:|---:|
| 5000 | 10000 | 10 | 424242 | 2 |
| 5000 | 10000 | 20 | 424242 | 2 |
| 5000 | 10000 | 40 | 424242 | 2 |
| 5000 | 10000 | 80 | 424242 | 2 |

Plus one optional validation at 10,000 workers / 10,000 jobs, pool 40 (§10).

### Configuration mechanism

The repository contains **no** Hikari configuration — `application.yml` sets only datasource url/username/password, so HikariCP's own default of 10 applied to every run before today.

`SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` is a stock Spring Boot property reachable by relaxed binding, so no Java or `application.yml` change was needed. One pass-through line was added to `docker-compose.yml`:

```yaml
SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE: ${HIKARI_MAX_POOL_SIZE:-10}
```

Unset, this yields 10 — byte-for-byte the previous behaviour. Verified empirically: unset → `hikaricp.connections.max = 10.0`; `HIKARI_MAX_POOL_SIZE=20` → `20.0`.

Note `minimumIdle` follows `maximumPoolSize` (HikariCP's documented default), so these runs varied both. That is the default behaviour, not an additional variable introduced here.

## 6. Results

Each cell is the mean of two replicates. Individual replicates in §6.1.

| Pool | Offered req/s | Achieved req/s | Acquire mean | Acquire max | Usage mean | Claim query mean | DB CPU | CP CPU | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 5,873 | **1,020** | 154.2 ms | 7.66 s | 8.92 ms | 4.52 ms | 328% | 528% | 6.4% |
| 20 | 6,000 | **1,333** | 118.5 ms | 6.58 s | 14.38 ms | 4.70 ms | 593% | 621% | 2.5% |
| 40 | 5,982 | **1,539** | 81.8 ms | 7.35 s | 23.24 ms | 8.61 ms | 583% | 743% | 2.7% |
| 80 | 6,000 | **1,531** | 59.1 ms | 4.31 s | 44.01 ms | 18.84 ms | 651% | 745% | 2.1% |

Acquire/usage are Micrometer **mean and max only**. Percentiles are unavailable without enabling histogram publication, which would have changed configuration and was therefore not done.

| Pool | Jobs Completed | Attempts | Reassignments | Duplicate Claims | DLQ | Orphans |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 10,000 / 10,000 | 11,085 / 10,000 | 1,085 / 0 | 0 | 0 | 0 |
| 20 | 10,000 / 10,000 | 10,003 / 10,000 | 3 / 0 | 0 | 0 | 0 |
| 40 | 10,000 / 10,000 | 10,000 / 10,000 | 0 / 0 | 0 | 0 | 0 |
| 80 | 10,000 / 10,000 | 10,000 / 10,000 | 0 / 0 | 0 | 0 | 0 |

### 6.1 Replicate variance

| Pool | Achieved r1 | Achieved r2 | Spread |
|---:|---:|---:|---:|
| 10 | 948 | 1,092 | 15% |
| 20 | 1,273 | 1,392 | 9% |
| 40 | 1,662 | 1,415 | 17% |
| 80 | 1,642 | 1,419 | 16% |

Run-to-run spread is 9–17%. **The pool 40 and pool 80 means (1,539 vs 1,531) differ by 0.5% — far inside that variance, so they are not distinguishable by this experiment.** Neither replicate was discarded.

Latency (mean of replicates):

| Pool | Heartbeat p50/p95/p99 | Poll p50/p95/p99 | Completion p50/p95/p99 |
|---:|---|---|---|
| 10 | 4,359 / 7,250 / 8,293 ms | 3,960 / 7,814 / 8,555 ms | 3,688 / 5,210 / 5,752 ms |
| 20 | 2,555 / 5,596 / 6,410 ms | 2,555 / 6,303 / 6,823 ms | 3,176 / 5,872 / 6,303 ms |
| 40 | 2,068 / 5,695 / 6,104 ms | 2,010 / 5,808 / 6,226 ms | 2,355 / 5,313 / 5,871 ms |
| 80 | 2,091 / 5,701 / 6,171 ms | 2,016 / 5,815 / 6,230 ms | 2,934 / 5,495 / 6,067 ms |

## 7. PostgreSQL Behavior

| Pool | Peak active sessions | DB CPU | Claim query mean | Claim query max | Lock waits |
|---:|---:|---:|---:|---:|---:|
| 10 | 7.5 | 328% | 4.52 ms | 138 ms | 0 |
| 20 | 11 | 593% | 4.70 ms | 144 ms | 0 |
| 40 | 28.5 | 583% | 8.61 ms | 251 ms | 0 |
| 80 | 53 | 651% | 18.84 ms | 790 ms | 0 |

Two things move together as the pool widens: concurrent sessions rise roughly with the pool, and **the same claim query gets slower** — 4.52 ms at pool 10 to 18.84 ms at pool 80, a 4.2× increase for identical SQL against an identical dataset. Connection hold time rises similarly (8.92 → 44.01 ms, 4.9×).

Lock waits remained **0** at every pool size. The slowdown is not lock contention.

DB CPU roughly doubles from pool 10 to 20, then plateaus in the 580–650% band while sessions grow from 11 to 53.

## 8. Redis Behavior

Measured from `INFO commandstats` per run (counters reset before each run):

| Pool | SET calls | SET µs/call | Failed calls | Redis CPU |
|---:|---:|---:|---:|---:|
| 10 | ~36k | ~15 µs | 0 | 19.4% |
| 20 | ~36k | ~15 µs | 0 | 20.4% |
| 40 | ~36k | ~15 µs | 0 | 14.6% |
| 80 | ~36k | ~15 µs | 0 | 11.7% |

Redis was unaffected by pool size and never showed an error. Its contribution to the 8.92–44.01 ms connection hold remains under 1%. The Day 8 Redis-coupling hypothesis remains **unsupported** by measurement.

## 9. Kafka / Outbox Behavior

| Pool | Outbox drain rate | Unpublished after run |
|---:|---:|---:|
| 10 | 100–104 events/sec | 30,998 – 35,478 |
| 20 | 100–104 events/sec | 32,211 – 32,211 |
| 40 | 100 events/sec | 32,597 – 32,636 |
| 80 | 100 events/sec | 33,236 – 33,303 |

**The outbox drain rate is exactly 100 events/sec at every pool size** — an 8× pool increase changed it by nothing. Kafka producer latency stayed ~1.5 ms with no errors, and Kafka CPU was never the constraint.

This confirms the outbox is an **independent** bottleneck on event publication, entirely decoupled from job-execution throughput. Job throughput rose 51% while event publication did not move at all. Not modified today.

## 10. Optional 10K Validation — aborted

One run at 10,000 workers / 10,000 jobs with pool 40:

```
GENERATOR-SATURATED - probe wake-up p99 = 748.0ms exceeds 100.0ms
requests achieved : 820/sec
total=74673 successes=6310 failures=68363   (91.5% failure)
transport errors: ConnectException=8324, HttpConnectTimeoutException=49111,
                  HttpTimeoutException=10823, IOException=105
jobs completed: 2
```

Generator saturation returned decisively, so per the experiment rules this line was stopped and **no 10K claim is made**. The run is invalid as a control-plane measurement.

Worth recording: even under total collapse, 3,913 attempts produced 3,913 distinct execution ids with **0 duplicate claims and 0 over-budget jobs**. Correctness did not degrade with throughput.

## 11. Bottleneck Movement

**The bottleneck moved off Hikari, but where it moved cannot be isolated on this host.**

Evidence the pool was limiting (pool 10 → 40):
- Achieved throughput +51% (1,020 → 1,539 req/s)
- Acquisition latency −47% (154.2 → 81.8 ms)
- Error rate 6.4% → 2.7%
- Heartbeat p50 −53% (4,359 → 2,068 ms)

Evidence the limit relocated (pool 40 → 80):
- Throughput flat (1,539 → 1,531, inside 17% replicate variance)
- Acquisition still falling (81.8 → 59.1 ms) — the pool is no longer the wait point
- Connection hold time +89% (23.24 → 44.01 ms)
- Claim query +119% (8.61 → 18.84 ms) for identical SQL
- Concurrent DB sessions 28.5 → 53

So widening the pool past ~40 buys no throughput while each unit of work takes longer — the signature of a downstream resource absorbing the added concurrency.

**The confound.** Summed container CPU across runs:

| Pool | Container CPU sum (of 1600% available) |
|---:|---:|
| 10 | 856%, 970% |
| 20 | 1354%, 1197% |
| 40 | 1394%, 1426% |
| 80 | 1285%, **1623%** |

At pool 40–80 the containers alone consume 80–100%+ of the host's 16 cores, before counting the generator JVM (~80%). The throughput plateau coincides exactly with host CPU exhaustion.

Therefore the slower claim queries and longer connection holds are consistent with **either** PostgreSQL becoming the limiting resource **or** every component being CPU-starved by co-tenancy. This experiment cannot separate the two, and I am not claiming it can.

## 12. Measured vs Calculated vs Hypothesis vs Confirmed

**MEASURED**
- Achieved throughput 1,020 / 1,333 / 1,539 / 1,531 req/s at pools 10/20/40/80.
- Acquisition mean 154.2 / 118.5 / 81.8 / 59.1 ms.
- Connection usage mean 8.92 / 14.38 / 23.24 / 44.01 ms.
- Claim query mean 4.52 / 4.70 / 8.61 / 18.84 ms; lock waits 0 throughout.
- Peak DB sessions 7.5 / 11 / 28.5 / 53; DB CPU 328 / 593 / 583 / 651%.
- Outbox drain 100 events/sec at every pool size.
- Redis SET ~15 µs, 0 failures, at every pool size.
- Error rate 6.4% → 2.1% as the pool widened.
- Container CPU sum reaching 1,623% of 1,600% at pool 80.
- Replicate spread 9–17%.

**CALCULATED**
- Throughput gain +30.7% (pool 20), +50.9% (pool 40), +50.1% (pool 80) vs pool 10.
- Pool utilization: 10/10, 19.5/20, 39/40, 80/80 at peak.
- Pool 40 vs 80 difference (0.5%) is below replicate variance (9–17%).

**HYPOTHESIS**
- PostgreSQL is the new limiting resource beyond pool ~40. Supported by rising claim latency and hold time, but **not separable from host CPU exhaustion** on this single co-tenant host.
- The residual ~2% transport error rate reflects connection-acceptance pressure. Cause not isolated.

**CONFIRMED**
- **HikariCP maximum pool size of 10 was limiting throughput at 5,000 logical workers.** A controlled single-variable change raised throughput 51% and cut acquisition latency 47%, with all other variables held constant across two replicates. This upgrades the Day 10 correlation to a demonstrated causal relationship.
- **The outbox publisher is an independent bottleneck on event publication**, unaffected by pool size (100 events/sec across an 8× pool change).

Explicitly **not** confirmed: that PostgreSQL is now the bottleneck.

## 13. Limitations

- Single host with generator co-resident; at pool 40–80 the host is CPU-saturated, which confounds the bottleneck-movement analysis. A separate load-generation host is required to resolve it.
- Two replicates per configuration; 9–17% spread means differences smaller than ~20% are not resolvable.
- Hikari acquire/usage available as mean and max only — no percentiles without a configuration change.
- `minimumIdle` tracks `maximumPoolSize` by Hikari default, so both varied together.
- 60-second steady-state windows; no soak testing.
- Queue latency (submit → first claim) not measured.
- Outbox drain measured as a rate over 30 s, not to full drain (~850 s at 100/sec).
- PostgreSQL ran with stock configuration; `shared_buffers`, `max_connections` etc. were not tuned or examined as variables.

## 14. Engineering Interpretation

The measurements show that the default pool of 10 was holding back throughput at 5,000 logical workers: widening it to 40 increased achieved throughput by about half and roughly halved connection-acquisition latency and the error rate, with identical workload, seed and infrastructure. That answers the Day 10 question — the pool was not merely saturated, it was limiting.

They also show that the gain stops. Between pool 40 and 80 throughput does not move outside run-to-run variance, while connection hold time and claim-query latency roughly double. Adding concurrency past that point changes where time is spent rather than how much work completes: threads stop queueing for connections and start queueing inside the database path instead.

What this experiment cannot say is *what* absorbs that concurrency. PostgreSQL session count and query latency both rise, which looks like the database becoming the constraint — but at those pool sizes the containers alone are consuming essentially the whole 16-core host, so component-level starvation is an equally consistent explanation. Distinguishing them requires moving the load generator off the machine under test.

Separately, the outbox publisher is confirmed as an independent constraint: an 8× change in pool size left event publication at exactly 100 events/sec. Job execution and event publication scale independently, and the backlog (~33,000 events after a 10,000-job run) is a publication-latency problem, not a job-throughput problem.

Correctness was unaffected by every configuration tested. All 10,000 jobs reached `COMPLETED` in all eight runs, with zero duplicate claims, zero execution-id collisions, zero orphaned jobs, zero dead-letters, and no over-budget attempts — including in the failed 10K run where 91.5% of requests errored. One observation worth noting: pool 10 replicate 1 produced 1,085 reassignments (attempts 11,085 vs 10,000) because latency pushed leases past expiry, while pools 40 and 80 produced zero. Lower pool pressure reduced lease churn.

**Potential resume metric — requires final review:** "Ran a controlled single-variable connection-pool experiment (4 pool sizes × 2 replicates) against 5,000 logical workers and 10,000 jobs, demonstrating a 51% throughput increase and 47% reduction in connection-acquisition latency, and identifying the point at which further pool growth stops yielding throughput."

No configuration is recommended as "best" here; this document reports what each configuration measured. Selecting a value is a separate decision requiring a host that is not CPU-saturated.
