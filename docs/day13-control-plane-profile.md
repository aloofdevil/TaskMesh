# Day 13 — Control Plane CPU Profiling

## 1. Objective

Day 12 showed throughput is far more sensitive to control-plane CPU than to PostgreSQL CPU. This day identifies **where** that CPU goes, using JVM profiling rather than inference. No production code was changed.

## 2. Day 12 Motivation

Cutting the control plane from ~7.5 to 4 cores cost 60% of throughput; the same proportional cut to PostgreSQL cost 15%. Under control-plane starvation, Hikari acquisition collapsed from 93.8 ms to 0.9 ms with active connections falling from 40 to 1 — the control plane could not generate enough concurrent work to occupy its own pool. Whatever limits throughput in that state is inside the control-plane process.

## 3. Environment

| Item | Value |
|---|---|
| Git commit | `e3703eb` |
| CPU / RAM | Intel i5-12500H, 12 physical / 16 logical; 15.7 GiB |
| Docker Desktop | 16 CPUs, 7.602 GiB, WSL2 |
| Control-plane runtime | `eclipse-temurin:21-jre`, OpenJDK 21.0.12 |
| Host JDK (analysis) | 21.0.2, `jfr` CLI |
| PostgreSQL / Redis / Kafka | 16.15 / 7.4.11 / 4.2.1 |

The runtime image is a **JRE** — it ships `jfr` but not `jcmd`, so a recording cannot be attached to a running JVM. Recording had to be started with JVM flags.

## 4. Profiling Method

Java Flight Recorder, started at JVM launch:

```
-XX:FlightRecorderOptions=stackdepth=128
-XX:StartFlightRecording=settings=profile,delay=45s,duration=90s,filename=/tmp/cp.jfr,name=day13
```

`settings=profile` gives CPU execution samples, allocation sampling, monitor/blocking events, thread states and GC. `stackdepth=128` (default 64) was needed so Spring/Hibernate stacks are not truncated before reaching application frames.

`delay=45s` excludes JVM boot, Spring context initialisation and the 10,000-job submission from the recording. Measured timings confirm the window landed correctly: readiness at t+27 s, submission complete and load generator starting at t+48 s, generator finished t+127 s — against a recording window of t+45 s to t+135 s.

To attach the flags without rebuilding the image or touching application code, one pass-through was added to `docker-compose.yml`, defaulting to the exact value already baked into `docker/Dockerfile.control-plane`:

```yaml
JAVA_OPTS: ${CP_JAVA_OPTS:--XX:MaxRAMPercentage=75.0 -Dnetworkaddress.cache.ttl=10}
```

Verified: with `CP_JAVA_OPTS` unset, `docker compose config` resolves `JAVA_OPTS` to the Dockerfile value — unchanged behaviour.

### Profiling overhead

| Run | JFR | Achieved req/s |
|---|---|---:|
| P0 | off | 1,320 |
| P1 | profile | 1,454 |
| P2 | profile | 1,381 |

The profiled runs were **faster** than the unprofiled baseline. JFR overhead is therefore below the run-to-run noise floor (~10% spread here, 9–17% established on Day 11) and cannot be quantified from these measurements — it is not that overhead is zero, but that it is smaller than the variance. No claim of a specific overhead figure is made.

Note that today's absolute throughput (1,320–1,454 req/s) is below Day 12's run A (1,875 req/s) in an identical configuration. Cross-day variation remains large; Day 13 figures should not be compared to Day 12's.

## 5. Workload

5,000 logical workers, 10,000 jobs, Hikari 40, seed 424242, ramp 500/sec, 60 s run, capacity 2. All heartbeat/poll/lease/reaper/retry/outbox/Redis/Kafka configuration unchanged.

## 6. Request Mix

Generator-side counts, P1, over the ~70 s generator lifetime:

| Endpoint | Requests | Share | Approx rate | Notes |
|---|---:|---:|---:|---|
| poll (`/claim`) | 40,803 | 37.7% | ~583/s | 29,677 returned 204 empty (72.7% of polls) |
| heartbeat | 33,974 | 31.4% | ~485/s | 204 No Content |
| registration | 5,000 (+5,151 retries) | ~9.4% | — | burst during ramp |
| claim (successful) | 9,553 | 8.8% | ~136/s | subset of polls |
| completion | 9,553 | 8.8% | ~136/s | |
| lease renewal | 9,158 | 8.5% | ~131/s | |
| deregistration | 4,867 | 4.5% | — | burst at shutdown |
| **total HTTP** | **108,373** | 100% | ~1,548/s | 204s: 77,641 (71.6%) |

**Polls and heartbeats together are 69% of all requests**, and empty polls alone are 27.4% of every request the control plane serves.

## 7. CPU Hot Paths

**These are CPU execution samples, not exact CPU time.** P1: 4,214 samples; P2: 4,152. Percentages are share of samples.

### By thread group

| Thread group | P1 samples | P1 % | P2 % |
|---|---:|---:|---:|
| `http-nio-8080-exec` (Tomcat request threads) | 3,825 | **90.8%** | **92.9%** |
| `lettuce-nioEventLoop` (Redis) | 164 | 3.9% | 3.3% |
| `kafka-producer` | 108 | 2.6% | 2.6% |
| `scheduling-` (reaper + outbox) | 96 | 2.3% | 0.7% |
| Hikari threads | 17 | 0.4% | 0.5% |

CPU is overwhelmingly in request handling. The reaper and outbox publisher together account for 0.7–2.3%.

### Leaf frames — where the CPU actually was (P1)

| Rank | Class | Method | Samples | % | Thread |
|---:|---|---|---:|---:|---|
| 1 | `java.util.concurrent.ConcurrentHashMap` | `get` | 174 | 4.1% | http-nio |
| 2 | `java.util.HashMap` | `getNode` | 153 | 3.6% | http-nio |
| 3 | `java.lang.ThreadLocal$ThreadLocalMap` | `getEntry` | 95 | 2.3% | http-nio |
| 4 | `java.lang.String` | `hashCode` | 72 | 1.7% | http-nio |
| 5 | `ch.qos.logback.classic.Logger` | `isDebugEnabled` | 67 | 1.6% | http-nio |
| 6 | `org.springframework.util.ObjectUtils` | `nullSafeEquals` | 57 | 1.4% | http-nio |
| 7 | `java.util.HashMap` | `putVal` | 43 | 1.0% | http-nio |
| 8 | `java.lang.AbstractStringBuilder` | `ensureCapacityInternal` | 38 | 0.9% | http-nio |
| 9 | `ch.qos.logback.classic.LoggerContext` | `getTurboFilterChainDecision` | 33 | 0.8% | http-nio |
| 10 | `org.springframework.core.ReactiveAdapterRegistry` | `getAdapter` | 31 | 0.7% | http-nio |
| 11 | `org.springframework.core.ResolvableType` | `forType` | 30 | 0.7% | http-nio |
| 12 | `io.micrometer.core.instrument.distribution.TimeWindowMax` | `record` | 29 | 0.7% | http-nio |

P2's top frames are the same set in nearly the same order.

### Leaf frame by component (exclusive attribution)

| Component | P1 % | P2 % |
|---|---:|---:|
| JDK (collections, ThreadLocal, String) | 43.7% | 43.1% |
| Hibernate / JPA | 14.7% | 13.6% |
| HTTP / Tomcat / Spring-web | 11.4% | — |
| Spring (other) | 10.3% | — |
| Micrometer metrics | 3.5% | — |
| PostgreSQL JDBC driver | 3.3% | — |
| Logging | 3.1% | — |
| Jackson (JSON) | 2.0% | — |
| Spring tx/AOP | 1.9% | — |
| Kafka client | 1.5% | — |
| Hikari pool | 1.0% | — |
| Lettuce / Redis | 0.9% | — |
| **TaskMesh application code** | **0.5%** | — |

**TaskMesh's own code is almost never the leaf frame.** The CPU is spent in framework and JDK machinery invoked on its behalf.

### Inclusive attribution — component present anywhere in the stack

| Component | P1 % |
|---|---:|
| JDK reflection (`jdk.internal.reflect`) | **61.4%** |
| Spring tx / AOP / CGLIB | **60.5%** |
| HTTP / Tomcat / Spring-web | 77.4% |
| TaskMesh application code | 53.1% |
| Hibernate / JPA | 52.0% |
| Hikari pool | 10.3% |
| Micrometer metrics | 9.3% |
| PostgreSQL JDBC driver | 8.3% |
| Logging | 4.5% |
| Lettuce / Redis | 4.2% |
| Jackson | 3.3% |
| Kafka client | 2.8% |

### Attribution to application operations

Topmost `com.taskmesh` frame per sample — which operation owns the CPU:

| Operation | Samples | % |
|---|---:|---:|
| **No TaskMesh frame at all** | 1,975 | **46.9%** |
| `DispatchService.doClaim` + CGLIB proxy | 708 | 16.8% |
| `WorkerService.heartbeat` + CGLIB proxy | 470 | 11.2% |
| `ExecutionService.complete` + proxy | 189 | 4.5% |
| `JobEventRecorder.recordJobEvent` / `recordWorkerEvent` | 169 | 4.0% |
| `ExecutionService.renewLease` + proxy | 147 | 3.5% |
| `WorkerService.register` + proxy | 137 | 3.3% |
| `WorkerService.requireActive` | 114 | 2.7% |
| `WorkerLivenessCache.markAlive` | 91 | 2.2% |
| `WorkerService.deregister` | 69 | 1.6% |
| `OutboxPublisher.send` | 22 | 0.5% |

**Nearly half of all CPU samples never reach TaskMesh code.** That 46.9% breaks down as:

| Phase | % of all samples |
|---|---:|
| unclassified framework plumbing | 14.0% |
| Spring MVC dispatch | 12.1% |
| Tomcat HTTP parse/write | 9.4% |
| Micrometer | 5.6% |
| Jackson serialize/deserialize | 2.8% |
| Spring return-value handling | 2.3% |
| Logging | 0.5% |

## 8. Thread Analysis

`jdk.ThreadPark` — 122,554 events, of which **118,954 (97.1%) are Tomcat `http-nio-8080-exec` threads**. Request threads spend heavily on waiting, consistent with Day 12's finding that ~156–160 threads queue for 40 connections.

Other parking: scheduler 2,206, Hikari connection-adder 1,126, Catalina-utility 212, Hikari housekeeper 40.

### Monitor contention — the strongest single finding

`jdk.JavaMonitorEnter`, P1: 4,818 events, **total blocked time 203,499 ms**, mean 42.24 ms, max 384.0 ms. In a 90-second window that is roughly 2.3 threads continuously blocked.

By monitor class: `java.lang.Class` **3,398 events** (70.5%), `java.util.HashMap` 541, `java.lang.Object` 466, Tomcat `SynchronizedQueue` 189.

Tracing the `java.lang.Class` contention to its source:

```
java.util.TimeZone.getTimeZone                                          3398
org.hibernate.type.descriptor.jdbc.TimestampUtcAsJdbcTimestampJdbcType$2.doExtract  1769
org.hibernate.type.descriptor.jdbc.BasicExtractor.extract                           1769
org.hibernate.type.descriptor.jdbc.TimestampUtcAsJdbcTimestampJdbcType$1.doBind     1629
org.hibernate.type.descriptor.jdbc.BasicBinder.bind                                 1629
```

`java.util.TimeZone.getTimeZone()` synchronises on a class-level lock in the JDK. Hibernate calls it on **every** `TIMESTAMPTZ` bind and extract. The `jobs` table carries several such columns (`scheduled_at`, `lease_until`, `created_at`, `updated_at`), and claim/renew/complete all bind and extract them.

**97.7% of all monitor blocking (4,708 of 4,818 events) is on `http-nio` request threads** — squarely in the hot path.

The Hibernate JDBC type involved is selected by existing configuration in `application.yml`:

```yaml
hibernate:
  jdbc:
    time_zone: UTC
```

**Reproducibility:** present in both runs but with substantially different magnitude — P1: 3,398 `java.lang.Class` events, 203,499 ms blocked; P2: 1,016 events, 60,014 ms blocked, mean 36.73 ms. The phenomenon reproduces; its magnitude varies by ~3.4× between runs.

## 9. Database Interaction

Following the Day 12 discipline of separating layers:

| Layer | Evidence |
|---|---|
| Application Java time | TaskMesh code is the leaf frame in only 0.5% of samples |
| Hibernate/JPA processing | 14.7% exclusive as leaf, 52.0% inclusive |
| PostgreSQL JDBC driver | 3.3% exclusive, 8.3% inclusive |
| Connection acquisition | Hikari 1.0% exclusive, 10.3% inclusive; acquire mean 96.5 ms (metric) |
| PostgreSQL server execution | not visible to JFR; PG container CPU 320–668% |

Hibernate is the single largest *identified* library consumer inside the database path — roughly 4× the JDBC driver's share as a leaf. Timestamp binding/extraction is both a CPU cost and the source of the lock contention above.

This reinforces the Day 12 caveat: `taskmesh.jobs.claim` (11.3–13.4 ms in these runs) spans Java, Hibernate, connection acquisition and SQL. It is not a PostgreSQL query timer.

## 10. Redis

3.9% of CPU samples (P1) / 3.3% (P2), on `lettuce-nioEventLoop` threads; 4.2% inclusive. `WorkerLivenessCache.markAlive` owns 2.2% of samples.

Redis remains a small CPU contributor, consistent with Days 10–12. Note the Day 8 structural observation still holds — `markAlive` executes inside the heartbeat transaction — but at these volumes its measured cost is minor.

## 11. Kafka / Outbox

`kafka-producer` threads: 2.6% of samples in both runs. `OutboxPublisher.send` owns 0.5%. Scheduler threads (reaper + outbox) total 0.7–2.3%.

Outbox backlog after each run: 34,069–38,099 unpublished events, drain ~100 events/sec as in every run since Day 10.

**The outbox publisher is not a meaningful consumer of control-plane CPU.** Its 100 events/sec ceiling is a latency/serialisation property, not a CPU cost. Unchanged today.

## 12. Allocation / GC

**GC is not a significant contributor.** Over the 90 s window: 40 young collections, 62 pause events, **total pause 1,181.9 ms (1.3% of wall time)**, mean 19.06 ms, max 106.00 ms. Heap cycled between 62.9 MB and 137.6 MB.

Top allocated types (`jdk.ObjectAllocationSample`, 22,204 samples): `byte[]` 2,909, `Object[]` 1,748, `int[]` 603, `String` 572, `HashMap$Node[]` 570, `HashMap` 525, `ArrayList` 475, `HashMap$Node` 447, `LinkedHashMap` 411, `ConcurrentHashMap$Node` 351, and `org.springframework.core.ResolvableType` 299.

The profile is dominated by generic framework allocation, not by TaskMesh DTOs. `ResolvableType` allocation corroborates the Spring reflection/type-resolution cost seen in the leaf frames. **Identified as candidates only; nothing optimised.**

## 13. CPU Accounting

Units, per Day 12 discipline:

- **`docker stats` CPUPerc** — 100% ≈ one core; 16 cores ≈ 1600% capacity.
- **JFR execution samples** — a *share of sampled CPU activity inside the JVM*, not a fraction of host CPU and not directly convertible to core-seconds.

These are not combined. Container CPU during the profiling runs: control-plane 932–989%, PostgreSQL 320–668%. The JFR percentages describe how the control plane's ~9.3–9.9 cores were distributed internally.

## 14. Candidate Bottlenecks

**Candidate 1 — Framework request overhead (Spring MVC dispatch, Tomcat, reflection, proxies)**
*Evidence:* 46.9% of samples never reach application code; reflection present in 61.4% of stacks, Spring tx/AOP/CGLIB in 60.5%; Spring MVC dispatch 12.1% and Tomcat parse/write 9.4% as identifiable phases; TaskMesh code is the leaf in only 0.5%.
*Counter-evidence:* this is intrinsic to the chosen framework, spread across many frames rather than concentrated in one hot method; no single fixable target is implied.
*Classification:* **MEASURED** (the distribution). Its causal contribution to the throughput ceiling is **HYPOTHESIS**.

**Candidate 2 — `TimeZone.getTimeZone` lock contention in Hibernate timestamp handling**
*Evidence:* 203,499 ms blocked in a 90 s window (P1), mean 42.24 ms per event; 70.5% of all monitor events on one `java.lang.Class` lock; traced to `TimestampUtcAsJdbcTimestampJdbcType` bind/extract; 97.7% of blocking on request threads; reproduced in P2.
*Counter-evidence:* magnitude varies 3.4× between P1 and P2; blocked time is *wall* time on parked threads, not CPU consumption — it inflates latency and occupies request threads and their connections rather than burning cores. Throughput impact has not been demonstrated.
*Classification:* **MEASURED** contention. Causal link to throughput is **HYPOTHESIS** — and the most specific, testable one produced today.

**Candidate 3 — Non-productive polling volume**
*Evidence:* 29,677 empty polls = 27.4% of all requests; polls + heartbeats = 69% of traffic; each empty poll still runs `requireActive` plus the claim query inside a transaction.
*Counter-evidence:* per-sample cost is unremarkable; the claim path's 16.8% covers all 40,803 polls, so per-request cost is comparable to heartbeat's.
*Classification:* **MEASURED** volume; efficiency impact **HYPOTHESIS**.

**Candidate 4 — Logging and metrics instrumentation**
*Evidence:* logging 3.1% exclusive / 4.5% inclusive, with `Logger.isDebugEnabled` alone at 1.6%; Micrometer 3.5% exclusive / 9.3% inclusive, appearing as 5.6% of the non-application phase.
*Counter-evidence:* both are single-digit; removing them entirely could not explain the ceiling.
*Classification:* **MEASURED**, minor.

**Ruled out as significant CPU contributors:** GC (1.3% of wall time in pauses), Jackson/JSON (2.0% exclusive, 3.3% inclusive), Kafka/outbox (2.6%), Redis (3.9%), reaper/outbox scheduler threads (0.7–2.3%).

## 15. Confirmed Findings

Only what the measurements establish directly:

1. **Control-plane CPU is spent in request handling, not background work.** 90.8–92.9% of samples on Tomcat request threads; scheduler threads 0.7–2.3%. Reproduced across both runs.
2. **TaskMesh application code is almost never the CPU leaf** — 0.5% of samples. The cost is in framework and JDK machinery.
3. **A specific JDK-level lock is heavily contended in the hot path**: `TimeZone.getTimeZone` via Hibernate timestamp bind/extract, 70.5% of monitor events, 97.7% on request threads, present in both runs.
4. **GC, Jackson, Kafka, Redis and the outbox are not significant CPU contributors** at this workload.
5. **JFR overhead is below the measurement noise floor** and could not be quantified.

Not confirmed: that any of these *causes* the throughput ceiling. Day 13 is identification; establishing causality requires a controlled change, which is Day 14's job.

## 16. Hypotheses for Day 14

Three candidates, in order of how specifically the evidence points at them:

1. **`TimeZone.getTimeZone` contention via Hibernate's UTC timestamp handling.** The most specific and most testable finding: one identified lock, in the hot path, with 203 s of blocked time in a 90 s window. A controlled experiment could determine whether reducing this contention changes throughput or latency.
2. **Non-productive polling volume.** 27.4% of all requests are empty polls that still open a transaction and run two queries. Reducing this is a protocol-level question, not a micro-optimisation.
3. **Per-request framework overhead.** The largest single share (46.9%), but diffuse; it is the cost of the stack rather than a defect, and has no obvious narrow intervention.

## 17. Limitations

- JFR execution samples are a statistical sample of *on-CPU* activity. They do not measure blocked/waiting time, which is why `ThreadPark` and `JavaMonitorEnter` are reported separately and must not be added to the sample percentages.
- ~4,200 samples per run over 90 s is sufficient for ranking major components but not for resolving sub-1% differences.
- Monitor-contention magnitude varied 3.4× between the two runs; only its presence, not its size, is well established.
- Blocked time is wall time across threads and cannot be converted to CPU cost or to throughput impact.
- PostgreSQL server-side execution is invisible to JFR; only the JDBC client side appears.
- Two profiling runs; cross-day throughput variance (1,320–1,454 today vs 1,875 on Day 12) remains unexplained and larger than within-day spread.
- P1 left 447 jobs QUEUED because the 60 s window closed before they were claimed — not a correctness failure, but it means P1's totals are over 9,553 completed jobs rather than 10,000.
- One worker remained registered after P0 and P1 (`active_workers=1`); deregistration did not complete for it. Recorded, not investigated.
- `settings=profile` allocation sampling is itself sampled; allocation figures are relative, not absolute rates.

## 18. Engineering Interpretation

The profile answers the Day 12 question about where control-plane CPU goes, and the answer is not where the service's own logic lives. Over 90% of CPU samples are on Tomcat request threads, and TaskMesh's own code is the executing frame in half a percent of them. Nearly half of all samples never reach application code at all — they are spent in Spring MVC dispatch, Tomcat HTTP handling, reflection and proxying before any TaskMesh method is entered. Reflection appears somewhere in 61.4% of stacks and Spring transaction/AOP proxying in 60.5%.

That rules several things out cleanly. GC accounts for 1.2 seconds of pause across a 90-second window. JSON serialisation, the Kafka producer, the Redis client and the outbox publisher are each in the low single digits. The reaper and outbox scheduler threads together account for at most 2.3%. None of these can explain the ceiling.

The most specific finding is a lock. `java.util.TimeZone.getTimeZone` synchronises on a class-level monitor, and Hibernate calls it on every `TIMESTAMPTZ` bind and extract — which on this schema means several times per claim, renewal and completion. It accounts for 70.5% of all monitor-enter events, and 97.7% of blocking happens on request threads. In P1 that is 203 seconds of blocked thread time inside a 90-second recording.

Care is needed in interpreting that number. Blocked time is wall time on parked threads, not CPU burn, so it does not appear in the CPU sample percentages and cannot be added to them. Its plausible effect is on latency and on how long request threads hold their database connections — which connects it to the Day 12 observation that ~156 threads queue behind 40 connections — but that chain is inference, not measurement. Its magnitude also varied by 3.4× between the two runs, so only its presence is firmly established.

Separately, the request mix shows that 27.4% of everything the control plane serves is an empty poll: a request that opens a transaction, checks the worker is active, runs the claim query, and returns 204 having accomplished nothing. That is a protocol characteristic rather than a code defect, and it was visible in the arithmetic from Day 8, but it is now measured under load.

No optimisation is recommended from this report. The framework overhead is the largest share but is diffuse and intrinsic; the lock contention is the most specific and most testable; the polling volume is the most structural. Establishing which of them actually constrains throughput requires changing one and measuring, which is Day 14.

**Potential resume metric — requires final review:** "Profiled a Spring Boot control plane under 5,000 logical workers with Java Flight Recorder, attributing 90% of CPU to request-handling threads and identifying a JDK-level lock in Hibernate's timestamp handling accounting for 70% of monitor contention in the hot path."
