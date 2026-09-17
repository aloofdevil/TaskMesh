# Day 14 — TimeZone Contention Optimization Experiment

## 1. Objective

Test whether reducing the `java.util.TimeZone.getTimeZone` contention identified by Day 13 profiling produces a measurable, repeatable performance improvement under the 5,000-worker workload, while preserving UTC timestamp semantics exactly.

This is a single-hypothesis experiment. One variable changes.

## 2. Day 13 Hypothesis

Day 13 attributed 70.5% of all `JavaMonitorEnter` events to a single `java.lang.Class` monitor, traced to `java.util.TimeZone.getTimeZone` called from Hibernate's timestamp bind/extract, with 97.7% of the blocking on HTTP request threads and 203,499 ms of blocked thread time in a 90 s window. It was classified as the most specific and most testable candidate — explicitly **not** as a confirmed bottleneck.

## 3. Current Implementation

Traced from source, not assumed:

- All timestamp columns are `TIMESTAMPTZ`; all entity fields are `java.time.Instant`. No `@Temporal`, no converters, no `@TimeZoneStorage`.
- `application.yml` sets `hibernate.jdbc.time_zone: UTC`.
- A diagnostic test against the live Hibernate metamodel confirmed this selects **`TimestampUtcAsJdbcTimestampJdbcType`** for `Instant` attributes.
- Its bytecode (`javap -c`) shows per-bind:

```
Timestamp.from(Instant)
TimeZone.getTimeZone(String)      ← static synchronized in the JDK
Calendar.getInstance(TimeZone)    ← fresh allocation every bind
PreparedStatement.setTimestamp(int, Timestamp, Calendar)
```

- JDK 21 `TimeZone.java:548`: `public static synchronized TimeZone getTimeZone(String ID)`. Being `static synchronized`, it locks `TimeZone.class` — precisely the `java.lang.Class` monitor JFR reported. Every request thread doing any timestamp bind or extract serialises on one JVM-wide lock.

## 4. Exact Optimization Applied

One line added to `taskmesh-control-plane/src/main/resources/application.yml`:

```yaml
spring.jpa.properties.hibernate.type.preferred_instant_jdbc_type: TIMESTAMP_WITH_TIMEZONE
```

`hibernate.type.preferred_instant_jdbc_type` is a supported Hibernate setting that selects the JDBC type used for `Instant`. It resolves to `TimestampWithTimeZoneJdbcType`, whose bytecode contains **no `TimeZone` or `Calendar` references** and binds instead:

```
JavaType.unwrap(value, OffsetDateTime.class, options)
PreparedStatement.setObject(int, OffsetDateTime, TIMESTAMP_WITH_TIMEZONE)
```

`hibernate.jdbc.time_zone: UTC` was deliberately **left in place**. Removing it was considered and rejected: it has a wider blast radius (it governs all JDBC timestamp handling, not just `Instant`), and the Day 14 rules explicitly warn that removing it is not obviously semantics-preserving. The chosen setting is narrower — it targets exactly the `Instant` mapping that the contention comes from.

Two Dockerfiles were also repaired; see §19.

## 5. Why Semantic Equivalence Is Preserved

Argued *and* proven.

Both bindings write the same absolute instant to a `TIMESTAMPTZ` column: one via `Timestamp` + UTC `Calendar`, the other via `OffsetDateTime` at UTC. `TIMESTAMPTZ` stores an absolute point in time, so neither path involves a local-time interpretation.

Argument is not proof, so `TimestampSemanticsContract` pins both configurations against values PostgreSQL itself renders. It runs the **same four tests twice** — once forcing `TIMESTAMP_UTC` (the old binding), once with the new default — and asserts:

1. The expected `JdbcType` is actually selected (guards against a config regression silently invalidating the test).
2. Six edge-case instants round-trip through JPA unchanged: the epoch, both 2024 US DST transitions, microsecond precision at both ends of a second, and a far-future value.
3. The value PostgreSQL **actually stored**, rendered by PostgreSQL in UTC via raw SQL, equals an independently computed expected text. This bypasses Hibernate's extractor entirely, so a bug symmetric across bind and extract cannot hide behind a round trip.
4. Epoch microseconds match numerically, independent of text rendering.
5. `lease_until` — which drives reaper decisions — round-trips exactly.
6. `scheduled_at <= now()` comparisons against the database clock behave correctly for past and future jobs under both bindings.

The assertions are **absolute, not comparative**: each configuration is checked against independently derived expectations. Comparing the two configurations to each other could be satisfied by both being wrong in the same way.

**Result: 8/8 tests pass under both bindings** — and since the discovery fix described in §19, all 8 run as part of the default suite.

## 6. Experimental Setup

| Constant | Value |
|---|---|
| Logical workers | 5,000 |
| Jobs | 10,000 |
| Hikari max pool | 40 |
| Seed | 424242 |
| Ramp-up | 500/sec |
| Run duration | **90 s** (raised from Day 13's 60 s so all 10,000 jobs reach terminal state) |
| Worker capacity | 2 |
| JFR | `settings=profile, delay=45s, duration=110s, stackdepth=128` |

**Replicates: 4 per arm.** Two were run initially; because the throughput ranges overlapped, two more were added per arm rather than concluding from n=2.

Both arms run the **same image**. The baseline restores the old behaviour with a system property via the existing `CP_JAVA_OPTS` pass-through:

```
-Dspring.jpa.properties.hibernate.type.preferred_instant_jdbc_type=TIMESTAMP_UTC
```

Every run began with `docker compose down -v --remove-orphans` — fresh containers, fresh volumes, empty database, fresh Kafka.

## 7. Environment

Git `c9f272a`; Intel i5-12500H (12 physical / 16 logical), 15.7 GiB; Docker Desktop 16 CPUs / 7.602 GiB, WSL2; PostgreSQL 16.15, Redis 7.4.11, Kafka 4.2.1; control plane `eclipse-temurin:21-jre` (OpenJDK 21.0.12); Hibernate 7.4.5.Final; Spring Boot 4.1.1.

## 8. Baseline Results

| Run | Achieved req/s | Complete-10k after | Probe p99 | CP CPU | PG CPU |
|---|---:|---:|---:|---:|---:|
| base_r1 | 1,757 | 63 s | 36.9 ms | 821% | 516% |
| base_r2 | 1,875 | 66 s | 36.1 ms | 866% | 537% |
| base_r3 | 1,782 | 65 s | 28.5 ms | 837% | 598% |
| base_r4 | 1,962 | 61 s | 32.1 ms | 883% | 654% |
| **mean** | **1,844** | **63.8 s** | 33.4 ms | 852% | 576% |

All four baseline runs completed 10,000/10,000 jobs inside the 90 s window.

## 9. Optimized Results

| Run | Achieved req/s | Complete-10k after | Probe p99 | CP CPU | PG CPU |
|---|---:|---:|---:|---:|---:|
| opt_r1 | 1,860 | 62 s | 28.5 ms | 864% | 487% |
| opt_r2 | 1,944 | 60 s | 29.7 ms | 853% | 502% |
| opt_r3 | 2,000 | 60 s | 25.8 ms | 798% | 559% |
| opt_r4 | 1,596 | 90 s | **76.7 ms** | 722% | 360% |
| **mean** | **1,850** | **68.0 s** | 40.2 ms | 809% | 477% |

All four optimized runs also completed 10,000/10,000 jobs.

## 10. Replicate-to-Replicate Variation

| Arm | Range | Spread |
|---|---|---:|
| Baseline | 1,757 – 1,962 req/s | 11.7% |
| Optimized | 1,596 – 2,000 req/s | 25.3% |

The ranges overlap almost completely. Any difference below roughly 12% cannot be resolved by this experiment.

`opt_r4` is anomalous on grounds independent of the configuration under test: its generator probe p99 was 76.7 ms against 25–37 ms everywhere else (approaching the 100 ms saturation threshold), while its container CPU was abnormally *low* (control-plane 722%, PostgreSQL 360%, versus ~800–880% / ~490–650%). Low CPU combined with degraded scheduling indicates external host interference, not an effect of the binding.

Excluding it on that stated criterion: optimized mean 1,935 req/s versus baseline 1,844 — **+4.9%**, still inside the baseline arm's own 11.7% spread. Both analyses are reported; neither is chosen to favour a conclusion.

## 11. JFR Comparison

Same JFR methodology as Day 13 (`settings=profile`, `delay=45s`, `duration=110s`, `stackdepth=128`), applied identically to **both** arms, so it cannot bias the comparison.

The recordings confirm the JdbcType actually switched — this is the check that caught an invalid first round (§19):

| CPU samples containing | base_r1 | base_r2 | base_r3 | base_r4 | mean | opt_r1 | opt_r2 | opt_r3 | opt_r4 | mean |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `TimestampUtcAsJdbcTimestampJdbcType` (Calendar path) | 103 | 100 | 90 | 112 | **101** | 7 | 2 | 1 | 6 | **4** |
| `TimestampWithTimeZoneJdbcType` (OffsetDateTime path) | 0 | 0 | 0 | 0 | **0** | 75 | 76 | 75 | 70 | **74** |

The old path effectively disappears and the new one appears, in every optimized replicate and in none of the baselines.

## 12. TimeZone.getTimeZone() Comparison

Every `java.lang.Class` monitor event in all eight recordings resolved to `java.util.TimeZone.getTimeZone` in the stack, so the two counts are identical.

| Run | `TimeZone.getTimeZone` contention events |
|---|---:|
| base_r1 | 1,527 |
| base_r2 | 2,272 |
| base_r3 | 2,037 |
| base_r4 | 1,129 |
| **baseline mean** | **1,741** |
| opt_r1 | 20 |
| opt_r2 | 5 |
| opt_r3 | 10 |
| opt_r4 | 5 |
| **optimized mean** | **10** |

**−99.4%, with completely non-overlapping ranges**: baseline [1,129–2,272] versus optimized [5–20]. The intervention did exactly what it was designed to do.

The residual 5–20 events are expected: only `Instant` mapping was changed, and other Hibernate timestamp paths still exist.

## 13. Monitor Contention Comparison

| Metric | Baseline mean | Optimized mean | Δ | Ranges |
|---|---:|---:|---:|---|
| Total `JavaMonitorEnter` events | 3,461 | 1,908 | **−44.9%** | [2,805–4,071] vs [1,454–2,429] — non-overlapping |
| Total monitor blocked time | 131,619 ms | 89,876 ms | −31.7% | [114,436–157,905] vs [58,602–148,953] — overlapping |

Total monitor-enter events are non-overlapping between arms. Blocked time overlaps solely because of `opt_r4`'s 148,953 ms; the other three optimized runs sit at 58,602–79,115 ms against a baseline floor of 114,436 ms.

`ThreadPark` events were 184,426–223,552 across all runs with no systematic difference between arms — request threads still spend most of their time waiting, as on Day 13.

## 14. Request Latency Comparison

Mean of four replicates per arm:

| Metric | Baseline | Optimized | Δ |
|---|---:|---:|---:|
| Registration p50 | 5,684 ms | 5,952 ms | +4.7% |
| Registration p95 | 7,572 ms | 7,678 ms | +1.4% |
| Heartbeat p50 | 1,880 ms | 1,862 ms | −1.0% |
| Heartbeat p95 | 5,447 ms | 5,691 ms | +4.5% |
| Heartbeat p99 | 6,354 ms | 6,803 ms | +7.1% |
| Poll p50 | 1,789 ms | 1,789 ms | 0% |
| Poll p95 | 5,136 ms | 5,218 ms | +1.6% |
| Poll p99 | 6,484 ms | 6,878 ms | +6.1% |
| Completion p50 | 3,307 ms | 3,130 ms | −5.4% |
| Completion p95 | 5,694 ms | 5,245 ms | **−7.9%** |
| Completion p99 | 6,013 ms | 5,935 ms | −1.3% |
| Renewal p95 | 6,047 ms | 6,121 ms | +1.2% |

Directionally **mixed** — completion latency improves while heartbeat and poll tails worsen slightly — and every difference sits well inside replicate spread. Presented in full rather than selecting the favourable rows.

## 15. Hikari Comparison

| Metric | Baseline mean | Optimized mean | Δ |
|---|---:|---:|---:|
| Connection acquire | 79.0 ms | 79.2 ms | +0.3% |
| Connection hold (usage) | 20.9 ms | 20.9 ms | 0% |
| `taskmesh.jobs.claim` timer | 8.09 ms | 7.79 ms | −3.7% |
| Max pending threads | 158.5 | 157.8 | −0.4% |
| Max active | 40 / 40 | 40 / 40 | — |

**This is the most informative table in the experiment.** Day 13 hypothesised that lock contention might be inflating how long request threads hold their database connections. Removing 99.4% of that contention left acquire time and hold time *identical*, with ~158 threads still queued behind 40 connections in both arms. That hypothesis is not supported.

## 16. PostgreSQL Comparison

| Metric | Baseline mean | Optimized mean | Δ |
|---|---:|---:|---:|
| PostgreSQL CPU (peak) | 576% | 477% | −17.2% |
| Max active sessions | 25.8 | 23.8 | −7.8% |
| Lock waits | 0 | 0 | — |
| Control-plane CPU (peak) | 852% | 809% | −5.0% |

PostgreSQL behaviour remains comparable; both CPU figures sit inside the range seen across Days 11–13 and are pulled down partly by `opt_r4`'s abnormally low utilisation. Lock waits remained zero in every run, as in every experiment since Day 10.

**Redis** was unaffected: ~15 µs per SET, 0 failed calls, CPU in the usual low-double-digit range. **Kafka and the outbox** were likewise unchanged — backlog after each run was 31,400–32,031 unpublished events in both arms, draining at ~100 events/sec, identical to every run since Day 10 and independent of this change.

## 17. Throughput Comparison

| | Baseline | Optimized | Δ |
|---|---:|---:|---:|
| All four replicates | 1,844 req/s | 1,850 req/s | **+0.3%** |
| Excluding `opt_r4` | 1,844 req/s | 1,935 req/s | +4.9% |

Against a baseline replicate spread of 11.7%, neither figure separates the arms. Throughput did not measurably improve.

## 18. Completion-Time Comparison

| | Baseline | Optimized | Δ |
|---|---:|---:|---:|
| All four replicates | 63.8 s | 68.0 s | +6.6% (worse) |
| Excluding `opt_r4` | 63.8 s | 60.7 s | −4.8% |

The full-dataset figure is worse and the outlier-excluded figure is better, both by less than replicate spread. This measure is also unable to separate the arms — which is precisely why it was recorded alongside fixed-window throughput, per the Day 14 benchmark correction.

## 19. Correctness Results

All 8 runs: **10,000 / 10,000 jobs COMPLETED**, duplicate claims **0**, execution-id collisions **0** (distinct ids always equal attempts), orphaned RUNNING **0**, over-budget attempts **0**, DLQ **0**, invalid state transitions **0**. Reassignments were 0–23 across runs, all recovered. `opt_r4` left one worker registered (`active_workers=1`).

Full suite: **148 tests, 0 failures, 0 errors, 0 skipped** (control-plane 113, worker 1, load generator 34), verified from the Surefire XML reports rather than console output. No existing test was weakened or removed.

**Test-discovery correction.** As originally written, the timestamp tests lived in two `static` nested classes inside `TimestampSemanticsTest`. Surefire's default excludes drop inner classes (`**/*$*`), so they were never run by `mvn verify` — they passed only when targeted explicitly with `-Dtest`, which is how the 8/8 result in §5 was obtained. Renaming would not have helped, because the exclusion is on the nesting, not the name. They are now three top-level test sources — `TimestampSemanticsContract` (abstract, holds the four `@Test` methods), `TimestampCalendarBindingTest` and `TimestampOffsetDateTimeBindingTest` — so the suite discovers and executes **8 timestamp semantic test cases** (4 methods × 2 bindings) on every run. This was a test-layer change only; no production code, build configuration, or test logic was altered, and the earlier count of 137 was wrong (the correct figure at the time was 135, because these tests contributed nothing to the suite).

### A methodological error found and corrected mid-experiment

The first complete A/B round was **discarded as invalid**. Its JFR showed the Calendar-based JdbcType still in use in *both* arms. Investigation found the control-plane image was 24 hours old: `docker compose build` had been failing since Day 9, when `taskmesh-loadgen` was added to the parent POM without adding `COPY taskmesh-loadgen/pom.xml` to the Dockerfiles. The Maven reactor could not resolve the missing module, so `dependency:go-offline` exited 1 and the build never produced a new image.

Both Dockerfiles were repaired (one `COPY` line each), the image rebuilt, and the presence of the setting verified by extracting `BOOT-INF/classes/application.yml` from the built jar before re-running everything.

This does not affect Days 10–13: no production code changed in those days, and their configuration was injected at runtime through environment variables, which work against any image. It does mean the repository could not build a fresh control-plane or worker image between Day 9 and today.

## 20. Interpretation

The experiment cleanly separates two questions that are easy to conflate.

**Did the intervention remove the contention? Yes, decisively.** `TimeZone.getTimeZone` monitor events fell from a mean of 1,741 to 10 — a 99.4% reduction with completely non-overlapping ranges across four replicates per arm. Total monitor-enter events fell 44.9%, also non-overlapping. CPU samples confirm the Calendar-based binding was replaced by the OffsetDateTime one in every optimized run. Day 13's profiling was accurate and the chosen lever was the right one for the mechanism.

**Did removing it improve throughput? No, not measurably.** Achieved throughput moved from 1,844 to 1,850 req/s — 0.3%, against a baseline replicate spread of 11.7%. Even discarding the one anomalous optimized run on independent grounds (generator probe degradation and abnormally low CPU), the gap is 4.9%, still inside that spread. Completion time and latency percentiles are directionally mixed and equally inconclusive.

The most informative single observation is that Hikari behaviour did not move at all: acquire time 79.0 → 79.2 ms, hold time unchanged, and ~158 threads still queued behind 40 connections in both arms. The Day 13 report hypothesised that lock contention might be inflating how long request threads hold connections. That hypothesis is **not supported** — removing 99.4% of the contention left connection hold time identical.

This is a useful negative result. The contention was real, was in the hot path, and has been eliminated; it simply was not what limited throughput. Blocked time is wall time on parked threads, and while a thread is parked on that monitor it is not consuming CPU — so removing it frees a thread's wall-clock time without freeing the resource that was actually scarce. Day 12 showed throughput is bound by control-plane CPU, and Day 13 showed 46.9% of CPU samples never reach application code at all. Nothing in today's data contradicts that picture.

**Whether to keep the change is a reviewer decision, and I am not making it here.** In favour: it is proven semantics-preserving by absolute assertions against PostgreSQL, it removes a genuine JVM-wide lock from the hot path, it eliminates a per-bind `Calendar` allocation, and it shows no measured regression. Against: it produced no measurable improvement, so by the project's own discipline it is a change without demonstrated benefit. Reverting it is a one-line deletion; the `TimestampSemanticsTest` is worth keeping either way, since it pins timestamp behaviour that nothing else covered.

## 21. Hypothesis Classification

### NOT SUPPORTED

The intervention verifiably reduced the targeted contention by 99.4%, but produced **no repeatable performance improvement**: the measured throughput effect (+0.3% across all replicates, +4.9% excluding one anomalous run) is within experimental noise, given a baseline replicate spread of 11.7%.

To be precise about what is and is not being claimed:

- **Contention reduction: demonstrated** (non-overlapping ranges, four replicates per arm).
- **Semantic equivalence: demonstrated** (8/8 test cases asserting against values PostgreSQL actually stores).
- **Performance improvement: not demonstrated.**

The hypothesis under test was whether reducing the contention *produces a measurable, repeatable performance improvement*. It does not.

## 22. Remaining Bottleneck Candidates

Day 13 offered three candidates. Candidate 1 has now been tested and eliminated as a throughput constraint. Remaining, in order of evidential support:

1. **Per-request framework overhead.** 46.9% of CPU samples never reach TaskMesh code; reflection appears in 61.4% of stacks and Spring transaction/AOP proxying in 60.5%. The largest measured share, but diffuse.
2. **Non-productive polling volume.** 27.4% of all requests are empty polls that still open a transaction and run two queries. A protocol-level question rather than a micro-optimisation.
3. **Connection-pool queueing itself.** ~158 threads queue for 40 connections in both arms, with acquire time (79 ms) still an order of magnitude above hold time (21 ms). Day 11 showed widening the pool past 40 stops helping, and Day 12 could not separate PostgreSQL pressure from host CPU saturation.

## 23. Limitations

- Four replicates per arm. A 5% effect cannot be resolved against an 11.7% within-arm spread; this experiment can only detect fairly large throughput effects.
- `opt_r4` is anomalous on independent grounds (probe p99 76.7 ms, abnormally low CPU). It is reported in all aggregates and also analysed separately; excluding it is a judgement call, disclosed rather than hidden.
- Single host; generator co-resident with the full stack, as in all previous days.
- Blocked time is wall time across threads and cannot be converted to CPU cost or throughput impact.
- JFR overhead was not separately quantified today; Day 13 found it below the noise floor and the same settings were used in **both** arms, so it cannot bias the comparison.
- Cross-day throughput variance remains large and unexplained (Day 12 A: 1,875; Day 13: 1,320–1,454; today's baseline: 1,757–1,962).
- Only `Instant` mapping was changed. Other Hibernate timestamp paths are untouched, which is why a residual 5–20 `java.lang.Class` events remain.

## 24. Reproduction

```bash
# Build (requires the Dockerfile fix from this day)
./mvnw clean verify
docker compose build control-plane

# Verify the setting is in the image
CID=$(docker create taskmesh-control-plane:latest)
docker cp "$CID":/app/app.jar ./app.jar && docker rm -f "$CID"
unzip -p app.jar BOOT-INF/classes/application.yml | grep preferred_instant

# OPTIMIZED arm (application.yml default)
HIKARI_MAX_POOL_SIZE=40 CP_JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -Dnetworkaddress.cache.ttl=10 \
  -XX:FlightRecorderOptions=stackdepth=128 \
  -XX:StartFlightRecording=settings=profile,delay=45s,duration=110s,filename=/tmp/cp.jfr" \
  docker compose up -d

# BASELINE arm (same image, old binding restored)
HIKARI_MAX_POOL_SIZE=40 CP_JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -Dnetworkaddress.cache.ttl=10 \
  -Dspring.jpa.properties.hibernate.type.preferred_instant_jdbc_type=TIMESTAMP_UTC \
  -XX:FlightRecorderOptions=stackdepth=128 \
  -XX:StartFlightRecording=settings=profile,delay=45s,duration=110s,filename=/tmp/cp.jfr" \
  docker compose up -d

# Both arms: stop bundled worker, submit 10k jobs, then
java -jar taskmesh-loadgen/target/taskmesh-loadgen-0.0.1-SNAPSHOT.jar \
  --workers=5000 --ramp-up=500 --seed=424242 --run-duration=90s

# Contention comparison
jfr print --events jdk.JavaMonitorEnter cp.jfr | grep -c "monitorClass = java.lang.Class"
jfr print --events jdk.ExecutionSample --stack-depth 64 cp.jfr | grep -c "TimestampWithTimeZoneJdbcType"
```
