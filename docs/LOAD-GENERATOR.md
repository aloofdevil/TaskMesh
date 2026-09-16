# TaskMesh logical worker load generator

`taskmesh-loadgen` represents thousands of independent TaskMesh workers from a **single OS process**, so the control plane can be benchmarked at worker counts that would be impossible to reach with containers.

It is a **load generator**, not a replacement for the production worker. `taskmesh-worker` remains the real implementation; nothing in this module is on its classpath, and the production worker was not modified to support it.

## What a logical worker is

A **logical worker** is a simulated worker *identity* that behaves like a real one. Each has its own:

- unique worker id, registered with the control plane
- lifecycle state (`CREATED → REGISTERING → ACTIVE → DRAINING → STOPPED`, or `FAILED`)
- heartbeat, polling and lease-renewal schedules, each independently jittered
- advertised capacity and in-flight execution set
- execution ids and lease state, including fenced state

It speaks the **same HTTP protocol** as the real worker — the same endpoints, the same `taskmesh-common` request/response records. The simulator depends on `taskmesh-common` precisely so it cannot drift from the production contract.

### Why logical workers instead of real ones

A real worker is a JVM in a container. At 25,000 workers that is 25,000 JVMs — hundreds of gigabytes of heap and far more CPU than the control plane under test. The bottleneck would be the test harness, and nothing would be learned about TaskMesh.

What the control plane actually sees from a worker is a *stream of HTTP requests on a schedule*. A logical worker reproduces that faithfully at a tiny fraction of the cost.

## Counting categories — never collapse these

The single most important discipline in this benchmark. These are different numbers and are reported separately:

| Category | Meaning | At 5,000 logical workers (measured) |
|---|---|---|
| **Logical workers** | Simulated worker identities | 5,000 |
| **Virtual threads** | JDK scheduling construct driving them, 1:1 | 5,000 |
| **Carrier/platform threads** | OS threads underneath | ~16 carrier, 25 platform |
| **OS processes** | — | **1** |
| **Containers/pods** | For logical workers | **0** |
| **Job slots** | `capacity × workers` — a **ceiling** | 10,000 |
| **Actual concurrent jobs** | Jobs `RUNNING` with a live lease | Measured in PostgreSQL, **never** assumed equal to slots |
| **Requests offered** | Calculated from config | 6,000/sec |
| **Requests achieved** | Measured | 1,465/sec |

"5,000 workers" never means 5,000 threads, processes, containers, or concurrent jobs. A job slot ceiling is not a measurement of concurrency — the authoritative concurrent-job count comes from the database, not from generator counters.

## Concurrency model

**One virtual thread per logical worker**, not one per scheduled activity.

The real `WorkerRuntime` has three `@Scheduled` methods plus an execution pool. Mirroring that literally would cost four threads per worker — 100,000 threads at the 25,000 target. Instead each logical worker runs a **deadline loop** on one virtual thread, tracking four kinds of deadline (heartbeat, poll, lease renewal, and each execution's finish time), sleeping until the earliest and acting on whatever is due.

Virtual threads were chosen over an event-driven state machine or async HTTP because the TaskMesh protocol is **synchronous request/response**. A blocking call on a virtual thread parks the thread and releases its carrier, so the sequencing stays identical to the real worker while thousands of simultaneously-blocked workers remain affordable. An event-driven rewrite would have meant hand-rolling the worker state machine, risking a simulator that measures a *different* protocol than production.

Consequences, all deliberate:

- HTTP calls block the loop, exactly as they occupy a scheduler thread in the real worker.
- Because calls block, other deadlines can slip — real worker behaviour, and measured rather than hidden.
- Polling stops while `inFlight == capacity`, as `pollForWork()` does.
- A fenced execution is abandoned silently: no completion, no failure report.

Simulated work is a **deadline, not a sleep and not a busy loop** — the point is to exercise the protocol, not burn generator CPU that would then be mistaken for control-plane cost.

## Jitter

Without jitter, every worker fires on the same absolute boundaries: 25,000 workers would deliver one enormous burst per second instead of a steady rate, and the benchmark would measure a thundering herd.

Each logical worker gets an independent uniform phase offset across each interval, drawn from a **seeded** RNG. Passing the same `--seed` reproduces the same offsets, so runs are comparable.

## Telling generator limits from control-plane limits

The critical distinction. Two independent instruments:

- **Worker scheduling delay** — how late each due action started. Rises when the fleet is not keeping to schedule, *for either reason*.
- **Scheduler probe** — one virtual thread that only sleeps on a fixed 100 ms interval and measures its own wake-up lateness. It **issues no requests**, so its lateness cannot be caused by the control plane.

The verdict logic follows from that:

| Probe | Worker delay | Verdict |
|---|---|---|
| on time | on time | Generator kept up; latencies belong to the control plane |
| on time | late | **Downstream latency** pushing workers late — not generator saturation |
| late | late | **GENERATOR-SATURATED** — latency figures are contaminated |

This distinction was not theoretical. At 1,000 workers the naive "scheduling delay is high ⇒ generator saturated" rule fired while generator CPU was only 3.7% — a misattribution. The probe showed p99 wake-up of 20 ms against a worker scheduling delay of 987 ms, correctly identifying downstream latency.

**Known limitation:** worker scheduling delay is capped at ~1 s by design (a deadline cannot fall more than 1 s into the past, which prevents catch-up bursts). Once it pins at ~987 ms the instrument is saturated and cannot report *how* late things were. The probe is unaffected.

## Running it

Build (from the repository root):

```bash
./mvnw -pl taskmesh-loadgen -am -DskipTests install
```

Start the stack, then stop the real worker so the generator supplies the workers:

```bash
docker compose up -d
docker compose stop worker
```

Submit some jobs, then run a level:

```bash
# 100 logical workers
java -jar taskmesh-loadgen/target/taskmesh-loadgen-0.0.1-SNAPSHOT.jar \
  --workers=100 --ramp-up=100 --seed=1001 --run-duration=40s

# 500
java -jar taskmesh-loadgen/target/taskmesh-loadgen-0.0.1-SNAPSHOT.jar \
  --workers=500 --ramp-up=250 --seed=2002 --run-duration=40s

# 1000
java -jar taskmesh-loadgen/target/taskmesh-loadgen-0.0.1-SNAPSHOT.jar \
  --workers=1000 --ramp-up=250 --seed=3003 --run-duration=40s
```

The generator is **not** part of the Docker Compose stack, by design: `docker compose up` must never start thousands of simulated workers.

### Options

| Flag | Default | Notes |
|---|---|---|
| `--workers` | 100 | logical workers |
| `--capacity` | 2 | job slots per worker (production value) |
| `--heartbeat-interval` | 5s | production value |
| `--poll-interval` | 1s | production value |
| `--lease-renew-interval` | 10s | production value |
| `--job-duration` | 2s | matches the production stand-in |
| `--ramp-up` | 100/sec | `0` = start all at once |
| `--control-plane-url` | `http://localhost:8080` | |
| `--seed` | nanoTime | set it for reproducible runs |
| `--run-duration` | 60s | after ramp-up completes |
| `--request-timeout` | 10s | |

Durations accept `500ms`, `5s`, `2m`, or bare milliseconds. Timing defaults are the **production** values from `taskmesh-worker/src/main/resources/application.yml`; overriding them is allowed but the effective configuration is printed and recorded with every run.

### Ramp-up

Workers start at `--ramp-up` per second. This matters: starting 25,000 workers simultaneously measures a registration stampede rather than steady-state behaviour. The burst is available (`--ramp-up=0`) but must be chosen deliberately.

## Interpreting the report

Read it in this order:

1. **Generator verdict** — if it says `GENERATOR-SATURATED`, stop; every latency below is contaminated and says nothing about the control plane.
2. **Counting categories** — check offered vs achieved. A large gap with a healthy probe means the control plane could not absorb the offered rate.
3. **Per-activity sections** — heartbeats, polling, execution, leases, HTTP.
4. **Resources** — generator CPU and heap. Process RSS is reported as `NOT MEASURED`: it is not observable from inside the JVM and must be read from the OS.

Generator counters describe what *this process* offered and observed. **Job outcomes must be verified in PostgreSQL** — a load generator that grades its own homework is not evidence. Every validation run checks the same invariants directly:

```sql
select (select count(*) from jobs where status='COMPLETED') completed,
       (select count(*) from jobs where status='RUNNING') still_running,
       (select count(*) from job_attempts) attempts,
       (select count(distinct execution_id) from job_attempts) distinct_exec,
       (select count(*) from (select job_id, attempt_number from job_attempts
          group by job_id, attempt_number having count(*)>1) x) dup_claims;
```

## What is not claimed

Implementation support is not benchmark success. The module is written to reach 25,000 logical workers, but see [`docs/day9-validation.md`](day9-validation.md) for what has actually been run and at what level. No level above those figures has been measured, and no performance optimization of TaskMesh has been attempted.
