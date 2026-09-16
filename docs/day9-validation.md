# Day 9 validation results

What was actually run, on the date of the Day 9 build. Every figure here came from an executed run; nothing is projected. See [`LOAD-GENERATOR.md`](LOAD-GENERATOR.md) for how to reproduce.

**These are not a scalability claim.** They are validation that the generator works and that TaskMesh's correctness invariants hold at the levels tested. Bottleneck analysis and optimization are explicitly out of scope for Day 9.

## Environment

Single host: Windows 11, 16 available processors. Control plane, PostgreSQL, Redis and Kafka in Docker Compose; the generator as a separate JVM on the **same machine**, competing for the same CPU. The real `taskmesh-worker` container was stopped so the generator supplied all workers.

This co-tenancy matters: generator, control plane and datastores share 16 cores, so control-plane figures here understate what dedicated hardware would show.

## Results

| Logical workers | Registered | Registration failures (retried) | Heartbeat success | Claims | Completed (DB) | Duplicate claims | Orphaned RUNNING | Generator CPU | Generator RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 100/100 | 0 | 810/810 | 400 | 400 | 0 | 0 | 4.8% | 182 MiB |
| 500 | 500/500 | 0 | 4090/4090 | 2000 | 2000 | 0 | 0 | 5.9% | 278 MiB |
| 1,000 | 1000/1000 | 1 | 8404/8406 | 3000 | 3000 | 0 | 0 | 5.0% | 332 MiB |
| 5,000 | 5000/5000 | 1834 | 22078/22113 | 2002 | 2000 | 0 | 0 | 6.6% | 626 MiB |

Registration failures are *retried attempts*, not workers lost: all workers at every level eventually registered.

Generator RSS was read from the OS (`Get-Process`), not from inside the JVM.

## Request rates

| Logical workers | Offered (calculated) | Achieved (measured) | Ratio |
|---:|---:|---:|---:|
| 100 | 120/sec | 134/sec | delivered |
| 500 | 600/sec | 659/sec | delivered |
| 1,000 | 1,200/sec | 1,206/sec | delivered |
| 5,000 | 6,000/sec | 1,465/sec | **24%** |

Achieved slightly exceeds offered at the lower levels because "offered" counts only heartbeats and polls, while achieved includes claims, renewals and completions.

At 100–1,000 the measured rate tracks the Day 8 model (≈1.2–1.3 × workers) closely. At 5,000 the generator delivered only 24% of the offered rate.

## Generator saturation

| Logical workers | Probe wake-up p99 | Worker scheduling delay p99 | Verdict |
|---:|---:|---:|---|
| 100 | — | 15.4 ms | Generator kept up |
| 500 | — | 206.5 ms | Generator kept up |
| 1,000 | 20.0 ms | 987 ms | Generator had headroom; **downstream latency** |
| 5,000 | 37.6 ms | 987 ms (pinned) | Generator had headroom; **downstream latency** |

The 100 and 500 runs predate the probe; their verdicts rest on scheduling delay alone.

At 1,000 and 5,000 the probe stayed healthy (20 ms and 37.6 ms) while generator CPU never exceeded 6.6%. **The generator was not the limiting factor at any level tested.** The shortfall at 5,000 is downstream.

Caveats, stated rather than buried:

- Worker scheduling delay **pins at ~987 ms** (the ~1 s cap). At 5,000 it is saturated at p50, p95 and p99 alike, so it cannot report how late things really were.
- What limits throughput at 5,000 has **not** been diagnosed. Day 8 raised hypotheses (Hikari pool at its default of 10, the empty-poll load, the serial outbox publisher); none has been measured, and this run does not test them.
- Co-tenancy on one host means the control plane was competing with the generator for CPU.

## Observed errors

| Logical workers | HTTP requests | Transport errors | Rate |
|---:|---:|---:|---:|
| 100 | 5,532 | 0 | 0% |
| 500 | 27,952 | 0 | 0% |
| 1,000 | 53,902 | 27 `ConnectException` | 0.05% |
| 5,000 | 62,446 | 2,041 `ConnectException` | 3.3% |

Registration latency degraded sharply with scale: p50 was 523 ms at 500 workers and **4,446 ms** at 5,000 (during a 500/sec ramp). No HTTP 5xx responses were returned at any level — the failures were connection-establishment failures, not application errors.

## Correctness

Verified in PostgreSQL after every run, not from generator counters:

| Invariant | 100 | 500 | 1,000 | 5,000 |
|---|---|---|---|---|
| No duplicate authoritative claims | 0 | 0 | 0 | 0 |
| Execution ids unique | 400/400 | 2000/2000 | 3000/3000 | 2002/2002 |
| All claimed jobs reached COMPLETED | 400 | 2000 | 3000 | 2000 |
| No orphaned RUNNING after shutdown | 0 | 0 | 0 | 0 |
| `attempt_count ≤ max_attempts` violations | 0 | 0 | 0 | 0 |
| Workers left registered after shutdown | 0 | 0 | 0 | 0 |
| Invalid lifecycle states | 0 | 0 | 0 | 0 |

At 5,000 workers, 2 jobs required a second attempt: their completion reports were lost to `ConnectException`, the leases lapsed, and the reaper reassigned them under new execution ids. Exactly 2 `JOB_RETRYING` events were recorded, and both jobs reached `COMPLETED`. **This is at-least-once execution working correctly under load, not a correctness failure** — and it is the behaviour TaskMesh documents.

All final logical worker states were `STOPPED` at every level, with zero executions still held by the generator at exit.

## Not run

Per the Day 9 scope: **10,000 and 25,000 logical workers were not benchmarked.** The module is written to support them; that is implementation support, not a measured result. No bottleneck was optimized and no TaskMesh configuration was changed.
