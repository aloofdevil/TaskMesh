# Day 12 — Resource Isolation Experiment

## 1. Objective

Day 11 confirmed that HikariCP's default pool of 10 limited throughput, and that widening it to 40 raised throughput ~51%. It could not determine what limits throughput *after* that: PostgreSQL pressure, control-plane CPU, or host CPU contention from co-locating the load generator.

Day 12 answers that question by measurement, at a fixed Hikari pool of 40.

## 2. Day 11 Motivation — and a correction to it

Day 11 flagged host CPU as a confound because summed container CPU reached 1285–1623% of the 1600% available. It suggested moving the load generator off the machine to resolve this.

**Inspecting the Day 11 raw data first showed that reasoning was misdirected.** Converting both measurement systems into a common unit at pool 40:

| Component | CPU (docker-stats units, 100% = 1 core) |
|---|---:|
| control-plane | 746%, 739% |
| postgres | 572%, 594% |
| kafka | 67%, 86% |
| redis | 10%, 7% |
| **containers total** | **1394%, 1426%** |
| **load generator** | **75%, 82%** (0.75–0.82 cores) |
| total | 1469%, 1508% of 1600% (92%, 94% of host) |

The generator accounts for roughly **5% of total CPU consumption**. Relocating it to a second machine would free about 0.8 of 16 cores and move host utilisation from ~93% to ~88%. That cannot explain a throughput plateau.

The host CPU confound is real, but it is produced by **control-plane (746%) + PostgreSQL (572%)**, not by the generator. The experiment was designed accordingly.

## 3. Experimental Design

`A` and `B` are the co-resident/separated comparison originally requested. Because §2 showed that comparison can only move a ~5% variable, three CPU-budget configurations were added to answer the actual causal question — which component throughput is sensitive to.

| Run | Setup | Stack cpuset | Generator affinity | PG budget | CP budget |
|---|---|---|---|---:|---:|
| **A** | Co-resident (Day 11 conditions) | 0–15 | unpinned | 16 | 16 |
| **B** | Resource-separated | 0–13 | pinned to 14–15 | 14 | 14 |
| **C** | PG headroom | 0–15 | unpinned | 10 | 16 |
| **D** | PG constrained | 0–15 | unpinned | 4 | 16 |
| **E** | CP constrained | 0–15 | unpinned | 16 | 4 |

`D` and `E` apply a comparable CPU cut (−42% / −47% against observed usage) to one component at a time. Throughput sensitivity identifies which component is on the critical path — the diagnosis that isolation was meant to provide.

Two replicates each, 10 runs total.

**Was true isolation possible? No.** Docker Desktop is allocated all 16 logical CPUs — the same 16 the generator uses. There is no second machine, and provisioning paid cloud infrastructure was out of scope. `B` is a local approximation using cpuset + Windows processor affinity, and §11 shows it failed for a reason worth recording.

## 4. Environment

| Item | Value |
|---|---|
| Git commit | `f99fc12` |
| CPU | Intel i5-12500H — 12 physical / 16 logical |
| RAM | 15.7 GiB |
| OS | Windows 11 Home 10.0.26200 |
| Docker Desktop | **16 CPUs**, 7.602 GiB, WSL2 backend (kernel 6.6.87.2) |
| PostgreSQL / Redis / Kafka | 16.15 / 7.4.11 / 4.2.1 |
| Control plane | `taskmesh-control-plane:latest`, 1 Compose instance |
| Load generator | `taskmesh-loadgen-0.0.1-SNAPSHOT`, host JVM (outside Docker) |

Held constant: 5,000 logical workers, 10,000 jobs, Hikari 40, seed 424242, ramp 500/sec, 60 s duration, capacity 2, and all job/heartbeat/poll/lease/reaper/outbox timing. No application logic, SQL, schema, index, Redis, Kafka, outbox, Tomcat, or scheduler change.

## 5. Resource Configuration

CPU budgets were applied through an opt-in Compose overlay, `docker-compose.day12-cpu.yml`, never loaded by a plain `docker compose up`:

```yaml
services:
  postgres:
    cpus: ${PG_CPUS:-16}
    cpuset: ${STACK_CPUSET:-0-15}
  redis:
    cpuset: ${STACK_CPUSET:-0-15}
  kafka:
    cpuset: ${STACK_CPUSET:-0-15}
  control-plane:
    cpus: ${CP_CPUS:-16}
    cpuset: ${STACK_CPUSET:-0-15}
```

This allocates **host CPU to containers**. It changes no PostgreSQL, Redis or Kafka setting. Applied limits were read back from `docker inspect` per run and recorded in each run's metadata.

Generator affinity in `B` was set with `Process.ProcessorAffinity = 0xC000` (CPUs 14–15) after JVM start, and the applied value logged.

## 6. Commands

```bash
docker compose -f docker-compose.yml -f docker-compose.day12-cpu.yml down -v --remove-orphans
HIKARI_MAX_POOL_SIZE=40 STACK_CPUSET=<set> PG_CPUS=<n> CP_CPUS=<n> \
  docker compose -f docker-compose.yml -f docker-compose.day12-cpu.yml up -d
# wait for readiness; assert hikaricp.connections.max == 40; stop bundled worker
java Submit.java 10000 <label> http://localhost:8080 64
java -jar taskmesh-loadgen/target/taskmesh-loadgen-0.0.1-SNAPSHOT.jar \
  --workers=5000 --ramp-up=500 --seed=424242 --run-duration=60s
```

## 7. Results

Means of two replicates.

| Run | Setup | Workers | Jobs | Hikari | Achieved req/s | Generator CPU | CP CPU | PG CPU |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A | co-resident | 5000 | 10000 | 40 | **1,875** | 5.0–5.9% | 802% | 677% |
| B | separated *(invalid)* | 5000 | 10000 | 40 | 1,627 | 5.6–6.2% | 777% | 507% |
| C | PG 10 cores | 5000 | 10000 | 40 | **1,797** | 6.5–6.8% | 754% | 753% |
| D | PG 4 cores | 5000 | 10000 | 40 | **1,597** | 5.2–6.3% | 793% | 412% |
| E | CP 4 cores | 5000 | 10000 | 40 | **759** | 3.8–6.8% | 424% | 288% |

| Run | Acquire mean | Usage mean | Claim mean | DB sessions (peak) | Hikari active/pending | Errors | Reassignments |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 93.8 ms | 25.56 ms | 8.34 ms | 27–32 | 39–40 / 159 | 3.1% | 0, 0 |
| B | 76.5 ms | 21.42 ms | 8.38 ms | 14–21 | 40 / 156–160 | 1.8% | 0, 3 |
| C | 95.3 ms | 25.75 ms | 8.25 ms | 28–35 | 40 / 157–158 | 4.2% | 0, 0 |
| D | 106.1 ms | 28.37 ms | 8.64 ms | 16–29 | 40 / 156 | 3.7% | 0, 1 |
| E | **0.9 ms** | 34.17 ms | **42.81 ms** | 1 | **1 / ~0** | 6.0% | 252, 521 |

Replicate spread: A 4%, B 4%, C 2%, D 9%, E 7% — tighter than Day 11's 9–17%, but differences under ~10% remain indistinguishable.

**Cross-day note:** run A reproduces Day 11's pool-40 configuration exactly and achieved 1,875 req/s versus Day 11's 1,539 — a 22% gap larger than within-day variance. Day-to-day machine state is therefore a real source of variation, and Day 11 and Day 12 numbers should not be compared directly.

## 8. Host Resource Accounting

Two measurement systems are involved and are **not** interchangeable:

- **`docker stats` CPUPerc** — 100% ≈ one fully utilised core. With 16 cores, capacity is 1600%.
- **Generator `process.cpu.usage`** (JDK `OperatingSystemMXBean.getProcessCpuLoad`) — a fraction of the *whole machine*. 5% ≈ 0.8 cores ≈ 80% in docker-stats units.

Converted to the common docker-stats unit:

| Run | Containers | Generator | Total | % of 1600% |
|---|---:|---:|---:|---:|
| A | 1,526% | ~87% | ~1,613% | **101%** |
| B | 1,325% | ~94% | ~1,419% | 89% |
| C | 1,558% | ~106% | ~1,664% | **104%** |
| D | 1,252% | ~92% | ~1,344% | 84% |
| E | 763% | ~85% | ~848% | 53% |

Windows Task Manager percentages were not combined with these and are not reported.

## 9. PostgreSQL Behavior

The budget sweep is the core evidence:

| PG budget | PG CPU used | Achieved req/s | Δ throughput vs A |
|---:|---:|---:|---:|
| 16 (unlimited) | 677% | 1,875 | — |
| 10 | 753% | 1,797 | −4% (within variance) |
| 4 | 412% (capped) | 1,597 | **−15%** |

Cutting PostgreSQL's CPU by ~42% (6.9 → 4 cores) cost only **15%** of throughput. Raising its ceiling from 10 to 16 cores changed nothing measurable. Lock waits were **0** in every run. Claim-timer values barely moved across A/C/D (8.25–8.64 ms) despite PG CPU ranging 412–753%.

That sensitivity is markedly sublinear. A binding constraint would produce roughly proportional loss.

## 10. Control Plane Behavior

The symmetric cut tells a different story:

| CP budget | CP CPU used | Achieved req/s | Δ throughput vs A |
|---:|---:|---:|---:|
| 16 (unlimited) | 802% | 1,875 | — |
| 4 | 424% (capped) | 759 | **−60%** |

A comparable CPU cut (−47%) applied to the control plane cost **four times** as much throughput as the same cut applied to PostgreSQL.

Run `E` is also diagnostically clean in a second way. With the control plane starved:

- Hikari **active fell to 1**, pending to ~0, and acquisition to **0.9 ms** (from 93.8 ms).
- PostgreSQL CPU fell to 288% and sessions to 1.
- The `taskmesh.jobs.claim` timer *rose* to 42.81 ms.

The control plane could not generate enough concurrent work to use its own pool. The pool stopped being a wait point entirely and the constraint moved wholly inside the control-plane JVM.

**Measurement caveat this exposed:** `taskmesh.jobs.claim` wraps the entire `DispatchService.claim()` method — `requireActive`, connection acquisition, transaction management and SQL. It is a control-plane-side timer, **not** a PostgreSQL query timer. Its 5× rise in `E` occurred while PostgreSQL CPU *fell*, which proves it carries control-plane time. Day 11's observation that "claim latency rose with pool size" therefore cannot be attributed to PostgreSQL on its own.

Control-plane CPU was the largest single consumer in every unconstrained run (702–884%), exceeding PostgreSQL in most.

## 11. Generator Behavior

`B` pinned the generator to 2 cores (0xC000). It **saturated in both replicates**: probe wake-up p99 of 396.9 ms and 455.9 ms against a 100 ms threshold.

Per the standing rule, **`B` is invalid as a control-plane measurement** and its 1,627 req/s is not evidence about TaskMesh. Its useful output is a quantified generator requirement: although the generator averages 0.75–1.1 cores, 2 dedicated cores are not sufficient — it needs burst headroom.

`B` was also not a clean test even had it worked: it gave the stack 14 cores instead of 16 while removing ~0.8 cores of competition, a net loss of ~1.2 cores. On a 16-core host the stack cannot be given more than 16 cores, so local partitioning cannot reproduce what a second machine would provide.

In all other runs the generator stayed unsaturated (probe p99 15.7–50.6 ms) at 3.8–6.8% CPU and 645–847 MiB RSS.

## 12. Redis

Unaffected across all configurations: 4.7–27.2% CPU, ~15 µs per SET, 0 failed calls. Redis is not implicated in any configuration tested, consistent with Days 10 and 11.

## 13. Kafka / Outbox

Outbox drain measured **100–105 events/sec in every run**, including `E` where job throughput fell 60%, and `D` where PostgreSQL was constrained. Backlog after each 10,000-job run was 33,078–36,423 events.

Resource separation and CPU budgets changed the drain rate by nothing. The outbox remains an **independent event-publication bottleneck**, decoupled from both job throughput and component CPU allocation. Not modified.

## 14. Correctness

All 10 runs: **10,000 / 10,000 jobs COMPLETED**, duplicate claims **0**, execution-id collisions **0** (distinct ids always equal attempts), orphaned RUNNING **0**, over-budget attempts **0**, DLQ **0**, workers left registered **0**.

Reassignments were 0–3 in A/B/C/D but rose to **252 and 521 in `E`**, where control-plane starvation pushed leases past their 30 s expiry. The reaper recovered every one of them, and all jobs still reached `COMPLETED` under new execution ids — at-least-once behaving as designed under severe resource starvation.

## 15. Bottleneck Classification

**MEASURED**
- Achieved throughput: A 1,875 · B 1,627 *(invalid)* · C 1,797 · D 1,597 · E 759 req/s.
- PG CPU: 677 / 507 / 753 / 412 / 288%. CP CPU: 802 / 777 / 754 / 793 / 424%.
- Hikari acquire: 93.8 / 76.5 / 95.3 / 106.1 / **0.9** ms; active 39–40 everywhere except E (1).
- Generator CPU 3.8–6.8% (0.6–1.1 cores) in all runs; generator = ~5% of total consumption.
- Probe p99: 15.7–50.6 ms except B (396.9, 455.9 ms).
- Lock waits 0 in every run. Outbox 100–105 events/sec in every run.
- Container+generator CPU reached 101% and 104% of 1600% in A and C.

**CALCULATED**
- −42% PG CPU → −15% throughput; −47% CP CPU → −60% throughput (≈4× sensitivity ratio).
- A vs C differ by 4%, below replicate spread — not distinguishable.
- Day 11 pool-40 (1,539) vs Day 12 run A (1,875): +22% cross-day variation.

**HYPOTHESIS**
- Control-plane work is dominated by request handling and transaction management rather than SQL execution. Consistent with E, but the internal breakdown was not profiled.
- The residual 1.8–6.0% transport error rate reflects connection-acceptance pressure; cause not isolated.

**CONFIRMED**
- **Throughput at Hikari 40 is substantially more sensitive to control-plane CPU than to PostgreSQL CPU** — a comparable cut costs 60% versus 15%, across two replicates each with all else held constant.
- **The load generator is not a meaningful CPU confound**, at ~5% of total consumption — directly contradicting the assumption the isolation experiment was built on.
- **The outbox is an independent bottleneck**, invariant across every resource configuration tested.

Explicitly **not** confirmed: PostgreSQL as the binding bottleneck.

## 16. Day 11 Hypothesis Reassessment

**Was PostgreSQL confirmed as the next bottleneck? No.** It is on the critical path — constraining it to 4 cores did cost 15% — but its sensitivity is sublinear, giving it headroom (10 vs 16 cores) changed nothing measurable, and lock waits were zero throughout. Day 11's supporting evidence, the rising `taskmesh.jobs.claim` timer, turns out to be a control-plane-side measurement that rises when the control plane is starved even as PostgreSQL CPU falls.

**Was host CPU contention the primary explanation? Partly, but not as Day 11 framed it.** Host CPU saturation is real — A and C ran at 101% and 104% of nominal capacity. But it is not caused by generator co-tenancy (~5%); it is caused by the control plane being the single largest consumer (702–884%). Removing the generator would recover ~0.8 of 16 cores.

**Overall: this maps to Scenario 3 — multiple contributors, with one clearly dominant.** Control-plane CPU is the dominant sensitivity, PostgreSQL contributes secondarily, and aggregate host saturation is the environment in which both operate. The Day 11 plateau is better explained by control-plane CPU than by PostgreSQL.

## 17. Limitations

- **No true isolation.** Single 16-core host, no second machine. `B` is a local approximation that failed by saturating the generator. A second load-generation host remains the only way to remove co-tenancy entirely.
- cpuset under Docker Desktop pins to WSL2 **VM vCPUs**; the hypervisor may still schedule those vCPUs across physical cores. Partitioning is meaningful but not a hardware guarantee.
- Two replicates per configuration; differences under ~10% are not resolvable.
- Cross-day variation (22%) exceeds within-day spread, so Day 11 and Day 12 figures are not directly comparable.
- CPU budgets change *allocation*, not efficiency; they identify sensitivity, not the internal cause of control-plane CPU consumption.
- The control plane was not profiled — what consumes its CPU is unmeasured.
- 60-second windows, no soak testing. Hikari percentiles unavailable (mean/max only).
- `taskmesh.jobs.claim` is control-plane-side and cannot be read as pure SQL latency.

## 18. Engineering Interpretation

At a Hikari pool of 40, throughput responds far more to control-plane CPU than to PostgreSQL CPU. Removing roughly the same share of CPU from each cost 60% of throughput for the control plane and 15% for PostgreSQL. Giving PostgreSQL more headroom than it was already using produced no measurable change.

The starved-control-plane run is the clearest single observation of the day. With the control plane on 4 cores, connection acquisition fell from 93.8 ms to 0.9 ms and active connections dropped from 40 to 1 — the control plane could no longer produce enough concurrent work to occupy its own pool. Whatever limits throughput in that state is inside the control-plane process, not in front of the database.

That also corrects a measurement assumption carried from Day 11. The `taskmesh.jobs.claim` timer spans the whole claim method rather than the SQL alone; it rose fivefold while PostgreSQL CPU fell, so its growth across Day 11's pool sweep cannot be read as evidence about PostgreSQL.

The isolation premise itself did not survive inspection. The load generator consumes about 5% of total CPU, so moving it to another machine would recover roughly 0.8 of 16 cores. The host saturation Day 11 observed comes from the control plane and PostgreSQL together, with the control plane the larger of the two. The attempt to partition locally failed in an informative way: two dedicated cores were not enough for the generator, which saturated at probe p99 of ~400 ms despite averaging around one core.

Correctness was unaffected by every resource configuration tested, including the severely starved one. All 10,000 jobs reached a terminal state in all ten runs with zero duplicate claims and zero execution-id collisions. The 252 and 521 reassignments under control-plane starvation are the lease mechanism working as specified, not a defect.

The outbox remains entirely separate. Its drain rate was 100–105 events/sec regardless of pool size, CPU budget, or partitioning — unchanged across every experiment since Day 10.

**Potential resume metric — requires final review:** "Diagnosed a distributed scheduler's throughput ceiling through controlled CPU-budget experiments (5 configurations × 2 replicates, 5,000 logical workers / 10,000 jobs), showing throughput was 4× more sensitive to control-plane CPU than to database CPU and correcting an earlier hypothesis that the database was the constraint."

No configuration is described here as preferable; this reports what each measured.
