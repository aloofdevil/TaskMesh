# Day 24 — Multi-Instance Safety of Key-Aware Outbox Publisher Ownership

Every number is labelled **MEASURED** (directly observed), **CALCULATED**
(derived from measured values), **INFERRED** (interpretation supported by
measurements but not directly proven), or **HYPOTHESIS** (proposed, untested).
Raw artefacts live outside the repository under `results24/<label>/`.

## 1. Research Question

> Does the Day 23 key-aware sharding mechanism preserve per-key ordering and
> exclusive ownership of publishing work when **multiple control-plane
> instances** run concurrently against the same PostgreSQL and Kafka?

Day 23 flagged single-instance-only as its most serious limitation but did not
test it. Day 24 tests it.

**Answer, measured: no.** Details in §7–§11.

## 2. Current Architecture

Inspected before anything was modified.

The claim is either unsharded:

```sql
SELECT * FROM job_events WHERE published_at IS NULL
 ORDER BY id ASC LIMIT 100 FOR UPDATE SKIP LOCKED
```

or, with Day 23's flag on, restricted to a shard:

```sql
   AND ((hashtext(aggregate_id) % :shards) + :shards) % :shards = :shard
```

Two facts from the code decide this experiment's design:

1. **`shard` and `shards` are purely local.** `OutboxPublisherScheduler`
   assigns `shard = i` for task `i` and `shards = concurrency`, both read from
   this JVM's own configuration. Nothing consults the database, another
   instance, or any external coordinator. Two instances therefore each run
   shards `0..C-1` — **the same set**.
2. **At concurrency 1 the sharded path is never reached.**
   `publish()` does `concurrency == 1 ? publisher.publishPending() :
   publishConcurrently(...)`, and `publishPending()` is the *unsharded* claim.
   So `key-aware-sharding=true` is **silently inert at concurrency 1**.

Fact 2 was not documented on Day 23 and is a real defect in the prototype: a
single-threaded publisher gets none of the advertised behaviour and nothing says
so. It is now pinned by a test (§Implementation changes).

## 3. Day 23 Mechanism and What It Proved

Day 23 measured, single instance: 3,522 / 4,492 / 4,781 ordering violations at
C=2/4/8 for the baseline, and **0 at every C** for the sharded prototype. Its
ownership evidence was that two *concurrently held* claims share no key.

That result stands and is reproduced here (§8). What it did not establish is
that the boundary survives a second JVM, because the boundary is enforced by the
query predicate **plus** the assumption that exactly one thread exists per
shard — an assumption only one process can make about itself.

## 4. Experimental Design

**Arm A — one control-plane instance**, sharding on, C = 2, 4, 8.
**Arm B — two control-plane instances**, sharding on, C per instance = 1/1,
2/2, 4/4, 8/8. Both instances share one PostgreSQL, one Kafka, one topic.

2 replicates per arm; **14 runs, all valid**. Arms were interleaved (1-instance
and 2-instance at each concurrency, then repeated) so host drift lands on both.

The second instance is added by a compose overlay kept **outside the
repository** (`day24-two-instances.yml`), applied with `-f`, so
`docker-compose.yml` is not modified for an experiment. It runs the same image,
same database, same Redis, same Kafka, actuator on host port 8081, and its lease
reaper disabled so a second background writer does not confound the publisher
measurement.

### A configuration trap the assertions caught

The first two-instance attempt was **marked INVALID by the harness**: instance A
reported an empty `JOB_EVENTS_TOPIC` while instance B had the dedicated one. The
repository's compose file has no `JOB_EVENTS_TOPIC` pass-through, so instance A
fell back to `application.yml`'s default and the two instances would have
published to **different topics** — silently invalidating every comparison. The
overlay now sets it for both. Recorded because the run was discarded, not
quietly fixed.

## 5. Preconditions

Checked per run, aborting before any measurement (MEASURED, all 14 passed):

| Precondition | Result |
|---|---|
| exactly 10,000 events inserted | pass, all runs |
| exactly 20 distinct keys | pass, all runs |
| every key exactly 500 events | pass (0 keys with a wrong count) |
| no pre-existing id/sequence violations | pass (0) |
| all events start unpublished | pass (0 already published) |
| dedicated Kafka topic | `taskmesh.day24.<label>`, one per run, 3 partitions asserted via `kafka-topics.sh --describe` |
| both instances on the same database | asserted: `DB_HOST=postgres`, `DB_NAME=taskmesh` on both |
| both instances same shard configuration | asserted: `OUTBOX_KEY_AWARE_SHARDING`, `OUTBOX_PUBLISHER_CONCURRENCY`, `OUTBOX_ASYNC_SENDS`, `JOB_EVENTS_TOPIC` compared on both containers |

Any mismatch writes `INVALID=<reason>` and exits before measuring.

## 6. Configuration

| | |
|---|---|
| Workload | 20 keys × 500 events = 10,000; deterministic SQL generator, **no randomness** |
| Kafka partitions | 3 |
| Outbox batch size | 100 |
| Poll interval | 100 ms |
| `async-sends` | **false** in every arm |
| `key-aware-sharding` | **true** in every arm |
| HikariCP max pool | 40 |
| Git commit | `fbc2da3` (Day 23 staged, uncommitted — see §Git) |
| Java | 21.0.2 LTS |
| Spring Boot | 4.1.1 |
| PostgreSQL | 16-alpine |
| Kafka | 4.2.1, single-node KRaft, RF=1 |
| Docker | Server 29.8.0, **16 CPUs, 8,162,320,384 B (7.60 GiB)** |
| Standing confound | six `k8s_*` containers from Day 6, present as in Days 17–23 |

## 7. Results Table

All 14 runs valid. OOO = out-of-order transitions; rate = OOO ÷ transitions.

| Instances | C each | Rep | Events | Published | Unpublished | Dup | Missing | OOO | rate | keys hit | max disp | 1st violation | DLQ | Result |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|
| 1 | 2 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | **0** | 0.0000 | 0 | 0 | — | 0 | ordered |
| 1 | 2 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | **0** | 0.0000 | 0 | 0 | — | 0 | ordered |
| 1 | 4 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | **0** | 0.0000 | 0 | 0 | — | 0 | ordered |
| 1 | 4 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | **0** | 0.0000 | 0 | 0 | — | 0 | ordered |
| 1 | 8 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | **0** | 0.0000 | 0 | 0 | — | 0 | ordered |
| 1 | 8 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | **0** | 0.0000 | 0 | 0 | — | 0 | ordered |
| 2 | 1 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | 3,679 | 0.3686 | 20 | 5 | 12 | 0 | **violated**\* |
| 2 | 1 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | 3,011 | 0.3017 | 20 | 3 | 14 | 0 | **violated**\* |
| 2 | 2 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | 3,458 | 0.3465 | 20 | 9 | 13 | 0 | **violated** |
| 2 | 2 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | 3,526 | 0.3533 | 20 | 7 | 10 | 0 | **violated** |
| 2 | 4 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | 4,451 | 0.4460 | 20 | 25 | 6 | 0 | **violated** |
| 2 | 4 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | 3,589 | 0.3596 | 20 | 14 | 13 | 0 | **violated** |
| 2 | 8 | 1 | 10,000 | 10,000 | 0 | 0 | 0 | 3,571 | 0.3578 | 20 | 50 | 28 | 0 | **violated** |
| 2 | 8 | 2 | 10,000 | 10,000 | 0 | 0 | 0 | 3,467 | 0.3474 | 20 | 55 | 26 | 0 | **violated** |

\* At C=1 sharding is **inert** (§2 fact 2), so these two arms measure two
unsharded instances, not two sharded ones. They are reported because they were
run, and labelled so they are not mistaken for a sharding result.

**Every one of the six single-instance runs: 0 violations. Every one of the
eight two-instance runs: thousands.** Correctness gates — duplicates, missing,
unpublished, DLQ, Kafka record errors — were **0 in all 14 runs**.

Max displacement scales with *combined* concurrency across instances (3–5 at
1+1, 7–9 at 2+2, 14–25 at 4+4, 50–55 at 8+8), consistent with Day 22's finding
that displacement tracks the number of concurrently in-flight batches.

## 8. Ownership Evidence

Instance identity exists nowhere in the data model, so it was added as
**instrumentation**: one DEBUG record per pass carrying `instance`, `shard`,
`shards`, `count` and the exact claimed event ids. One line per pass, never per
event, and only on sharded claims.

| Instances | C each | Passes | Distinct instances | Shards seen | **Shards touched by >1 instance** | **Keys touched by >1 instance** | **Rows claimed by >1 instance** |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 2 | 99–100 | 1 | 2 | **0** | **0** | **0** |
| 1 | 4 | 100 | 1 | 4 | **0** | **0** | **0** |
| 1 | 8 | 100 | 1 | 8 | **0** | **0** | **0** |
| 2 | 1 | **0** | 0 | 0 | — | — | 0 |
| 2 | 2 | 99–100 | 2 | 2 | **2 of 2** | **20 of 20** | **0** |
| 2 | 4 | 100 | 2 | 4 | **4 of 4** | **20 of 20** | **0** |
| 2 | 8 | 100 | 2 | 8 | **8 of 8** | **20 of 20** | **0** |

Per-shard detail for one 4/4 run (MEASURED):

```
shard 0: instances=[0035588d, 018c75db]  passes 10 / 10
shard 1: instances=[0035588d, 018c75db]  passes 15 / 15
shard 2: instances=[0035588d, 018c75db]  passes 10 / 10
shard 3: instances=[0035588d, 018c75db]  passes 15 / 15
```

Every shard was worked by both instances, in near-equal share. There is no
ownership; there is only a filter that both instances apply identically.

The two-instance C=1 arms logged **zero** claim records — independent
confirmation of the §2 short-circuit, since the sharded claim is the only thing
that logs.

**Row locking works exactly as advertised and is not the same thing as
ownership.** `rows claimed by >1 instance = 0` in every run: `FOR UPDATE SKIP
LOCKED` never let two instances hold the same row. It also never stopped them
holding *different rows of the same key*, which is what ordering depends on.

Attribution coverage: 9,800–9,900 of 10,000 rows per run. The shortfall is one
pass's log line per run not surviving the grep; internal consistency was checked
(`count` field always equalled the number of parsed ids, no id claimed twice).

## 9. Kafka Ordering Results

Measured Kafka-side, per partition from earliest, so file order is offset order
and a key's records arrive already in delivery order.

| Arm | violations | rate | keys violated | max displacement |
|---|--:|--:|--:|--:|
| 1 instance, C=2/4/8 | **0** | 0.0000 | 0 / 20 | 0 |
| 2 instances, C=2/2 | 3,458–3,526 | ~0.35 | 20 / 20 | 7–9 |
| 2 instances, C=4/4 | 3,589–4,451 | 0.36–0.45 | 20 / 20 | 14–25 |
| 2 instances, C=8/8 | 3,467–3,571 | ~0.35 | 20 / 20 | 50–55 |

### A methodology trap, found and reported

`publish_inversions` — the database-side ordering check used on Days 22 and 23,
comparing `published_at` between consecutive events of a key — read **0 in all
14 runs**, including the eight with thousands of Kafka violations.

It is **not a valid ordering proxy across instances**. `published_at` is the
publishing transaction's *start* time, but the Kafka send happens *during* that
transaction. Within one JVM the tick barrier kept transaction-start order and
send order aligned, which is why the measure worked on Days 22–23. Across two
instances nothing synchronises them: instance B's transaction can start later
and still send first.

Had Day 24 relied on the inherited measure, it would have concluded
"multi-instance sharding preserves ordering" — the exact opposite of the truth.
All ordering conclusions here rest on the Kafka-side sequence check.

## 10. Failure / Restart Observations

One controlled run (MEASURED). **Deviation stated up front:** the backlog is
20 keys × 2,000 = 40,000 events, not 10,000, because a 10,000-event drain
completes in ~10 s — too short to stop an instance mid-drain. Everything else is
unchanged: two instances, C=4 each, sharding on, async off.

| Phase | t | Unpublished | Drain rate |
|---|--:|--:|--:|
| both instances up | 0–15 s | 40,000 → 25,100 | **960 /s** |
| instance B stopped | 15 s | 25,100 | — |
| B down | 16–30 s | 25,100 → 16,700 | **523 /s** |
| B restarted | 30 s | 16,700 | — |
| B back | 31–60 s | 16,700 → 0 | **559 /s** |
| complete | 60 s | 0 | — |

Findings:

- **No shard stalled.** Unpublished decreased monotonically for the entire time
  B was down (MEASURED). Because both instances run every shard, the survivor
  already covered all of them — there was nothing to take over.
- **Throughput roughly halved** while one of two instances was down (960 → 523
  /s), then did not fully recover in the remaining window (559 /s). Why it did
  not return to ~960 /s is **not established**: the remaining backlog was
  smaller and shards drain unevenly near the end. Not investigated.
- **Nothing lost or duplicated**: 40,000 consumed, 40,000 unique, 0 duplicates,
  0 missing, 0 unpublished, 0 DLQ.
- **Ordering still violated**: 9,447 out-of-order transitions (rate 0.2363), all
  20 keys, max displacement 30.

This is the exact mirror of Day 23's single-instance failure mode. There, a dead
publishing thread stalled its shard **forever** because ownership was real.
Here, a dead instance stalls nothing because ownership is absent. The two
results describe the same missing mechanism from opposite sides: **you cannot
have takeover without ownership, and you cannot have ordering without it
either.**

## 11. Interpretation

Against the stated hypotheses:

- **H1 — both instances process the same shard because ownership is JVM-local.**
  **CONFIRMED** (MEASURED). Every shard was worked by both instances in all six
  sharded two-instance runs — 2 of 2, 4 of 4, 8 of 8 — with near-equal pass
  counts.
- **H2 — if both process the same shard, per-key ordering may again be
  violated.** **CONFIRMED** (MEASURED). 3,458–4,451 violations across all 20
  keys in every sharded two-instance run, against 0 in every single-instance
  run at the same concurrency, same topic, same configuration, interleaved
  execution.
- **H3 — `SKIP LOCKED` prevents duplicate row claims but not ordered
  ownership.** **CONFIRMED** (MEASURED). Rows claimed by more than one instance:
  **0**, every run. Keys claimed by more than one instance: **20 of 20**, every
  sharded two-instance run. Both halves of the hypothesis hold simultaneously,
  and that combination is precisely the failure.
- **H4 — a real distributed ownership mechanism is required.** **SUPPORTED, not
  proven.** Supported because the measured failure is exactly the absence of
  cross-instance ownership, and because single-instance runs with ownership show
  0 violations under otherwise identical conditions. Not proven because Day 24
  implemented no such mechanism and therefore did not demonstrate one working.

**The concrete failure mode**, stated with the caveat that the specific
interleaving below is illustrative of the measured class of failure rather than
a transcript of one observed key:

```
Key K hashes to shard 2. Both instance A and instance B run shard 2.
A claims shard-2 rows [.. K5 K6 ..]; B concurrently claims the next
shard-2 rows [.. K7 K8 ..]. SKIP LOCKED keeps the row sets disjoint.
Nothing orders A's send against B's send, so K7 can reach Kafka before K5.
```

What is measured, not illustrative: both instances worked every shard; no row
was claimed twice; and a key's sequence arrived out of order for all 20 keys.

**Day 23's mechanism is not multi-instance safe, and the reason is structural,
not a tuning problem.** The shard predicate is a pure function of the key, so it
partitions *rows*; ownership additionally requires that exactly one worker
applies each shard, and that is an assumption a process can only make about
itself.

## 12. Limitations

1. **Two instances only.** Three or more untested; no reason to expect
   improvement, but it was not measured.
2. **The C=1 arms do not test sharding** (§2 fact 2) and are labelled as such.
3. **One restart run, one failure shape.** Only a clean `docker compose stop`
   was exercised — not a crash mid-transaction, a network partition, a paused
   container, or a database failover. Recovery conclusions cover only this case.
4. **Why throughput did not recover to 960 /s after restart is unexplained**
   (§10).
5. **Throughput at C=8/8 is coarse.** The drain finishes in 1.6–1.7 s against
   1 s sampling, so ~2 samples; that figure should not be compared precisely
   with the others.
6. **Instance identity is instrumentation, not production data.** If the DEBUG
   logger is off there is no way to attribute a published row to an instance —
   an observability gap in its own right.
7. **1–2% of rows unattributed per run** (§8), from log lines lost to the grep.
8. **One workload shape**: 20 uniform keys, interleaved ids, 500 events each.
   Skew untested.
9. **Single broker, RF=1**, and the six `k8s_*` containers were present
   throughout.
10. **The illustrative interleaving in §11 was not captured as a specific
    observed trace**; the claim log records claims, not send timestamps.

## 13. Architectural Implications

**What mechanism must exist for key-aware outbox sharding to be safe across
multiple control-plane instances?**

The requirement the experiment establishes: *at any moment, each shard must have
at most one live publisher across the whole deployment, and that assignment must
survive instance failure.* A deterministic key→shard function is necessary but
demonstrably not sufficient — Day 24 had one and still failed.

Compared factually; **not ranked**, because Day 24 measured none of them:

| | Correctness basis | Failure recovery | Complexity | DB dependency | Rebalancing | Interaction with publisher concurrency |
|---|---|---|---|---|---|---|
| **A. Shard lease table** | a row per shard with holder + expiry; claim requires a valid lease | automatic on expiry, tunable | moderate; reuses the job-lease pattern already in the codebase | adds a table and periodic writes | on lease expiry, no coordination needed | shard count must be decoupled from per-instance concurrency |
| **B. Advisory locks** | `pg_try_advisory_lock` per shard; holder is the owner | automatic — locks die with the session | low; no schema change | session-scoped locks, held for the pass or the process | implicit, whoever grabs it next | same decoupling needed; a long-held lock blocks the shard |
| **C. Fixed ownership + heartbeat** | static assignment plus liveness | needs explicit detection and reassignment | high | depends on where assignment lives | manual or coordinator-driven | concurrency becomes a deployment-wide constant |
| **D. Leader election** | one publisher elected; others idle | automatic on re-election | moderate, but wastes capacity | depends on the election store | trivial — one owner | caps total publishing at one instance |
| **E. Partition-aware ownership** | one publisher owns a Kafka partition end to end | needs the same liveness problem solved | moderate | none extra | tied to consumer-group-like rebalance | concurrency capped at partition count (3 here) |

Two constraints Day 24 makes concrete for any of them:

- **Shard count must stop being `publisher-concurrency`.** Today it is the
  instance's own thread count, so two instances with different concurrency would
  compute *different shard spaces* for the same key — a worse failure than the
  one measured. A deployment-wide fixed shard count is a prerequisite.
- **The C=1 short-circuit must be fixed**, or any ownership scheme is silently
  bypassed on single-threaded publishers.

Day 23's single-instance result plus Day 24's two-instance result together say
the mechanism is *sound in principle and unenforceable in the current
deployment model*. The gap is entirely in enforcement, not in the hashing.

## 14. Next Experiment Recommendation

In order:

1. **Decouple shard count from publisher concurrency** and re-run Day 24's
   matrix. This is a prerequisite for every design in §13 and is the smallest
   change that makes the shard space deployment-wide rather than per-JVM. Until
   it exists, no ownership mechanism can be tested honestly.
2. **Prototype Option B (advisory locks)** — the smallest mechanism that could
   work, needing no schema change, with ownership dying automatically with the
   session. Measure it against the same 1-instance/2-instance matrix, and
   specifically measure what Day 24 could not: whether a stalled shard recovers,
   and how long that takes.
3. **Then Option A (lease table)** if B's session-scoped semantics prove too
   coarse, since the codebase already has the lease pattern.
4. **Add instance identity to the outbox row** (or to a published-by column) so
   ownership is auditable from data rather than from DEBUG logs (§12 item 6).
5. **Before any of it**, fix the C=1 short-circuit — currently pinned as a
   passing test asserting the defective behaviour, so the fix will surface as a
   deliberate test change.

Not recommended yet: implementing any ownership mechanism in production. Day 24
establishes the problem; it does not validate a solution.
