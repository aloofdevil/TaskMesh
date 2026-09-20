# Day 23 — Key-Aware Publisher Ownership: Prototype and Experiment

Every number is labelled **MEASURED** (directly observed), **CALCULATED**
(derived from measured values), **INFERRED** (interpretation supported by
measurements but not directly proven), or **HYPOTHESIS** (proposed, untested).
Raw artefacts live outside the repository under `results23/<label>/`.

## 1. Objective

Days 21–22 established that per-key ordering is exact at
`publisher-concurrency = 1` and broken at every concurrency above it, in both
send modes, and localised the loss to concurrent publisher passes rather than
Kafka. Day 23 asks:

> Can publisher responsibility be partitioned so that several publishers run
> concurrently while every event for a key is handled by exactly one
> serialisation domain?

This is a prototype and an experiment. **No production default was changed.**

## 2. Current Architecture (Phase A — inspection only)

Read from the code before anything was modified.

**How rows are selected.** One query, no key awareness:

```sql
SELECT * FROM job_events
 WHERE published_at IS NULL
 ORDER BY id ASC
 LIMIT 100
 FOR UPDATE SKIP LOCKED
```

**How batches are formed.** The first `batch-size` unpublished rows by id,
skipping rows another transaction already holds.

**What locks are held.** Row-level `FOR UPDATE` locks on the claimed rows, held
for the whole `@Transactional publishPending()` — *including all Kafka I/O* —
released only at commit.

**When rows are marked published.** After Kafka acknowledges, in the same
transaction: `UPDATE job_events SET published_at = now() WHERE id IN (…) AND
published_at IS NULL`.

**When can two publishers claim events for the same key?** Whenever
`concurrency > 1`, and in this workload essentially always. Pass 1 takes ids
[1..100]; pass 2 skips those locked rows and takes [101..200]. Because a key's
events are spread across the id space, both batches contain the same key. Day 22
measured **19.7–20.0 of 20 keys in every pass**.

The ownership boundary is therefore an **id range**, and keys cut across id
ranges. That is the defect, stated precisely.

Supporting configuration: events are keyed by `aggregate_id`
(`kafkaTemplate.send(topic, event.getAggregateId(), message)`), topics have 3
partitions, `job_events` has `idx_job_events_unpublished ON (id) WHERE
published_at IS NULL` and `idx_job_events_aggregate ON (aggregate_id, id)`.

## 3. Root-Cause Hypothesis from Days 21–22

**H-RACE** — concurrent passes claim disjoint *id ranges* that each contain the
same *keys*, so the pass that commits first is not necessarily the one holding
the earlier events, and a key's events reach Kafka out of sequence.

Day 22 evidence: every pass carried all 20 keys; at C>1, 9–45 pass pairs per run
committed out of id order while C=1 had 0; max displacement matched the batch
geometry 5(C−1) to within 1 at every concurrency.

Day 23 tests this directly rather than inferring it (§10, §11).

## 4. Candidate Ownership Models (Phase C)

| Option | Mechanism | Enforceable? | Cost |
|---|---|---|---|
| **A — deterministic ownership in the query** | each pass claims only rows whose key hashes to its shard | **Yes, by construction** — the predicate is a pure function of the key, so two shards can never see the same row. No coordination, no new state. | Index scan filters by hash, so a shard scans ~P× more index entries to fill a batch; throughput becomes sensitive to hash balance |
| B — explicit shard column | store a shard on each row | Yes, and cheaper to index | Requires a **schema migration**, which Day 23 was told to avoid |
| C — advisory locks | take a per-key lock before processing | Partly — a batch holds ~20 keys, so ~20 locks per pass, with lock-ordering and deadlock risk; prevents *processing*, not *claiming* | Complexity, deadlock surface |
| D — application-level per-key serialisation | claim freely, serialise sends per key in the JVM | Only within one JVM, and the claim still interleaves id ranges, so commit order still races unless publish *and* mark are serialised together | Complex, and does not address the claim |

## 5. Selected Prototype

**Option A**, behind `taskmesh.outbox.key-aware-sharding` (**default `false`**).

```sql
SELECT * FROM job_events
 WHERE published_at IS NULL
   AND ((hashtext(aggregate_id) % :shards) + :shards) % :shards = :shard
 ORDER BY id ASC LIMIT :limit FOR UPDATE SKIP LOCKED
```

The scheduler assigns **task `i` → shard `i` of `concurrency`**, a fixed 1:1
mapping for the tick. Combined with the existing `future.get()` barrier — which
makes every pass of a tick finish before the next tick starts one — each shard
has at most one pass in flight at any moment.

Separation of concerns, as required:

| | |
|---|---|
| **Production code, default-off** | `OutboxProperties.keyAwareSharding`, `JobEventRepository.lockUnpublishedBatchForShard`, `OutboxPublisher.publishShard`, scheduler branch, `application.yml`, `docker-compose.yml` |
| **Test-only** | `OutboxKeyOwnershipTests` (8 tests, dedicated topic) |
| **Experiment-only** | `d23.sh`, `verify23.py`, `sup23.py` — outside the repository |

When the flag is off, the claim path is byte-identical to before: a separate
repository method is called, not a modified one.

## 6. Why This Prototype Was Selected

- **Smallest mechanism that can demonstrate the invariant.** No schema change,
  no new coordination service, no locks beyond those already taken.
- **Enforceable rather than advisory.** The boundary is the query predicate
  itself. A pass *cannot* see another shard's rows, so ownership does not depend
  on publishers behaving correctly. Options C and D enforce by convention or by
  in-JVM state.
- **Directly falsifiable.** Two concurrent claims either share a key or they do
  not, and that is testable in one assertion (§11).

`hashtext` is a PostgreSQL internal function: deterministic within a major
version, which is all a single-instance prototype needs, but **not a documented
stable hash across versions**. A production design would use an explicit,
version-pinned hash — see §17.

## 7. Experimental Methodology

Identical workload to Day 22, reusing that harness: **20 keys × 500 events =
10,000**, 3 partitions, batch 100, poll 100 ms, Hikari 40, deterministic SQL
generator, **no randomness**. `async-sends = false` in **every** arm — one new
variable at a time.

**Matrix, 14 runs, all valid:** baseline at C=1, 2, 4, 8 and prototype at C=2,
4, 8, **2 replicates each**.

**Execution order was interleaved** — baseline C=2, prototype C=2, baseline C=4,
prototype C=4, … — repeated for replicate 2, specifically to avoid the Day 22
confound where all of r1 ran before all of r2 and host drift loaded onto the
replicate number.

Per-run assertions, aborting on mismatch (MEASURED, all 14 passed):
`env_key_aware_sharding` matching the arm, `env_async_sends=false`,
`env_concurrency`, `env_poll_ms=100`, `partitions_actual=3`, 10,000 events
inserted, and **0 precondition violations** of "per key, id order equals seq
order".

Ordering is measured **Kafka-side**: one consumer pinned per partition reading
from earliest, so file order is offset order; a key maps to one partition, so its
records arrive already in delivery order. Sequence numbers are checked, not just
counts.

## 8. Baseline Results

MEASURED, 2 replicates per arm:

| C | throughput (range) | OOO violations (range) | rate | keys violated | max displacement | publish inversions |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 133–134 | **0** (0–0) | 0.0000 | 0 / 20 | 0 | **0** |
| 2 | 257 (256–259) | 3,522 (3,515–3,528) | 0.3529 | 20 / 20 | 5 | 296 |
| 4 | 490 (481–499) | 4,492 (4,468–4,515) | 0.4501 | 20 / 20 | 15 | 660 |
| 8 | 938 (938–939) | 4,781 (4,710–4,852) | 0.4791 | 20 / 20 | 35 | 1,050 |

This reproduces Day 22 independently, including the cliff at C=2 and the
5(C−1) displacement bound.

## 9. Prototype Results

MEASURED, 2 replicates per arm:

| C | throughput (range) | OOO violations | rate | keys violated | max displacement | publish inversions |
|--:|--:|--:|--:|--:|--:|--:|
| 2 | 253 (252–253) | **0** (0–0) | **0.0000** | **0 / 20** | **0** | **0** |
| 4 | 488 (483–493) | **0** (0–0) | **0.0000** | **0 / 20** | **0** | **0** |
| 8 | 873 (860–885) | **0** (0–0) | **0.0000** | **0 / 20** | **0** | **0** |

**Zero ordering violations at every tested concurrency, in both replicates.**

## 10. Ordering Analysis

Paired, interleaved, same host state:

| C | baseline OOO | prototype OOO | baseline max disp | prototype max disp |
|--:|--:|--:|--:|--:|
| 2 | 3,522 | **0** | 5 | **0** |
| 4 | 4,492 | **0** | 15 | **0** |
| 8 | 4,781 | **0** | 35 | **0** |

Every key was delivered as sequence 1..500 ascending in all six prototype runs
(MEASURED). `publish_inversions` — computed independently of Kafka, from
`published_at` ordering within each key — is also **0 in every prototype run**
against 296–1,050 in the baseline.

Two independent measurements, one Kafka-side and one database-side, agree that
ordering is intact.

**The prototype answers Day 22's question affirmatively: parallelism and per-key
ordering are not inherently in conflict — the conflict came from claiming by id
range.**

## 11. Ownership Analysis

### Direct evidence (tests, deterministic)

`OutboxKeyOwnershipTests` holds two claims open *simultaneously* — each
transaction claims, signals a latch, and blocks until both have claimed — so the
question asked is genuinely about concurrent ownership, not sequential claims:

| Test | Result |
|---|---|
| `currentClaimLetsTwoConcurrentPassesHoldTheSameKeys` | **passes** — the two concurrent baseline claims share keys. The Day 21/22 root cause, reproduced as an assertion. |
| `shardedClaimGivesTwoConcurrentPassesDisjointKeys` | **passes** — the two concurrent sharded claims share **no** key. |
| `everyEventOfAKeyIsVisibleToExactlyOneShard` | **passes** — the 4 shards partition the outbox: every event claimed by exactly one shard, and none by two. |
| `aKeyAlwaysMapsToTheSameShard` / `differentKeysCanMapToDifferentShards` | **pass** |

This is the strongest ownership evidence available, and it is direct rather than
inferred.

### Live-run evidence, and a methodology limit found and reported

Day 22 identified passes by `published_at` (fixed at transaction start). In the
live runs that technique **reaches its resolution limit here** and must not be
over-read:

| arm | C | single passes mixing shards | pure single passes | collided passes |
|---|--:|--:|--:|--:|
| baseline | 2 | **86–94** | 0 | 3–7 |
| baseline | 4 | 86–92 | 0 | 4–7 |
| baseline | 8 | 92–94 | 0 | 3–4 |
| prototype | 2 | **1** | 90–94 | 3–5 |
| prototype | 4 | **1** | 85–88 | 5–7 |
| prototype | 8 | **1** | 93–96 | 2–3 |

The contrast is stark and in the expected direction: the baseline's passes
essentially *all* mix shards; the prototype's are essentially all shard-pure.

But the residual "1" in every prototype run is **ambiguous, not zero**, and it is
reported as such. A pass claims at most `batch-size = 100` rows, so any
`published_at` group larger than that is provably two or more passes sharing one
microsecond — those are counted separately as *collided passes*. A group of
**≤100** rows containing two shards, however, cannot be distinguished from two
small partial passes that collided. Inspection of the C=2 pilot showed four such
groups at n=200 (definitionally two passes) and one at n=100 (ambiguous).

Sharding makes collisions *more* likely, because the tick barrier starts all
shard passes simultaneously — so this artefact is expected, and it is why the
live-run ownership numbers are supporting evidence rather than proof. The proof
is the test in the previous subsection, plus the outcome: **0 ordering
violations**.

### Shard load balance (MEASURED)

| C | keys per shard | events per shard |
|--:|---|---|
| 2 | 8 / 12 | 4,000 / 6,000 |
| 4 | 4 / 6 / 4 / 6 | 2,000 / 3,000 / 2,000 / 3,000 |
| 8 | 1 / 3 / 1 / 3 / 3 / 3 / 3 / 3 | 500 / 1,500 / 500 / 1,500 × 5 |

At C=8 the busiest shard carries **3× the lightest**. This is the prototype's
main cost mechanism — see §13.

## 12. Correctness Analysis

**All 14 runs passed every gate** (MEASURED):

| Check | baseline (8 runs) | prototype (6 runs) |
|---|---|---|
| produced | 10,000 | 10,000 |
| consumed | 10,000 | 10,000 |
| unique event IDs | 10,000 | 10,000 |
| duplicate IDs | **0** | **0** |
| missing IDs | **0** | **0** |
| final unpublished | **0** | **0** |
| DLQ | **0** | **0** |
| Kafka `record.error.total` | **0** | **0** |
| control-plane error log lines | **0** | **0** |

The prototype changes *which rows a pass can see*, not what happens to a row
once claimed: `markPublished` is still guarded on `published_at IS NULL`, sends
are still acknowledged before marking, and failures still leave rows unpublished.
`shardedPublishingStillPublishesEveryEventExactlyOnce` pins this. At-least-once
delivery is unchanged.

Zero duplicates across 140,000 published events is an observation under no-crash
conditions, **not** exactly-once.

## 13. Throughput / Resource Analysis

| C | baseline (range) | prototype (range) | ratio | ranges overlap? |
|--:|--:|--:|--:|---|
| 2 | 257 (256–259) | 253 (252–253) | 0.98 | **no** — real, ~1.6% cost |
| 4 | 490 (481–499) | 488 (483–493) | 1.00 | **yes** — indistinguishable |
| 8 | 938 (938–939) | 873 (860–885) | 0.93 | **no** — real, ~6.9% cost |

**The ordering guarantee cost roughly 0–7% throughput in this workload**, with
the cost concentrated at C=8. Reported honestly either way, and not optimised.

Resources (MEASURED, maxima over samples, averaged over replicates):

| arm | C | Hikari active | **Hikari pending** | acquire ms | **PG lock waits** | CP CPU% | threads | Kafka queue ms | Kafka errors | **claim ms** |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline | 2 | 2 | **0** | 0.08 | **0** | 49 | 32 | 5.18 | 0 | 7.4 |
| prototype | 2 | 2 | **0** | 0.11 | **0** | 93 | 32 | 5.20 | 0 | **6.5** |
| baseline | 4 | 4 | **0** | 0.05 | **0** | 100 | 34 | 5.22 | 0 | 12.8 |
| prototype | 4 | 4 | **0** | 0.07 | **0** | 68 | 34 | 5.18 | 0 | **10.3** |
| baseline | 8 | 8 | **0** | 0.03 | **0** | 78 | 38 | 5.20 | 0 | 21.0 |
| prototype | 8 | **6** | **0** | 0.04 | **0** | 77 | 38 | 5.21 | 0 | **20.0** |

Two observations the data supports:

- **The hash predicate did not make claiming slower.** Claim duration is
  *lower* in the prototype at every C (6.5 vs 7.4; 10.3 vs 12.8; 20.0 vs 21.0
  ms). Plausibly because a shard's claim locks fewer contended rows and skips
  less, but this is **INFERRED** — no `EXPLAIN ANALYZE` was run. So the
  throughput cost is **not** explained by claim cost.
- **Hikari active peaks at 6, not 8, in the prototype at C=8** — direct evidence
  that some shards find nothing and idle, consistent with the 1:3 key imbalance
  in §11. This is the **INFERRED** mechanism for the ~7% cost at C=8: the drain
  is bounded by the busiest shard while lighter shards sit idle.

Hikari pending and PostgreSQL lock waits were **0 at every sample of every run
in both arms**, so neither arm was pool- or lock-limited. No CPU bottleneck is
claimed; the CP CPU samples are sparse and non-monotonic.

## 14. Failure / Recovery Observations

Tested: `aKeyWhoseShardNeverRunsStaysUnpublishedButIsNotLost` (MEASURED).

With shard *s* never running while all others do:

- **Events are not lost.** They remain in `job_events` with `published_at IS
  NULL`, exactly as during a Kafka outage.
- **Only that shard's keys are affected.** Every remaining unpublished row
  belonged to shard *s*; other shards drained completely.
- **Events are not duplicated.** `markPublished` is still guarded.
- **The key is stuck until something covers that shard.** There is no takeover.
- **Recovery preserves ordering.** Once a publisher runs shard *s*, its key
  publishes in full sequence order.

**Limitation, stated plainly: the prototype has no failure-recovery or
rebalancing mechanism.** Shard coverage is implicit — shard *i* is covered
because thread *i* exists in this JVM. If a publishing thread dies, that shard
stops and its keys stall indefinitely. Nothing detects this, nothing reassigns
it, and no other publisher takes over. That is acceptable for a prototype and
disqualifying for production.

Not tested: mid-drain thread death, control-plane crash and restart mid-batch,
or concurrency changing between ticks.

## 15. Limitations

1. **Single instance only, and this is the most serious limitation.** Ownership
   holds because *one* JVM runs exactly one thread per shard. Two control-plane
   replicas would each run a full set of shards, two threads would own shard 3,
   and the boundary would vanish — quite possibly with ordering no better than
   the baseline. **This was not tested**, and the prototype must not be read as
   a distributed solution.
2. **No failure recovery or rebalancing** (§14).
3. **Concurrency is the shard count.** Changing `publisher-concurrency` re-hashes
   every key to a different shard. Mid-flight that could put a key's events in
   two shards at once, which is exactly the condition the design exists to
   prevent. Untested, and a real hazard.
4. **`hashtext` is not a stable documented hash** across PostgreSQL major
   versions. An upgrade could silently re-shard every key.
5. **Throughput depends on hash balance.** At C=8 over 20 keys the imbalance was
   1:3 and cost ~7%. Fewer keys than shards would leave shards permanently idle;
   a hot key would bound the whole drain regardless of concurrency.
6. **One workload shape.** 20 keys, uniform 500 events each, interleaved ids.
   Skewed key distributions were not tested and are the case most likely to hurt.
7. **Live-run ownership evidence is ambiguous at the margin** (§11): one
   ≤100-row pass group per prototype run contained two shards and cannot be
   distinguished from a timestamp collision. The proof rests on the concurrent
   claim tests, not on this.
8. **Only sequential sends tested.** `async-sends` stayed false in all 14 runs,
   deliberately. Interaction between sharding and async sends is untested.
9. **Index behaviour not measured.** The shard predicate filters rows found via
   an id-ordered partial index; on a large backlog a shard may scan far more
   index entries to fill a batch. At 10,000 rows this did not bite, and it was
   not measured at scale.
10. **Single broker, RF=1**, and the six `k8s_*` containers from Day 6 were
    present throughout, as in Days 17–22.

## 16. Conclusion

Against the Day 23 success criteria:

1. **Can publisher responsibility be partitioned by outbox key?** **Yes**
   (MEASURED). A hash predicate in the claim query partitions the outbox; the
   four shards were shown to claim disjoint, exhaustive row sets.
2. **Can multiple publishers run concurrently without a key being owned by
   two?** **Yes, within one instance** (MEASURED, by test, with both claims held
   simultaneously). Not established across instances (§15.1).
3. **Does the prototype preserve per-key ordering at C=2, 4 and 8?** **Yes —
   0 violations in all six runs**, against 3,522 / 4,492 / 4,781 for the paired
   baseline, confirmed independently Kafka-side and database-side.
4. **Does it preserve at-least-once correctness?** **Yes** (MEASURED): 0
   duplicates, 0 missing, 0 unpublished, 0 DLQ, 0 Kafka errors across all 14
   runs; the mark-published guard is unchanged.
5. **What happens when an ownership domain fails?** Its keys stall indefinitely
   and nothing recovers them automatically; nothing is lost or duplicated, and
   ordering survives eventual recovery (§14).
6. **What throughput cost?** ~1.6% at C=2, indistinguishable at C=4, ~6.9% at
   C=8 — the last INFERRED to come from shard load imbalance, since claim cost
   actually fell.
7. **Promising enough for a future production design?** **Yes, as a direction**
   — it converts a measured correctness defect into a measured, modest
   throughput cost. **No, as this implementation**, because of §15.1–15.4.
8. **What remains unproven?** Everything in §15, above all multi-instance
   behaviour, failure recovery, concurrency changes, and skewed keys.

**No production default was changed.** `key-aware-sharding` ships `false`,
`async-sends` ships `false`, `publisher-concurrency` ships `1`. The prototype is
not production-ready and is not claimed to be.

What Day 23 does settle is the question Day 22 left open: the ordering loss was
**not** an inevitable price of publisher parallelism. It was a consequence of
claiming by id range, and changing the claim predicate removes it.

## 17. Future Production Designs

**HYPOTHESIS** in every case; none implemented.

1. **Explicit shard column with a pinned hash** (Option B). Store the shard at
   insert time using an application-computed, version-independent hash, indexed
   as `(shard, id) WHERE published_at IS NULL`. Fixes §15.4 and §15.9, at the
   cost of a migration. The natural successor to this prototype.
2. **Shard leases in PostgreSQL.** Each publisher takes a short renewable lease
   on a shard, in the same style as the existing job leases. Gives failure
   detection and takeover (§15.2) and makes multi-instance safe (§15.1), since a
   shard has one lease-holder. Reuses a mechanism the codebase already has.
3. **Decouple shard count from concurrency.** Fix a large shard count (say 64)
   independent of thread count, and assign shards to threads. Removes the
   re-hash hazard in §15.3 and lets concurrency change without moving keys.
4. **Partition-aware ownership.** Align shards with Kafka partitions so one
   publisher owns one partition end to end. Caps concurrency at the partition
   count but makes ordering follow from the topic layout.
5. **Consumer-side sequencing.** Carry a per-key sequence number and let
   consumers reorder or detect gaps. Independent of all the above and useful
   regardless, since at-least-once already requires consumers to deduplicate.

Before any of these: measure a **skewed** key distribution (§15.6) and
**multi-instance** behaviour (§15.1). The current evidence comes from a uniform
20-key workload on a single instance, and both are exactly where this design is
most likely to disappoint.
