# Day 21 — Kafka Producer Idempotence and Per-Key Ordering

Every number is labelled **MEASURED** (directly observed), **CALCULATED**
(derived arithmetically from measured values), **INFERRED** (a reasonable
interpretation, not directly proven), or **HYPOTHESIS** (proposed, still
untested). Raw artefacts live outside the repository under the session
scratchpad `results21/<label>/`.

## 1. Objective

Day 20 made the outbox publisher submit an entire batch to the Kafka producer
before awaiting any acknowledgement, and closed with one unanswered correctness
question: it had *inferred* from documented client defaults that producer
idempotence — and therefore per-partition ordering — still held, without
measuring it.

Day 21 answers, experimentally:

1. What producer configuration is actually running?
2. Is idempotence actually enabled?
3. Does asynchronous sending preserve per-key ordering?
4. Are duplicate or missing event IDs observed?
5. Does retry preserve event identity?
6. Does asynchronous mode alter any required correctness property?

This is a correctness experiment. No throughput optimisation was attempted and
no default was changed.

## 2. Hypothesis

- **H1** — `enable.idempotence` resolves to `true`, because `acks=all` is set
  and nothing conflicts with it. *(Day 20's inference, now under test.)*
- **H2** — Per-key ordering holds in both modes, because events for one
  aggregate share a key and therefore a partition, and idempotence preserves
  per-partition order at up to 5 in-flight requests.
- **H3** — No duplicates or losses in either mode.
- **H4** — Asynchronous mode changes no required correctness property.

H4 turned out to be **wrong in a specific, reproducible way** (§9, §10).

## 3. Configuration

### Producer configuration actually running (MEASURED)

Read from the `ProducerConfig values:` dump the Kafka client itself logs when it
constructs the producer, captured from the running control-plane container. This
is the resolved configuration, defaults included — not a restatement of the YAML:

```
acks                                  = -1
enable.idempotence                    = true
retries                               = 2147483647
max.in.flight.requests.per.connection = 5
linger.ms                             = 5
batch.size                            = 16384
delivery.timeout.ms                   = 10000
request.timeout.ms                    = 4000
max.block.ms                          = 5000
transactional.id                      = null
compression.type                      = none
```

Note `acks = -1`, which is how the client represents `all`. `transactional.id`
is null: the producer is **idempotent, not transactional**. Only
`acks`, `max.block.ms`, `delivery.timeout.ms`, `request.timeout.ms` and the
serialisers are set explicitly in `application.yml`; `enable.idempotence`,
`retries` and `max.in.flight.requests.per.connection` are client defaults, which
is exactly why they needed measuring rather than assuming.

The same values are asserted independently in
`OutboxOrderingContract.theProducerIsConfiguredForIdempotentDelivery`, which
builds a `ProducerConfig` from the live `ProducerFactory`'s configuration map —
the same class `KafkaProducer` uses, so it reproduces the same resolution.

### TaskMesh configuration (identical across all arms)

| Setting | Value |
|---|---|
| Keys | 5 |
| Events per key | 200 |
| Total events | 1,000 |
| Kafka topic partitions | 3 |
| Outbox batch size | 100 |
| Poll interval | 100 ms |
| HikariCP max pool | 40 |
| Randomness / seed | **none — deterministic generator** |
| Publisher concurrency | 1 and 8 (varied) |
| `taskmesh.outbox.async-sends` | false and true (varied) |
| Git commit | `ef4d633` |

Asserted per run, aborting on mismatch (MEASURED, all four runs passed):
`env_async_sends` matching the arm, `env_concurrency` matching, and
`partitions_actual=3` from `kafka-topics.sh --describe`.

## 4. Methodology

### Event generation — deterministic, no job lifecycle

Events are inserted directly, so the experiment controls the exact key/sequence
structure and nothing depends on job scheduling timing:

```sql
INSERT INTO job_events (aggregate_id, event_type, payload)
SELECT 'ordkey' || k, 'JOB_QUEUED', json_build_object('seq', s)::jsonb
  FROM generate_series(1, 200) s, generate_series(0, 4) k
 ORDER BY s, k;
```

Ordering by `(seq, key)` spreads each key's events across the id space, so every
100-event batch holds 20 events for each of the 5 keys. That is the shape that
puts many records *for one key* in flight simultaneously under async sends — the
maximum-risk case, not a convenient one.

**Precondition, measured not assumed:** per key, database id order must equal
`seq` order, or the ordering question is meaningless. A window-function query
counted violations; it returned **0 in all four runs** (MEASURED), and the
harness aborts otherwise.

### Publication

The publisher is disabled while the backlog is built, then the control plane is
recreated with the publisher on at the concurrency and mode under test, with no
competing job load. Drain is complete when `count(*) - count(published_at) = 0`.

### Consumption — broker order, not consumer convenience

One `kafka-console-consumer` **pinned to a single partition** reading from
`earliest`, once per partition. A file's line order is therefore that
partition's offset order, and since a key maps to exactly one partition, a key's
records appear in one file already in delivery order. No offset parsing or
cross-partition sorting is involved, so the ordering result cannot be an
artefact of how records were collected.

The verifier reports, per key: record count, first/last sequence, partition set,
whether the sequence is ascending, whether it is exactly 1..200, inversion count
(`seq[i+1] <= seq[i]`), and missing sequence numbers.

### Automated tests

The same contract runs under both modes as JUnit tests against a real broker, at
publisher concurrency 1 (calling `publishPending()` directly). Ordering across
*concurrent* passes is deliberately excluded there, because Day 15 already
documented it as not guaranteed and including it would confound the question of
whether submit-all-then-wait reorders within a pass.

## 5. Sequential Results (`async-sends=false`)

| Arm | produced | consumed | unique IDs | duplicates | missing | out-of-order |
|---|---|---|---|---|---|---|
| C=1 | 1,000 | 1,000 | 1,000 | **0** | **0** | **0** |
| C=8 | 1,000 | 1,000 | 1,000 | **0** | **0** | **471** |

Per key (MEASURED):

| Arm | key | records | partition | ascending | exactly 1..200 | inversions |
|---|---|---|---|---|---|---|
| C=1 | ordkey0–4 | 200 each | 1,1,0,0,0 | **true** | **true** | **0** |
| C=8 | ordkey0 | 200 | 1 | false | false | 94 |
| C=8 | ordkey1 | 200 | 1 | false | false | 93 |
| C=8 | ordkey2 | 200 | 0 | false | false | 94 |
| C=8 | ordkey3 | 200 | 0 | false | false | 93 |
| C=8 | ordkey4 | 200 | 0 | false | false | 97 |

## 6. Asynchronous Results (`async-sends=true`)

| Arm | produced | consumed | unique IDs | duplicates | missing | out-of-order |
|---|---|---|---|---|---|---|
| C=1 | 1,000 | 1,000 | 1,000 | **0** | **0** | **0** |
| C=8 | 1,000 | 1,000 | 1,000 | **0** | **0** | **373** |

Per key (MEASURED):

| Arm | key | records | partition | ascending | exactly 1..200 | inversions |
|---|---|---|---|---|---|---|
| C=1 | ordkey0–4 | 200 each | 1,1,0,0,0 | **true** | **true** | **0** |
| C=8 | ordkey0 | 200 | 1 | false | false | 75 |
| C=8 | ordkey1 | 200 | 1 | false | false | 75 |
| C=8 | ordkey2 | 200 | 0 | false | false | 74 |
| C=8 | ordkey3 | 200 | 0 | false | false | 77 |
| C=8 | ordkey4 | 200 | 0 | false | false | 72 |

## 7. Ordering Analysis

Side by side (MEASURED):

| Publisher concurrency | sequential inversions | async inversions |
|---|---|---|
| **1** | **0** | **0** |
| **8** | **471** | **373** |

Three findings, and they need to be kept apart:

**(a) At publisher concurrency 1 — the shipped default — per-key ordering holds
in both modes, exactly.** Every one of the 5 keys was delivered as sequence
1..200 ascending on a single partition, in both arms. This is the direct answer
to Day 20's open question: submit-all-then-wait does **not** reorder a key's
events within a pass, even with 20 records for that key in flight at once, across
10 successive batches.

**(b) At publisher concurrency 8, per-key ordering is heavily violated — in both
modes.** Roughly 93–97 inversions per key sequentially and 72–77 async, out of
200 records per key. Nothing was lost or duplicated; the records simply arrive
out of sequence.

**(c) This violation is not caused by Day 20.** The sequential arm — the shipped
default send path — is the *worse* of the two at C=8 (471 vs 373). The cause is
concurrent publisher passes: `lockUnpublishedBatch` hands disjoint batches to C
threads with `SKIP LOCKED`, and those threads race, so a key's later events can
reach the broker before its earlier ones. Day 15 documented this trade-off in
prose when concurrency was introduced; **Day 21 is the first time it has been
measured.**

That 373 < 471 is consistent across all five keys, but rests on **one replicate
per arm**, so the direction is suggestive and the magnitude is not established
(§11). The claim that matters is not which is worse — it is that **neither
preserves per-key ordering at C=8**.

**A documentation defect this exposes.** `KafkaTopicsConfig` states that events
are keyed "so all events for one aggregate land on the same partition and a
consumer sees that job's history in order". The first half is verified (§7a:
every key occupied exactly one partition in all four runs). The second half is
**false whenever `publisher-concurrency > 1`**, which is not noted there. No code
was changed for this; it is recorded as a finding.

Days 17–20 all measured throughput at C=8…32. Those performance numbers are not
invalidated by this, but they were obtained in a configuration that does not
deliver per-key ordering.

## 8. Duplicate / Missing Analysis

| Arm | total produced | total consumed | unique event IDs | duplicates | missing |
|---|---|---|---|---|---|
| seq C=1 | 1,000 | 1,000 | 1,000 | 0 | 0 |
| async C=1 | 1,000 | 1,000 | 1,000 | 0 | 0 |
| seq C=8 | 1,000 | 1,000 | 1,000 | 0 | 0 |
| async C=8 | 1,000 | 1,000 | 1,000 | 0 | 0 |

`final_unpublished = 0` and `published_rows = 1000` in all four runs, and
`cp_log_error_lines = 0` (MEASURED).

**No duplicate event IDs were observed in any run, and no event was missing.**

That sentence is the whole claim. It is **not** a demonstration of exactly-once
delivery: TaskMesh publishes **at-least-once** by design, and a crash between
Kafka's acknowledgement and the `published_at` commit would re-send an event on
the next pass. These runs contained no such crash, so they had no opportunity to
produce the duplicate the design permits. Observing zero duplicates over 4,000
events bounds how often it happens under these conditions; it proves nothing
about the guarantee.

## 9. Failure / Retry Analysis

Deterministic, application-level failure injection, per the instruction to
prefer that over damaging the broker: one record in a key's sequence carries an
oversized payload exceeding `max.request.size=2048`, so the producer rejects
that record while accepting its neighbours. Five events for one key, sequence
1..5, with **seq 3 oversized**.

Results (MEASURED, asserted as tests):

| | sequential | async |
|---|---|---|
| published on the first pass | **2** (seq 1, 2) | **4** (seq 1, 2, 4, 5) |
| rejected event marked published | no | no |
| events left unpublished after retry | 0 | 0 |
| event ID stable across failure and retry | yes | yes |
| events lost | 0 | 0 |
| events duplicated | 0 | 0 |
| **order a consumer observes** | **1, 2, 3, 4, 5** | **1, 2, 4, 5, 3** |

All six required properties hold in both modes: the failed event stays
unpublished, stays eligible, publishes on a later pass, keeps its ID, does not
revert a successful event, and loses nothing.

But the observed order differs, and this is the substantive Day 21 finding
beyond the configuration check. The sequential path stops at the first failure,
so seq 4 and 5 are never attempted while 3 is blocked; when 3 becomes sendable
the same pass publishes 3, 4, 5 in id order and the consumer never sees the key
out of sequence. The batched path attempts all five, so 4 and 5 are acknowledged
while 3 is rejected, and 3 arrives *after* them.

Day 20 documented losing the "prefix" property. It did not connect that to
per-key ordering. **The concrete cost is that a partial failure reorders a key's
events under async sends, and cannot under sequential sends.** Both orders are
now pinned by `OutboxSequentialFailureOrderingTests` and
`OutboxAsyncFailureOrderingTests`, which declare the expected sequence
explicitly.

## 10. Conclusion

Classified separately, as required:

```
Producer idempotence:          VERIFIED
Per-key ordering:              VERIFIED at publisher-concurrency = 1 (the default),
                               in both send modes.
                               NOT VERIFIED at publisher-concurrency = 8 — measurably
                               VIOLATED in both modes (sequential 471, async 373
                               inversions over 1,000 events).
No duplicate IDs observed:     YES (0 across 4,000 published events in four runs,
                               plus every automated test)
No missing events observed:    YES (0; final unpublished = 0 in all four runs)
Retry correctness:             VERIFIED (both modes: unpublished on failure, eligible,
                               later published, ID stable, no revert, no loss)
```

**Does asynchronous mode alter any required correctness property?**

- Idempotence, duplicates, loss, and per-key ordering at the default
  concurrency: **no change** — the two modes are indistinguishable.
- **Ordering across a partial failure: yes, it changes.** Sequential delivers
  `1,2,3,4,5`; async delivers `1,2,4,5,3`. MEASURED.

**Recommendation on the default:** `taskmesh.outbox.async-sends` remains
**`false`**, unchanged. Day 21 removes the idempotence objection but adds a
concrete one, and the throughput case from Day 20 does not by itself justify
accepting reordering-on-failure. Enabling it should be a deliberate decision by
someone who has read §9.

A note on what this does *not* say: Kafka was never shown to be misbehaving.
Idempotence is on and working. The ordering losses measured here come from
TaskMesh's own concurrency and failure handling, not from the broker.

## 11. Limitations

1. **One replicate per experimental arm.** The four runs are single
   observations. The zero-inversion results at C=1 are exact (0 is 0 in 2,000
   events), and the C=8 violation is far too large to be noise, but the
   *comparison* 373 vs 471 is not established and should not be quoted as an
   effect size.
2. **Partition 2 received no records.** The 5 fixed keys hashed onto partitions
   0 and 1 only. Per-key ordering is unaffected — each key still had a single
   partition — but the experiment did not exercise a 3-partition spread, and the
   result does not depend on partition count.
3. **Ordering at concurrency other than 1 and 8 was not measured.** Whether the
   violation grows, saturates, or behaves differently at C=16/24/32 is untested.
4. **Zero duplicates is an observation under no-crash conditions**, not a
   guarantee (§8). No process kill between acknowledgement and commit was
   attempted, which is the scenario the at-least-once design actually admits.
5. **Only one failure mode was injected** — a producer-side record rejection.
   Broker unavailability mid-batch, a timeout on some records but not others, and
   partition leader change were not tested, and could reorder or duplicate
   differently.
6. **Idempotence was verified as configuration, not as behaviour under retry.**
   `enable.idempotence = true` is measured from the running client, and no retry
   storm was induced to observe the producer actually deduplicating or preserving
   order across an internal retry. That is the gap between "configured for
   idempotent delivery" and "observed to deliver idempotently".
7. **Single broker, RF=1, `min.insync.replicas=1`.** Idempotence and ordering
   under leader election or an under-replicated partition are untested.
8. **A second TaskMesh stack was running throughout** — six `k8s_*` containers
   from the Day 6 Kubernetes work, as in Days 17–20. It is background load, not
   a functional confound for a correctness experiment, but the host was shared.
9. **The consumer read each partition once, to its end.** No consumer restart,
   rebalance, or offset reset was exercised.
