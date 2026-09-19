# Day 22 — Publisher Concurrency vs Per-Key Ordering

Every number is labelled **MEASURED** (directly observed), **CALCULATED**
(derived from measured values), **INFERRED** (interpretation supported by
measurements but not directly proven), or **HYPOTHESIS** (proposed, untested).
Raw artefacts live outside the repository under the session scratchpad
`results22/<label>/`.

## 1. Objective

Day 21 found per-key ordering intact at `publisher-concurrency = 1` and violated
at 8, in *both* send modes — so the cause was not Day 20's asynchronous sends.
Day 22 asks:

> How does `publisher-concurrency` affect outbox throughput and per-key event
> ordering, and is there a measurable concurrency point where throughput gains
> come at the cost of ordering violations?

No production code and no configuration was changed. One test was added (§13).

## 2. Hypothesis

- **H-RACE** — Concurrent publisher passes claim different batches that each
  contain events for the same key, so those batches can reach Kafka in an order
  that disagrees with event id order.
- **H-THRESHOLD** — Ordering degrades progressively as C rises.
- **H-ASYNC** — Async sends do not independently affect ordering, controlling
  for C.

H-THRESHOLD turned out to be **wrong**: degradation is a cliff at C=2, not a
gradient (§9).

## 3. Experimental Matrix

7 concurrency levels × 2 send modes × 2 replicates = **28 runs, all valid**.

| | C=1 | C=2 | C=4 | C=8 | C=16 | C=24 | C=32 |
|---|---|---|---|---|---|---|---|
| sequential | 2 | 2 | 2 | 2 | 2 | 2 | 2 |
| async | 2 | 2 | 2 | 2 | 2 | 2 | 2 |

The prompt said "8 concurrency levels" while listing 7 values; the 7 listed
values were used.

## 4. Environment

| | |
|---|---|
| Host | Windows 11, 16 logical CPUs |
| Docker | Server 29.8.0, **16 CPUs, 8,162,320,384 B (7.60 GiB)** allocated |
| PostgreSQL | 16-alpine |
| Kafka | 4.2.1, single-node KRaft, **replication factor 1**, `min.insync.replicas=1` |
| Control plane | Spring Boot 4.1.1 / Java 21 |
| Git commit | `ff6db67` |

Producer configuration, MEASURED from the running client's `ProducerConfig
values:` dump, identical in every arm:

```
enable.idempotence = true    acks = -1    retries = 2147483647
max.in.flight.requests.per.connection = 5    linger.ms = 5
```

**Standing confound:** the six `k8s_*` containers from the Day 6 Kubernetes work
were present throughout, as in Days 17–21 (MEASURED: 6 containers).

**Host state changed during the matrix.** See §14, item 1 — this is the most
important limitation on the throughput numbers.

## 5. Methodology

Reuses the Day 18–21 infrastructure rather than a parallel framework: the Day 21
deterministic generator and per-partition consumer, plus the Day 18–20
drain-rate method and the Day 19 per-tick instrumentation (already committed).

**Workload**, identical across all 28 runs: 20 keys × 500 events = 10,000
events; 3 partitions; batch size 100; poll interval 100 ms; Hikari 40; **no
randomness, no seed** — events are generated in SQL:

```sql
INSERT INTO job_events (aggregate_id, event_type, payload)
SELECT 'ordkey' || k, 'JOB_QUEUED', json_build_object('seq', s)::jsonb
  FROM generate_series(1, 500) s, generate_series(0, 19) k
 ORDER BY s, k;
```

Ordering by `(seq, key)` means each 100-event batch holds exactly **5 events per
key for all 20 keys** — so every concurrent pass carries every key. That is the
structure that makes the race observable rather than incidental.

**Publication.** Publisher disabled while the backlog is built; control plane
then recreated with the publisher on at the concurrency and mode under test,
with zero competing job load.

**Consumption.** One consumer pinned per partition, from earliest. A file's line
order is therefore that partition's offset order, and since a key maps to one
partition, a key's records appear in one file already in delivery order — no
offset parsing or cross-partition sorting, so ordering results cannot be an
artefact of collection.

**Metric definitions, stated before use:**

- **transition** — an adjacent pair within one key's delivered sequence; a key
  with N records has N−1 transitions.
- **out-of-order** — a transition where `seq[i+1] <= seq[i]`.
- **ordering violation rate** — out-of-order transitions ÷ total transitions,
  over all keys in the run.
- **max displacement** — `max |seq_at_index_i − (i+1)|`, i.e. how far the worst
  record sits from where it belonged.
- **throughput (tick)** — `(C × 100) / (mean tick duration + 100 ms poll)`, from
  the Day 19 per-tick record, over saturated ticks minus 2 warm-up.
- **throughput (backlog)** — T0/T1 anchored inside the 200 ms PostgreSQL-clock
  series, as in Days 18–20.
- **publish inversion** — an event whose publishing transaction began before
  that of the preceding event for the same key (§12).

## 6. Preconditions

Checked per run, aborting the run on failure (MEASURED, all 28 passed):

- `events_inserted = 10000`
- **per key, database id order equals seq order** — a window-function query
  counted violations and returned **0 in all 28 runs**. Without this the
  ordering question would be meaningless.
- `env_async_sends`, `env_concurrency`, `env_poll_ms` and `partitions_actual`
  all matched the intended arm.

Key→partition spread (MEASURED): 6 / 8 / 6 keys across partitions 0 / 1 / 2, so
all three partitions were exercised and no key spanned two.

## 7. Raw Results

All 28 runs. `tp_t` = tick method, `tp_b` = backlog method, `nTk`/`nBl` = sample
counts behind each, `OOO` = out-of-order transitions, `disp` = max displacement,
`pInv` = publish inversions.

| C | Mode | Rep | tp_t | nTk | tp_b | nBl | drain s | consumed | uniq | dup | miss | unpub | DLQ | OOO | keysV | disp | rate | pInv |
|--:|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | seq | 1 | — | 0 | 109 | 429 | 88.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | **0** | 0 | 0 | 0.0000 | **0** |
| 1 | seq | 2 | — | 0 | 102 | 461 | 95.4 | 10000 | 10000 | 0 | 0 | 0 | 0 | **0** | 0 | 0 | 0.0000 | **0** |
| 1 | async | 1 | — | 0 | 613 | 68 | 16.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | **0** | 0 | 0 | 0.0000 | **0** |
| 1 | async | 2 | — | 0 | 798 | 48 | 12.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | **0** | 0 | 0 | 0.0000 | **0** |
| 2 | seq | 1 | 201 | 47 | 199 | 227 | 48.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | 3756 | 20 | 5 | 0.3764 | 256 |
| 2 | seq | 2 | 253 | 47 | 250 | 181 | 39.4 | 10000 | 10000 | 0 | 0 | 0 | 0 | 3529 | 20 | 5 | 0.3536 | 249 |
| 2 | async | 1 | 1160 | 47 | 1126 | 29 | 8.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | 1151 | 20 | 6 | 0.1153 | 336 |
| 2 | async | 2 | 1543 | 47 | 1557 | 19 | 6.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | 2388 | 20 | 6 | 0.2393 | 237 |
| 4 | seq | 1 | 365 | 22 | 366 | 117 | 26.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4277 | 20 | 15 | 0.4286 | 607 |
| 4 | seq | 2 | 496 | 22 | 488 | 83 | 19.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4440 | 20 | 15 | 0.4449 | 696 |
| 4 | async | 1 | 1760 | 22 | 1608 | 15 | 6.2 | 10000 | 10000 | 0 | 0 | 0 | 0 | 2223 | 20 | 16 | 0.2227 | 506 |
| 4 | async | 2 | 2878 | 22 | — | 3 | 3.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | 3254 | 20 | 15 | 0.3261 | 578 |
| 8 | seq | 1 | 647 | 10 | 610 | 60 | 15.0 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4790 | 20 | 35 | 0.4800 | 969 |
| 8 | seq | 2 | 938 | 10 | 948 | 39 | 10.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4702 | 20 | 35 | 0.4711 | 779 |
| 8 | async | 1 | 3205 | 10 | — | 0 | 3.2 | 10000 | 10000 | 0 | 0 | 0 | 0 | 2497 | 20 | 35 | 0.2502 | 595 |
| 8 | async | 2 | 5226 | 10 | — | 0 | 2.0 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4105 | 20 | 35 | 0.4113 | 839 |
| 16 | seq | 1 | 1100 | 4 | 1108 | 29 | 8.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4527 | 20 | 74 | 0.4536 | 824 |
| 16 | seq | 2 | 1642 | 4 | 1714 | 15 | 6.0 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4826 | 20 | 75 | 0.4836 | 836 |
| 16 | async | 1 | 3919 | 4 | — | 0 | 2.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | 2758 | 20 | 74 | 0.2764 | 773 |
| 16 | async | 2 | 8078 | 4 | — | 0 | 1.4 | 10000 | 10000 | 0 | 0 | 0 | 0 | 3866 | 20 | 75 | 0.3874 | 917 |
| 24 | seq | 1 | 1527 | 2 | 1600 | 16 | 6.4 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4628 | 20 | 113 | 0.4637 | 916 |
| 24 | seq | 2 | 2178 | 2 | — | 6 | 4.2 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4491 | 20 | 114 | 0.4500 | 1049 |
| 24 | async | 1 | 4029 | 2 | — | 0 | 2.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | 2678 | 20 | 113 | 0.2683 | 884 |
| 24 | async | 2 | 7160 | 2 | — | 0 | 1.2 | 10000 | 10000 | 0 | 0 | 0 | 0 | 3621 | 20 | 114 | 0.3628 | 1004 |
| 32 | seq | 1 | 2104 | 1 | 1785 | 10 | 5.2 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4572 | 20 | 150 | 0.4581 | 867 |
| 32 | seq | 2 | 2893 | 1 | — | 1 | 3.4 | 10000 | 10000 | 0 | 0 | 0 | 0 | 4582 | 20 | 154 | 0.4591 | 981 |
| 32 | async | 1 | 7624 | 1 | — | 0 | 1.6 | 10000 | 10000 | 0 | 0 | 0 | 0 | 2435 | 20 | 154 | 0.2440 | 976 |
| 32 | async | 2 | 10828 | 1 | — | 0 | 0.8 | 10000 | 10000 | 0 | 0 | 0 | 0 | 3484 | 20 | 152 | 0.3491 | 1015 |

`—` means the method had too few samples to be usable, reported as unavailable
rather than estimated. At C=1 the `concurrency == 1` code path logs the short
DEBUG line without `tick_us`, so the tick method does not apply there; the
backlog method covers it well (429–461 samples).

## 8. Throughput Analysis

**Method agreement.** Where both methods have samples they agree within 1–9%
(C=2: 201/199, 253/250; C=4: 365/366; C=8: 647/610; C=16: 1100/1108; C=24:
1527/1600). At C=32 they diverge 18% (2104 vs 1785) on 1 tick and 10 samples —
that is where measurement quality collapses, and it is visible in the data
rather than asserted.

**Replicate 2 was faster than replicate 1 in every one of the 14 arms**, by
1.26×–2.06× (MEASURED). Because all of r1 ran before all of r2, replicate order
is confounded with host state, so the r1–r2 spread is **drift, not random
variation** (§14). The valid comparison is therefore *within* a replicate.

Within-replicate throughput (MEASURED):

| C | seq r1 | seq r2 | async r1 | async r2 | async/seq r1 | async/seq r2 |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 109 | 102 | 613 | 798 | 5.62 | 7.82 |
| 2 | 201 | 253 | 1160 | 1543 | 5.77 | 6.10 |
| 4 | 365 | 496 | 1760 | 2878 | 4.82 | 5.80 |
| 8 | 647 | 938 | 3205 | 5226 | 4.95 | 5.57 |
| 16 | 1100 | 1642 | 3919 | 8078 | 3.56 | 4.92 |
| 24 | 1527 | 2178 | 4029 | 7160 | 2.64 | 3.29 |
| 32 | 2104 | 2893 | 7624 | 10828 | 3.62 | 3.74 |

- **Throughput rises monotonically with C through C=32 in both modes and both
  replicates.** No flattening was observed anywhere in the tested range. The
  sequential marginal gain per step declines steadily (1.84×, 1.82×, 1.77×,
  1.70×, ~1.39×, 1.38× in r1) but never reaches 1.
- **The async advantage shrinks as C rises** — ~5.6× at C=1 down to ~2.6–3.7× at
  C=24–32 — consistent with Day 20's independent finding (5.06× at C=8 →
  2.53× at C=32).
- Above C=8 the tick method rests on ≤4 ticks and the backlog method on ≤29
  samples (0 for async), so **individual high-C values should not be ranked
  against each other**; e.g. async C=16 (3919–8078) and C=24 (4029–7160)
  overlap almost completely and are indistinguishable here.

## 9. Ordering Analysis

| C | seq OOO (range) | seq rate | async OOO (range) | async rate | keys violated | max displacement |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | **0** (0–0) | **0.0000** | **0** (0–0) | **0.0000** | 0 / 20 | 0 |
| 2 | 3642 (3529–3756) | 0.3650 | 1770 (1151–2388) | 0.1773 | 20 / 20 | 5–6 |
| 4 | 4358 (4277–4440) | 0.4367 | 2738 (2223–3254) | 0.2744 | 20 / 20 | 15–16 |
| 8 | 4746 (4702–4790) | 0.4756 | 3301 (2497–4105) | 0.3308 | 20 / 20 | 35 |
| 16 | 4676 (4527–4826) | 0.4686 | 3312 (2758–3866) | 0.3319 | 20 / 20 | 74–75 |
| 24 | 4560 (4491–4628) | 0.4569 | 3150 (2678–3621) | 0.3156 | 20 / 20 | 113–114 |
| 32 | 4577 (4572–4582) | 0.4586 | 2960 (2435–3484) | 0.2965 | 20 / 20 | 150–154 |

**Ordering degradation is a cliff at C=2, not a gradient.** C=1 gives exactly 0
violations in all four runs (both modes, both replicates); C=2 immediately gives
3,529–3,756 (sequential) or 1,151–2,388 (async) out of 9,980 transitions, with
**all 20 keys affected**. H-THRESHOLD is **not supported** — there is no
intermediate concurrency at which ordering is partially intact.

**The violation rate saturates; max displacement does not.** The rate plateaus
by C=4 (sequential ~0.44–0.48) and C=8 (async ~0.30–0.33), and does not grow
further through C=32. Max displacement, by contrast, grows linearly and
predictably:

| C | 5×(C−1) predicted | sequential | async |
|--:|--:|--:|--:|
| 1 | 0 | 0 | 0 |
| 2 | 5 | 5 | 6 |
| 4 | 15 | 15 | 16 |
| 8 | 35 | **35** | **35** |
| 16 | 75 | 75 | 75 |
| 24 | 115 | 114 | 114 |
| 32 | 155 | 154 | 154 |

Each batch of 100 events holds exactly 100/20 = **5 events per key**, so with C
batches in flight a key's events span 5C positions and one event can be
displaced by at most 5(C−1) (CALCULATED). The measured values match that bound
exactly or within 1 at every concurrency, **and are identical in both send
modes**. This is the quantitative signature of the mechanism in §12.

So the trade-off is not "more concurrency, more disorder" in a simple sense: the
*proportion* of misordered transitions stops rising after C≈4–8, while the
*distance* a record can be displaced keeps growing in direct proportion to C.

## 10. Duplicate / Loss Analysis

**All 28 runs passed every correctness gate** (MEASURED):

| Check | Result, all 28 runs |
|---|---|
| events produced | 10,000 |
| events consumed | 10,000 |
| unique event IDs | 10,000 |
| duplicate event IDs | **0** |
| missing event IDs | **0** |
| final unpublished | **0** |
| DLQ jobs | **0** |
| `record.error.total` | **0** |
| control-plane error/exception log lines | **0** |

Not one run failed a correctness gate, so no run required separate
classification. Zero duplicates across 280,000 published events is an
observation under no-crash conditions — it is **not** exactly-once. TaskMesh
publishes at-least-once by design, and a crash between Kafka's acknowledgement
and the `published_at` commit would re-send. No such crash occurred in these
runs, so they had no opportunity to produce the duplicate the design permits.

## 11. Resource Analysis

Maxima over samples, averaged across replicates (MEASURED):

| mode | C | Hikari active | **Hikari pending** | acquire ms | **PG lock waits** | PG CPU% | CP CPU% | CP mem MiB | threads | Kafka queue ms | Kafka latency ms | records/req |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| seq | 1 | 1 | **0** | 0.22 | **0** | 12 | 210 | 645 | 30 | 5.45 | 1.87 | 1.0 |
| seq | 2 | 2 | **0** | 0.11 | **0** | 16 | 61 | 601 | 32 | 5.36 | 2.00 | 2.0 |
| seq | 4 | 4 | **0** | 0.08 | **0** | 6 | 79 | 599 | 34 | 5.38 | 2.51 | 3.9 |
| seq | 8 | 8 | **0** | 0.03 | **0** | 7 | 64 | 589 | 38 | 5.30 | 3.27 | 7.3 |
| seq | 16 | 8 | **0** | 0.03 | **0** | 7 | 42 | 548 | 46 | 5.58 | 4.37 | 13.0 |
| seq | 24 | — | **0** | 0.04 | **0** | — | — | — | 54 | 5.53 | 5.16 | 17.6 |
| seq | 32 | — | **0** | 0.07 | **0** | — | — | — | 62 | 6.12 | 7.04 | 21.0 |
| async | 1 | — | **0** | 0.09 | **0** | 3 | 40 | 596 | 30 | 5.48 | 6.61 | 75.0 |
| async | 2 | — | **0** | 0.07 | **0** | 2 | 9 | 574 | 32 | 6.08 | 9.59 | 123.6 |
| async | 4–32 | — | **0** | 0.02–0.05 | **0** | — | — | — | 34–62 | 6.99–34.77 | 15.96–57.45 | 147–239 |

What the measurements support:

- **Not connection-pool limited.** `hikaricp.connections.pending` was **0 at
  every sample of every one of the 28 runs**, and acquire time never exceeded
  0.22 ms.
- **Not PostgreSQL-lock limited.** `wait_event_type = 'Lock'` was **0 at every
  sample of every run** — `SKIP LOCKED` skips rather than blocks, by design.
- **Not Kafka-error limited.** Zero record errors everywhere.
- Threads are exactly `30 + C` in every arm, so concurrency allocates threads as
  intended and nothing else.
- Kafka per-record queue time rises with C under async (5.48 → 34.77 ms) while
  staying ~5.3–6.1 ms under sequential — consistent with Day 20's finding that
  async amortises linger across a larger batch rather than eliminating it.

What the measurements do **not** support: any claim about a CPU bottleneck.
Container CPU is missing (`—`) for most high-C arms because the probe is
throttled to every second loop pass and those drains finish first. The one
striking value — 210% CP CPU at sequential C=1 — comes from the longest drain
(88.8 s) and therefore the most samples, so it is not comparable with arms that
were sampled once or not at all. **No bottleneck is claimed from this data.**

Nothing here explains the ordering result, which is the point: ordering loss is
an application-level sequencing property, not resource exhaustion.

## 12. SKIP LOCKED / Publisher-Pass Evidence

This was obtained **without instrumenting anything**.
`JobEventRepository.markPublished` sets `published_at = now()`, and PostgreSQL
fixes `now()` at *transaction start* — so every event marked by one pass carries
one identical timestamp, and different passes carry different ones. That makes
`published_at` a pass identifier that production code already writes. (§13 adds
a test pinning this property, since it is load-bearing for the analysis.)

Pass structure, replicate 1 (MEASURED):

| mode | C | passes | events/pass | **keys/pass** | **pass pairs committing out of id order** | widest inverted id span |
|---|--:|--:|--:|--:|--:|--:|
| seq | 1 | 100 | 100 | 20.0 | **0** | 0 |
| seq | 2 | 98 | 102 | 20.0 | 9 | 199 |
| seq | 4 | 101 | 99 | 20.0 | 24 | 399 |
| seq | 8 | 95 | 105 | 20.0 | 36 | 499 |
| seq | 16 | 100 | 100 | 20.0 | 36 | 813 |
| seq | 24 | 98 | 102 | 20.0 | 37 | 1086 |
| seq | 32 | 100 | 100 | 19.7 | 34 | 429 |
| async | 1 | 100 | 100 | 20.0 | **0** | 0 |
| async | 2 | 99 | 101 | 20.0 | 12 | 199 |
| async | 4 | 97 | 103 | 20.0 | 21 | 299 |
| async | 8 | 97 | 103 | 20.0 | 22 | 323 |
| async | 16 | 99 | 101 | 20.0 | 32 | 388 |
| async | 24 | 99 | 101 | 20.0 | 39 | 1099 |
| async | 32 | 99 | 101 | 19.9 | 45 | 2699 |

The chain, each link measured, with C=1 as the control that shows every link at
zero:

1. **Every pass claims ~100 events spanning all 20 keys** — `keys/pass` is
   19.7–20.0 at every concurrency. So any two concurrent passes necessarily hold
   events for the same keys.
2. **At C>1, passes commit out of id order** — 9–45 pass pairs per run where a
   lower id-range pass committed later than a higher one. At C=1: **0**.
3. **Therefore a key's later events can be published before its earlier ones** —
   `publish_inversions` 237–1,049 at C>1, and **0** at C=1 (§7).
4. **Therefore the consumer observes out-of-order sequences** — §9.
5. **The displacement bound matches the batch geometry** — max displacement =
   5(C−1) = (events per key per batch) × (concurrent batches − 1), to within 1 at
   every C (§9).

Where ordering is lost, stated precisely: **between the outbox claim and the
Kafka producer, at the boundary between concurrent publisher passes.** Not in
Kafka. Kafka's per-partition guarantee held throughout — every key occupied
exactly one partition, idempotence was enabled, zero record errors, zero
duplicates, zero losses. TaskMesh hands Kafka a correctly-keyed but
incorrectly-*sequenced* stream when C>1, and Kafka faithfully preserves the
order it is given.

Item 5 is **CALCULATED** agreement between a geometric prediction and measured
data; the causal attribution in 1–4 is **INFERRED** from the C=1 control plus the
pass-commit evidence, not from a direct trace of two passes racing over one
specific key.

## 13. Sequential vs Async Comparison

Controlling for C, async sends:

- **do not change whether ordering breaks.** Both modes are exactly 0 at C=1 and
  both break at C=2. The threshold is identical.
- **do not change the displacement bound.** Max displacement is 5(C−1) in both
  modes, identical to within 1 at every C and *exactly* equal at C=8, 16, 24, 32.
- **do produce fewer out-of-order transitions at the same C** — e.g. C=8: 3,301
  async vs 4,746 sequential; C=32: 2,960 vs 4,577. Async rate ~0.30 vs
  sequential ~0.46. Direction is consistent across all six C>1 levels and both
  replicates, so the direction is reproducible; the magnitude is confounded by
  the host drift in §14 and by wide async replicate spread (C=8: 2,497–4,105), so
  no effect size is claimed.
- **are much faster at every C** (§8).

**H-ASYNC is supported for the properties that matter** — async does not
introduce, worsen, or shift the ordering failure. It reorders *fewer*
transitions, by the *same* maximum distance, at *much* higher throughput.

The one place async is worse remains Day 21's finding, unchanged by Day 22:
across a *partial send failure* async reorders a key (`1,2,4,5,3`) where
sequential cannot. That is a different mechanism from the concurrency race
measured here.

**A trade-off worth stating plainly** (MEASURED, from §8 and §9): async at C=1
delivered 613–798 events/s with **0 ordering violations**, which is comparable to
sequential at C=8 (647–938/s) with **4,702–4,790 violations**, and faster than
sequential at C=4 (365–496/s). Within this workload, async sends at concurrency 1
obtained roughly the throughput that raising sequential concurrency to 8 obtained,
without giving up per-key ordering. This is an observation about one workload on
one host, not a recommendation — and **no default was changed** (§15).

## 14. Limitations

1. **Host state drifted across the matrix, and it is confounded with replicate
   number.** Replicate 2 was faster in all 14 arms (1.26×–2.06×). Because all of
   r1 ran before all of r2, the r1/r2 spread measures elapsed time, not
   variance. Consequences: throughput *levels* are not comparable across
   replicates; the reported ranges overstate random error; and no significance
   test is attempted. What survives is that the *shape* — monotonic rise, async
   ≫ sequential, declining async advantage — reproduced in both replicates
   independently. Interleaving replicates per arm would have avoided this and is
   the fix for any repeat.
2. **Throughput at C≥16 rests on very few samples.** 4 ticks at C=16, 2 at C=24,
   **1 at C=32**; async has 0 backlog samples at C≥8 because the drain finishes
   inside the 3 s warm-up guard. Individual high-C throughput values must not be
   ranked against each other.
3. **Ordering results are far more robust than throughput results.** Max
   displacement reproduced to within 1 across modes and replicates, and the C=1
   zero is exact in all four runs. The out-of-order *counts*, however, vary with
   host speed (async C=8: 2,497 vs 4,105) and correlate with throughput, so they
   are dynamic rather than structural.
4. **CPU is not measured well enough to identify a bottleneck** (§11), and none
   is claimed.
5. **One workload shape only.** 5 events per key per batch is what produced the
   5(C−1) bound. A workload with fewer keys, more keys, or clustered rather than
   interleaved ids would change the displacement geometry, and that was not
   tested.
6. **Zero duplicates is a no-crash observation**, not a guarantee (§10). No
   process kill between acknowledgement and commit was attempted.
7. **The race was not traced directly.** §12 infers the mechanism from pass
   commit order plus the C=1 control; it does not capture two passes
   simultaneously holding one named key with timestamps proving overlap.
8. **Single broker, RF=1, `min.insync.replicas=1`.** Leader election and
   under-replicated partitions are untested.
9. **The six `k8s_*` containers remained present throughout** (§4), as in Days
   17–21 — a standing background load on a host with 7.60 GiB allocated.
10. **Kafka partition count fixed at 3.** With 20 keys over 3 partitions, several
    keys share a partition; whether partition count interacts with the ordering
    result is untested.

## 15. Conclusions

Answering the Day 22 questions, strictly from measurement:

1. **How does publisher concurrency affect throughput?** It increases it
   monotonically through C=32 in both modes, with no flattening observed in the
   tested range. Sequential: ~109 → ~2,104 /s (r1). Async: ~613 → ~7,624 /s
   (r1). Marginal gain per step declines but stays above 1.
2. **At what concurrency does per-key ordering begin to degrade?** **C=2,
   immediately and completely** — all 20 keys affected. C=1 is exactly 0 in both
   modes and both replicates. It is a cliff, not a gradient.
3. **Does async sending independently affect ordering?** **No** for the threshold
   and the displacement bound, which are identical to sequential. It does produce
   consistently fewer out-of-order transitions at the same C (~0.30 vs ~0.46
   rate), reproducible in direction but not quantified.
4. **Are duplicate or missing events observed?** **No.** 0 duplicates, 0 missing,
   0 unpublished, 0 DLQ in all 28 runs — 280,000 published events. This is not a
   claim of exactly-once.
5. **Does SKIP LOCKED batch concurrency provide evidence for the race?** **Yes.**
   Every pass carries all 20 keys; at C>1, 9–45 pass pairs per run commit out of
   id order while C=1 has 0; publish inversions follow the same pattern; and the
   max displacement equals the batch geometry 5(C−1) to within 1. Ordering is lost
   **between the outbox claim and the producer, across concurrent passes** — not
   in Kafka.
6. **Where do throughput gains flatten?** They do not, within C≤32. What flattens
   is the *ordering violation rate*, by C≈4–8.
7. **What trade-off exists?** Per-key ordering is binary in concurrency: exact at
   C=1, broken at every C≥2. Past C=2 more concurrency buys throughput and costs
   displacement *distance* (5(C−1)) rather than additional violation *frequency*.
   Separately, async sends bought ~5.6× throughput at C=1 with ordering fully
   intact, in this workload.
8. **What remains unproven?** §14 — especially the direct trace of two passes
   racing over one key (7), any CPU bottleneck (4), the generality beyond this
   workload shape (5), and the magnitude of the async ordering advantage (3).

**No production default was changed.** `publisher-concurrency` remains 1 and
`async-sends` remains `false`. Day 22 measured a trade-off; it does not resolve
it.

One documentation defect stands from Day 21 and is reconfirmed here:
`KafkaTopicsConfig` states that keying means "a consumer sees that job's history
in order". The keying half is verified (every key on one partition, all 28 runs);
the ordering half is false for any `publisher-concurrency > 1`. Not changed —
recorded.

## 16. Follow-up Hypotheses

Untested. Listed as **HYPOTHESIS** only; none implemented.

- **H1 — publisher concurrency = 1.** Already the default and measured here to
  give exact ordering. Costs ~109 /s sequential, but §13 measured async at C=1
  reaching 613–798 /s with ordering intact, which may make H1 far less expensive
  than Day 15–20 assumed. The cheapest next experiment is simply to characterise
  async C=1 properly.
- **H2 — per-key serialisation.** Serialise passes per aggregate key. Would
  preserve ordering at C>1 but needs a coordination mechanism the outbox does not
  have, and 20 keys over C=32 passes would leave most passes idle.
- **H3 — key-aware outbox claiming.** Have each pass claim whole keys rather than
  an id range, so no two passes hold one key. §12 shows the current claim takes
  all 20 keys per batch, which is precisely what H3 would prevent.
- **H4 — partition-aware publisher ownership.** Assign each publisher thread a
  Kafka partition and claim only events hashing to it. Ordering follows from
  single ownership per partition; concurrency is then capped by partition count.
- **H5 — Kafka partition sequencing with consumer-side validation.** Accept
  producer-side disorder and carry a per-key sequence number for consumers to
  reorder or detect gaps. Moves the cost to every consumer.

Prerequisite for evaluating any of them: a direct trace of the race (§14 item 7)
and a second workload shape (§14 item 5), so a fix can be shown to address the
measured mechanism rather than this workload's geometry.
