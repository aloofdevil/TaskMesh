# TaskMesh demo runbook

A reproducible sequence that exercises every interesting behavior in the system. Every command here was run against the Docker Compose stack; the outputs quoted are real.

Nothing below uses an endpoint that does not exist. Where a scenario needs a job to *fail* — retry and dead-letter — the demo acts as a worker and calls the real worker protocol (`/internal/...`), because the bundled worker's `doWork()` is a sleep that always succeeds.

## 0. Start the stack

```bash
docker compose up -d --build
docker compose ps
```

All five services should report `healthy`:

```
control-plane  running  healthy
kafka          running  healthy
postgres       running  healthy
redis          running  healthy
worker         running  healthy
```

A shell helper used throughout:

```bash
submit() { curl -s -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d "{\"idempotencyKey\":\"$1\",\"type\":\"TEST_JOB\",\"payload\":{\"n\":1},\"priority\":5,\"maxAttempts\":3}"; }
```

Handy for watching state directly in the source of truth:

```bash
tmsql() { docker compose exec -T postgres psql -U taskmesh -d taskmesh "$@"; }
```

## 1. Submit a job and watch it complete

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"demo-1","type":"TEST_JOB","payload":{"n":1},"priority":5,"maxAttempts":3}'
```

`201 Created`, `"status":"QUEUED"`. A worker claims it within a second; after a few seconds:

```bash
curl localhost:8080/jobs/<id>
```

```
"status":"COMPLETED"   "attemptCount":1
```

Confirm the claim in the worker log:

```bash
docker compose logs worker | grep "Claimed job"
```

## 2. Cancel a queued job

Cancellation only applies before a worker claims the job, so schedule it into the future:

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"demo-cancel","type":"TEST_JOB","payload":{},"scheduledAt":"2030-01-01T00:00:00Z"}'

curl -X POST localhost:8080/jobs/<id>/cancel
```

`200 OK`, `"status":"CANCELLED"`.

## 3. Idempotency

Same key **and** same payload returns the original job rather than creating a second one:

```bash
submit demo-idem   # 201, id X
submit demo-idem   # 200, id X  (same id)
```

Same key with a **different** payload is a conflict:

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"demo-idem","type":"TEST_JOB","payload":{"n":999}}'
```

```
HTTP 409
"message":"Idempotency key 'demo-idem' was already used with a different payload"
```

## 4. Priority ordering and future scheduling

Stop the worker so claim order is observable rather than racing:

```bash
docker compose stop worker
```

Submit in ascending priority, plus a high-priority job scheduled an hour out:

```bash
for p in 1 9 5; do
  curl -s -o /dev/null -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
    -d "{\"idempotencyKey\":\"prio-$p\",\"type\":\"PRIO_$p\",\"payload\":{},\"priority\":$p}"
done
curl -s -o /dev/null -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"future-1","type":"FUTURE_JOB","payload":{},"priority":10,"scheduledAt":"2030-01-01T00:00:00Z"}'
```

Register a worker and claim repeatedly:

```bash
curl -s -X POST localhost:8080/internal/workers/register -H 'Content-Type: application/json' \
  -d '{"workerId":"demo-worker-1","hostname":"demo","capacity":4}'

for i in 1 2 3 4; do curl -s -X POST localhost:8080/internal/workers/demo-worker-1/claim; echo; done
```

Claims come back `PRIO_9`, `PRIO_5`, `PRIO_1` — priority order, not submission order. The fourth returns **`204 No Content`**: `FUTURE_JOB` has the *highest* priority but is not yet eligible, which is what proves `scheduled_at` gates claiming.

## 5. Retry with backoff, then dead-letter

Submit with a small attempt budget so the budget is exhausted quickly:

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"flaky-1","type":"FLAKY_JOB","payload":{},"priority":10,"maxAttempts":2}'

CLAIM=$(curl -s -X POST localhost:8080/internal/workers/demo-worker-1/claim)
# take jobId and executionId from $CLAIM
```

Report the first failure:

```bash
curl -X POST localhost:8080/internal/jobs/<jobId>/fail -H 'Content-Type: application/json' \
  -d '{"workerId":"demo-worker-1","executionId":"<executionId>","failureReason":"simulated transient failure"}'
```

```
"status":"RETRYING"   "attemptCount":1   "lastFailureReason":"simulated transient failure"
```

After the 1s backoff elapses it returns to `QUEUED`. Claim again — note the **execution id is different**, because the fencing token is per attempt — and fail once more:

```
"status":"DEAD_LETTER"   "attemptCount":2   "maxAttempts":2
```

## 6. Stale execution fencing

Replay the **first** execution id against the job:

```bash
curl -X POST localhost:8080/internal/jobs/<jobId>/complete -H 'Content-Type: application/json' \
  -d '{"workerId":"demo-worker-1","executionId":"<FIRST executionId>"}'
```

```
HTTP 409
"error":"STALE_EXECUTION"
"message":"Execution ... no longer owns job ...; it was reassigned, already finished, or never owned it"
```

This is the guarantee that makes crash recovery safe: a zombie worker cannot complete a job that has moved on without it.

```bash
curl -s -X POST localhost:8080/internal/workers/demo-worker-1/deregister
```

## 7. Multiple workers competing

```bash
docker compose up -d --scale worker=3
tmsql -c "select id, status from workers where status='ACTIVE'"
```

Submit a batch and watch it spread:

```bash
for i in $(seq 1 12); do
  curl -s -o /dev/null -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
    -d "{\"idempotencyKey\":\"multi-$i\",\"type\":\"TEST_JOB\",\"payload\":{\"i\":$i}}"
done

tmsql -c "select assigned_worker_id, count(*) from jobs where idempotency_key like 'multi-%' group by 1"
```

Observed: a clean `4 / 4 / 4` split, all `COMPLETED`.

The claim safety check — no job was ever handed to two workers:

```bash
tmsql -c "select (select count(*) from job_attempts) attempts,
                (select count(distinct execution_id) from job_attempts) distinct_exec,
                (select count(*) from (select job_id,attempt_number from job_attempts
                   group by job_id,attempt_number having count(*)>1) x) duplicates"
```

```
 attempts | distinct_exec | duplicates
       22 |            22 |          0
```

## 8. Worker crash → lease expiry → reassignment

Give workers a long job so one is genuinely in flight:

```bash
WORKER_JOB_DURATION_MS=60000 docker compose up -d --scale worker=3 --force-recreate worker
```

Submit, find the owner, then **SIGKILL** its container — no graceful shutdown:

```bash
submit crash-1
tmsql -c "select status, assigned_worker_id, current_execution_id from jobs where id='<id>'"

# the worker id is <container-hostname>-<suffix>
docker kill -s KILL <container>
```

Poll the job. The lease is honored until it lapses (~30s), then the reaper reassigns:

```
t+10s  RUNNING  worker=e482fe1ceae8-8723a9f8  exec=53a9075d-...
t+20s  RUNNING  worker=e482fe1ceae8-8723a9f8  exec=53a9075d-...
t+30s  RUNNING  worker=03c39c12f2a8-0141da45  exec=1336ac72-...   <-- new worker, new execution id
```

The dead worker's execution is now fenced (`409 STALE_EXECUTION`), and the job completes on its second attempt:

```
 status    | attempt_count
 COMPLETED |             2
```

The full event trail, all published:

```bash
tmsql -c "select event_type, (published_at is not null) published from job_events where aggregate_id='<id>' order by id"
```

```
 JOB_QUEUED | t
 JOB_RUNNING | t
 JOB_RETRYING | t      <-- lease expiry counted as a failed attempt
 JOB_QUEUED | t
 JOB_RUNNING | t
 JOB_COMPLETED | t
```

Restore the normal duration afterwards:

```bash
docker compose up -d --scale worker=3 --force-recreate worker
```

## 9. Redis outage

Redis is a liveness cache only, so losing it must degrade nothing that matters.

```bash
# Baseline: ~10ms
curl -o /dev/null -w '%{time_total}\n' -X POST localhost:8080/internal/workers/<id>/heartbeat

docker compose stop redis

# During the outage: ~1.0s, still HTTP 204 - fails fast instead of blocking for Lettuce's 60s default
curl -o /dev/null -w '%{time_total} %{http_code}\n' -X POST localhost:8080/internal/workers/<id>/heartbeat
```

Jobs keep flowing and readiness stays up:

```bash
submit redis-down-1     # reaches COMPLETED
curl -o /dev/null -w '%{http_code}\n' localhost:8080/actuator/health/readiness   # 200
docker compose logs control-plane | grep "PostgreSQL remains authoritative"
```

Restore and confirm recovery:

```bash
docker compose start redis
# heartbeat back to ~13ms, liveness keys repopulated
docker compose exec -T redis redis-cli KEYS 'taskmesh:worker:*'
```

## 10. Kafka outage and outbox accumulation

```bash
tmsql -tAc "select 'total='||count(*)||' pending='||(count(*)-count(published_at)) from job_events"
# total=96 pending=0

docker compose stop kafka
for i in 1 2 3; do submit "kafka-down-$i" >/dev/null; done
```

Jobs complete normally — Kafka is not in the job path:

```
 status    | count
 COMPLETED |     3
```

Events accumulate durably in PostgreSQL instead of being lost:

```
total=105 published=96 pending=9
```

Readiness stays `200`. Restart the broker and the backlog drains by itself:

```bash
docker compose start kafka
tmsql -tAc "select count(*)-count(published_at) from job_events"   # -> 0
```

The metrics show it retried rather than dropping anything:

```bash
curl -s localhost:8080/actuator/metrics/taskmesh.outbox.publish_failures   # 5.0
curl -s localhost:8080/actuator/metrics/taskmesh.outbox.published          # 105.0
```

Five failed publish attempts, 105 events eventually published — at-least-once delivery, with PostgreSQL holding the backlog.

## 11. Tear down

```bash
docker compose down -v
```

## Kubernetes

The same lifecycle, scaling and crash-recovery demonstrations run on Kubernetes — see [`day6-kubernetes.md`](day6-kubernetes.md).

```bash
kubectl apply -f k8s/
kubectl -n taskmesh wait --for=condition=available --timeout=300s deploy --all
kubectl -n taskmesh port-forward svc/control-plane 8080:8080
```

Worker ids there are pod names (injected via the downward API), so `psql -c "select id from workers"` returns rows like `taskmesh-worker-56dc6c9977-4n822` — one per replica.
