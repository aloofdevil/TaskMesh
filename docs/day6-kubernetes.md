# Running TaskMesh on Kubernetes

Local-cluster deployment of the control plane, workers, and their backing
services. For the fast development loop use Docker Compose instead
(`docker compose up -d`); Kubernetes is here for the multi-worker scaling
and failure demonstrations.

## Prerequisites

- A local Kubernetes cluster. Docker Desktop's built-in Kubernetes is the
  simplest (*Settings → Kubernetes → Enable Kubernetes*); kind or minikube
  work too.
- `kubectl` pointing at it: `kubectl config current-context`
- The two application images built locally:

```bash
docker compose build          # produces taskmesh-control-plane:latest
                              # and taskmesh-worker:latest
```

The manifests set `imagePullPolicy: IfNotPresent` and reference those
`:latest` tags, so the kubelet uses the local images and never reaches for
a registry. Docker Desktop's cluster shares the Docker daemon, so the
images are visible immediately. On **kind** you must load them first:

```bash
kind load docker-image taskmesh-control-plane:latest taskmesh-worker:latest
```

## Deploy

```bash
kubectl apply -f k8s/
kubectl -n taskmesh rollout status deployment/control-plane
kubectl -n taskmesh rollout status deployment/taskmesh-worker
```

Files apply in dependency order by name: namespace, config/secret,
PostgreSQL, Redis, Kafka, control plane, workers.

## Check what is running

```bash
kubectl -n taskmesh get pods
kubectl -n taskmesh get services
kubectl -n taskmesh get deployments
kubectl -n taskmesh logs -l app=taskmesh-worker --tail=20
```

## Talk to the API

There is no Ingress (out of scope), so port-forward:

```bash
kubectl -n taskmesh port-forward service/control-plane 8080:8080
```

Then, in another shell:

```bash
curl -s localhost:8080/actuator/health/readiness
curl -s localhost:8080/actuator/metrics/taskmesh.workers.active

curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"demo-1","type":"TEST_JOB","payload":{"n":1},"priority":5}'
```

## Scale workers

```bash
kubectl -n taskmesh scale deployment/taskmesh-worker --replicas=3
kubectl -n taskmesh get pods -l app=taskmesh-worker
```

Each pod takes its worker id from its own pod name (downward API,
`metadata.name`), so replicas register as distinct workers and compete for
jobs through the control plane's atomic claim. No coordination between
pods is needed, and no job is ever handed to two of them.

Scale back down with `--replicas=2`.

## Health endpoints

```bash
kubectl -n taskmesh exec deploy/control-plane -- \
  curl -s localhost:8080/actuator/health/liveness
kubectl -n taskmesh exec deploy/control-plane -- \
  curl -s localhost:8080/actuator/health/readiness
```

The two probes deliberately mean different things:

- **Liveness** includes only the process state. A Redis, Kafka or even
  PostgreSQL outage must not restart the control plane, because restarting
  fixes none of them and a restart loop makes recovery slower.
- **Readiness** additionally includes `db`. PostgreSQL is the authoritative
  store, so an instance that cannot reach it should not receive traffic.
  Redis (a liveness cache) and Kafka (fed asynchronously through the
  outbox) are excluded: the control plane keeps accepting and running jobs
  through an outage of either.

## Worker termination

On SIGTERM a worker stops claiming immediately, gives in-flight jobs up to
`WORKER_SHUTDOWN_DRAIN_MS` (20s) to finish and report their own results,
then deregisters and exits — comfortably inside the pod's
`terminationGracePeriodSeconds: 45`.

A job that cannot finish in that window is simply left. Nothing is
reported for it, so execution fencing is untouched: its lease lapses, the
reaper recovers it, and another worker runs it under a new execution id.
Draining is an optimisation to avoid redoing nearly-finished work, not a
correctness mechanism — the lease is what guarantees the job is not lost.

## Dependency outages

PostgreSQL is the only dependency the control plane cannot serve without,
and it is the only one in the readiness group. Redis and Kafka can both be
down without jobs stopping.

**Redis** is a liveness cache. `WorkerLivenessCache` treats every Redis
failure as non-fatal and logs "PostgreSQL remains authoritative", so
registration, heartbeats, claiming, leases and completion all keep working.
Two settings make that degradation actually graceful rather than merely
survivable:

- `spring.data.redis.timeout` / `connect-timeout` are set to 1s. Lettuce
  defaults to a **60 second** command timeout, so without this every call
  against a dead Redis blocks for a minute before the error handling runs.
  Measured against a stopped Redis, a heartbeat returns in ~1.0s.
- The scheduler pools are sized to the number of `@Scheduled` methods in
  each process (control plane 2, worker 3). Spring's default pool of 1
  makes them share a thread, so one blocked task stops all the others - a
  blocked heartbeat was enough to stop the worker claiming work entirely.

To reproduce:

```bash
kubectl -n taskmesh scale deploy/redis --replicas=0
# submit a job - it still gets claimed, run and completed
kubectl -n taskmesh scale deploy/redis --replicas=1
```

`-Dnetworkaddress.cache.ttl=10` in both images matters for the recovery
half of that cycle. The JVM caches successful DNS lookups, and a restarted
container can come back on a different IP; without a bounded TTL the client
reconnects to the stale address indefinitely and never recovers even though
the dependency is healthy and reachable by name.

**Kafka** is fed asynchronously through the outbox, so an outage delays
event publication but never blocks a job. Events accumulate unpublished in
`job_events` and drain when the broker returns.

## Tear down

```bash
kubectl delete -f k8s/
# PersistentVolumeClaims are namespaced, so deleting the namespace removes
# the PostgreSQL and Kafka volumes too:
kubectl delete namespace taskmesh
```
