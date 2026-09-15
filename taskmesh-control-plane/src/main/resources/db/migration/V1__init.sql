-- TaskMesh initial schema.
-- PostgreSQL is the sole source of truth for job state (see docs/architecture.md).
-- Redis and Kafka hold no state that this schema does not also hold durably.

-- ============================================================
-- workers: durable worker registry. Redis (Day 3) caches a
-- TTL'd liveness projection of this table for fast detection;
-- this table is what recovery decisions are ultimately based on.
-- ============================================================
CREATE TABLE workers (
    id                  TEXT PRIMARY KEY,
    hostname            TEXT        NOT NULL,
    capacity            INT         NOT NULL,
    status              TEXT        NOT NULL,
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workers_status_ck CHECK (status IN ('ACTIVE', 'DEAD', 'DEREGISTERED'))
);

-- Used by the reaper to find workers that have stopped heartbeating.
CREATE INDEX idx_workers_heartbeat ON workers (last_heartbeat_at) WHERE status = 'ACTIVE';

-- ============================================================
-- jobs: authoritative job state and the scheduling queue.
-- current_execution_id is the fencing token: every worker write
-- (renew/complete/fail) must present it, and a mismatch is
-- rejected as a stale execution (see docs/architecture.md).
-- ============================================================
CREATE TABLE jobs (
    id                      UUID PRIMARY KEY,
    idempotency_key         TEXT UNIQUE,
    payload_hash            TEXT,
    type                    TEXT        NOT NULL,
    payload                 JSONB       NOT NULL,
    priority                SMALLINT    NOT NULL DEFAULT 5,
    status                  TEXT        NOT NULL,
    attempt_count           INT         NOT NULL DEFAULT 0,
    max_attempts            INT         NOT NULL DEFAULT 3,
    scheduled_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigned_worker_id      TEXT REFERENCES workers (id),
    current_execution_id    UUID,
    lease_until             TIMESTAMPTZ,
    last_failure_reason     TEXT,
    result                  JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                 BIGINT      NOT NULL DEFAULT 0,
    CONSTRAINT jobs_status_ck CHECK (status IN
        ('QUEUED', 'RUNNING', 'RETRYING', 'COMPLETED', 'DEAD_LETTER', 'CANCELLED'))
);

-- Partial indexes: each indexes only the rows relevant to one
-- scheduler query, so they stay small regardless of how many
-- COMPLETED/DEAD_LETTER/CANCELLED jobs accumulate over time.

-- The claim query: highest-priority, earliest-scheduled queued jobs.
CREATE INDEX idx_jobs_claim ON jobs (priority DESC, scheduled_at ASC, id ASC)
    WHERE status = 'QUEUED';

-- The reaper's lease-expiry sweep.
CREATE INDEX idx_jobs_lease ON jobs (lease_until)
    WHERE status = 'RUNNING';

-- The reaper's backoff-elapsed sweep (RETRYING -> QUEUED promotion).
CREATE INDEX idx_jobs_retry ON jobs (scheduled_at)
    WHERE status = 'RETRYING';

-- ============================================================
-- job_attempts: one row per execution attempt. The
-- UNIQUE(job_id, attempt_number) constraint is one of the three
-- structural barriers (with SELECT ... FOR UPDATE SKIP LOCKED
-- and the status guard) that make double-assignment impossible.
-- ============================================================
CREATE TABLE job_attempts (
    id              UUID PRIMARY KEY,
    job_id          UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt_number  INT         NOT NULL,
    execution_id    UUID        NOT NULL UNIQUE,
    worker_id       TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    failure_reason  TEXT,
    CONSTRAINT job_attempts_status_ck CHECK (status IN
        ('RUNNING', 'SUCCEEDED', 'FAILED', 'LEASE_EXPIRED', 'ABORTED')),
    CONSTRAINT job_attempts_job_attempt_uq UNIQUE (job_id, attempt_number)
);

CREATE INDEX idx_job_attempts_job ON job_attempts (job_id, attempt_number);

-- ============================================================
-- job_events: append-only audit log AND the transactional
-- outbox for Kafka. A row is written in the same transaction as
-- the state change it describes; a separate relay (Day 5)
-- publishes unpublished rows to Kafka, so a Kafka outage never
-- blocks a state transition (see docs/architecture.md, Redis/
-- Kafka failure scenarios).
-- ============================================================
CREATE TABLE job_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

-- The outbox relay's poll query: unpublished rows, oldest first.
CREATE INDEX idx_job_events_unpublished ON job_events (id) WHERE published_at IS NULL;

-- Per-aggregate history lookups (e.g. all events for one job).
CREATE INDEX idx_job_events_aggregate ON job_events (aggregate_id, id);
