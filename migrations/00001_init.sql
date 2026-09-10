-- +goose Up

-- The lifecycle of a job. Using an enum (a fixed set of allowed values)
-- instead of free-text means the database itself rejects an invalid state.
CREATE TYPE job_state AS ENUM (
    'pending',    -- waiting to be claimed
    'running',    -- claimed by a worker, lease held
    'succeeded',  -- handler returned no error
    'dead'        -- gave up: attempts exhausted, or a permanent failure
);

CREATE TABLE jobs (
    id                BIGSERIAL     PRIMARY KEY,

    -- Logical stream. Workers subscribe to one or more queues.
    queue             TEXT          NOT NULL DEFAULT 'default',

    -- The work itself, as JSON. JSONB is Postgres's binary JSON type:
    -- it can be indexed and queried, unlike a plain text column.
    payload           JSONB         NOT NULL,

    state             job_state     NOT NULL DEFAULT 'pending',
    priority          SMALLINT      NOT NULL DEFAULT 0,

    -- Retry accounting. attempts increments on every claim, not on success,
    -- so a job that keeps crashing workers still eventually gives up.
    attempts          INT           NOT NULL DEFAULT 0,
    max_attempts      INT           NOT NULL DEFAULT 5,

    -- Not claimable until now() >= run_at. Powers both delayed jobs
    -- ("send this in 30s") and retry backoff.
    run_at            TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- THE LEASE. A worker claiming a job stamps its identity and a deadline
    -- here. If the worker dies, nothing cleans up -- but the deadline passes,
    -- and the reaper returns the job to 'pending'. This is what makes crash
    -- recovery work without any heartbeat protocol between processes.
    lease_expires_at  TIMESTAMPTZ,
    worker_id         TEXT,

    -- Caller-supplied dedupe key. Enqueuing twice with the same key
    -- returns the original job instead of creating a second one.
    idempotency_key   TEXT,

    last_error        TEXT,

    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ
);

-- Enqueue-side deduplication.
-- PARTIAL index (the WHERE clause): only rows that actually have a key are
-- indexed, so the millions of jobs with a NULL key don't collide with each
-- other and don't bloat the index.
CREATE UNIQUE INDEX idx_jobs_idempotency
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The index the claim query rides on. Column order matches the query's
-- filter-then-sort shape: queue (equality), then priority DESC, run_at ASC.
-- Partial again: only 'pending' rows are ever claimable, so completed jobs
-- never enter this index no matter how large the table grows.
CREATE INDEX idx_jobs_claim
    ON jobs (queue, priority DESC, run_at)
    WHERE state = 'pending';

-- The reaper's index: find running jobs whose lease has expired.
CREATE INDEX idx_jobs_lease
    ON jobs (lease_expires_at)
    WHERE state = 'running';

-- +goose Down
DROP TABLE IF EXISTS jobs;
DROP TYPE IF EXISTS job_state;
