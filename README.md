# Docket

[![CI](https://github.com/varshakodi/docket/actions/workflows/ci.yml/badge.svg)](https://github.com/varshakodi/docket/actions/workflows/ci.yml)

A durable, horizontally scalable job queue in Go, backed by PostgreSQL.

Enqueue a job; it survives crashes, restarts and deploys. Run as many workers as
you like; no two ever pick up the same job. A worker that dies mid-job has its
work handed to another one automatically. Failures retry with exponential
backoff and land in a dead-letter queue when they give up.

There is no queue server. **Postgres is the queue** — storage and coordination in
one place — which is what makes the whole thing about a thousand lines.

```
  producers                                    workers (any number)
  ─────────                                    ────────────────────
  docket enqueue ──┐                       ┌── claim ── run handler ── complete
  your app  ───────┤                       │       heartbeat every lease/3
                   ▼                       ▼
            ┌──────────────────────────────────────┐
            │  PostgreSQL          jobs table       │
            │  pending → running → succeeded/dead  │
            └──────────────────────────────────────┘
                                   ▲
                    reaper (in every worker) ── returns expired leases
                    to pending; recovers jobs from crashed workers
```

## Status

Working and tested: enqueue, claim, complete, retries with jittered backoff,
leases and heartbeats, crash recovery, dead-letter queue, concurrency, graceful
shutdown, idempotent enqueue.

Also: a benchmark harness with a naive-locking comparison, and fault-injection
tests that `SIGKILL` real worker processes. See [Benchmarks](#benchmarks).

Not yet: gRPC API, Prometheus metrics. See [Roadmap](#roadmap).

## Quick start

**With Docker** (nothing else needed):

```bash
docker compose up -d      # Postgres + migrations + a 4-lane worker
docker compose run --rm worker enqueue --queue default --payload '{"sleep_ms":1000}'
docker compose run --rm worker status 1
docker compose logs -f worker
```

**Without Docker** — Go 1.27+ and a local PostgreSQL 14+:

```bash
createdb docket
go build -o bin/docket ./cmd/docket
./bin/docket migrate up

# Terminal 1: run a worker that handles 4 jobs at a time
./bin/docket work --queue email --concurrency 4

# Terminal 2: give it something to do
./bin/docket enqueue --queue email --payload '{"to":"a@b.c","sleep_ms":2000}'
./bin/docket status 1
```

Set `DATABASE_URL` to point somewhere other than `localhost:5432/docket`.

To see crash recovery with your own eyes: enqueue a job with
`{"sleep_ms": 10000}`, start a worker with `--lease 3s --reap-interval 1s`,
`kill -9` it mid-job, start another worker, and watch `status` go from
`running` (owned by the dead one) to `succeeded` with `attempts: 2`.

The bundled worker runs a demo handler that reacts to two payload fields:
`{"sleep_ms": N}` simulates slow work and `{"fail": true}` simulates a failure.
Real applications register their own handler through the `worker` package.

## How it works

### One table

Every job is a row in `jobs` ([schema](migrations/00001_init.sql)). The columns
that matter:

| column | role |
|---|---|
| `state` | `pending` → `running` → `succeeded`, or `dead` |
| `run_at` | not claimable before this. Powers delayed jobs and retry backoff |
| `lease_expires_at`, `worker_id` | **the lease** — who owns the job and until when |
| `attempts`, `max_attempts` | retry budget; `attempts` increments on every claim |
| `idempotency_key` | optional; enqueuing the same key twice creates one job |

Three partial indexes keep the hot queries fast no matter how many finished
jobs accumulate: only `pending` rows are in the claim index, only `running`
rows are in the lease index.

### Claiming without collisions

Many workers poll the same table at once. Each must walk away with different
jobs, and none may wait for the others. That is one clause of SQL:

```sql
SELECT id FROM jobs
WHERE queue = $1 AND state = 'pending' AND run_at <= now()
ORDER BY priority DESC, run_at ASC
LIMIT $2
FOR UPDATE SKIP LOCKED
```

`FOR UPDATE` locks the selected rows. `SKIP LOCKED` says: if a row is already
locked by another worker's in-progress claim, don't wait for it — skip it and
take the next one. Without that clause, sixteen workers would serialise on row
locks and run one at a time. With it, each gets a disjoint set and throughput
scales with worker count. The claim and the state change are one statement, so
there is no window in which a second worker can see the row as still pending.

The concurrency test enqueues 500 jobs, points 16 goroutines at them, and
asserts every job was claimed exactly once.

### Leases, not locks

A worker that is `SIGKILL`ed never gets to clean up. Locks would die with its
connection; a lease is just a timestamp in the row, and timestamps don't care
whether the worker is alive. Three pieces:

- **Claim** stamps `worker_id` and `lease_expires_at = now() + lease`.
- **Heartbeat** — a running worker renews the lease every `lease / 3`. A job
  that takes an hour stays owned for an hour.
- **Reaper** — a sweep inside every worker process finds `running` rows whose
  lease has passed and returns them to `pending` (or `dead`, if out of
  attempts). No worker is special; if one dies, any other's reaper recovers its
  jobs.

Crash detection is therefore a timestamp comparison. Nothing sends a "worker
died" message, because a dead worker can't.

Every write that finishes a job — `Complete`, `Fail`, `ExtendLease`, `MarkDead`
— is guarded by `WHERE worker_id = $me`. A worker that stalled, lost its lease
and woke up later cannot overwrite a job that another worker has since taken.

### Retries

A failed job returns to `pending` with `run_at` pushed into the future by
`random(0, min(base × 2^attempts, max))`. The randomness ("full jitter") is
not decoration: when an upstream dependency fails, every job calling it fails
at the same instant, and without jitter they would all retry at the same
instant too — a synchronised wave that keeps the dependency down. Spreading
them out breaks the lockstep.

After `max_attempts`, a job is `dead`. It stays queryable in the dead-letter
queue (`docket dlq list`) and can be given a fresh attempt budget
(`docket dlq requeue ID`). A handler can also wrap `worker.ErrPermanent` to
skip retries entirely for failures that retrying cannot fix.

### Graceful shutdown

On `SIGTERM` or `SIGINT` a worker:

1. stops claiming new jobs;
2. waits up to `--grace` for in-flight jobs to finish (handlers are **not**
   interrupted during this window);
3. if any are still running, releases them back to `pending` so another worker
   picks them up in milliseconds rather than after the lease expires — and only
   then cancels the handlers.

This is what makes rolling deploys clean: `terminationGracePeriodSeconds` in
Kubernetes maps directly onto `--grace`.

### What is promised, and what isn't

Docket delivers **at least once**. A job is never silently lost: every failure
path — worker crash, database disconnect, `SIGKILL`, network partition — ends
in either completion or redelivery.

It does *not* deliver exactly once, because nothing can: a worker that
finishes the work and dies before recording success is indistinguishable from
one that never ran, and the queue must redeliver. So **handlers must be
idempotent** — safe to run twice for the same job. Keying external side
effects on `job.ID` is usually enough.

Enqueue-side deduplication is separate: pass an idempotency key and repeated
enqueues return the original job instead of creating a second one.

All timestamps are the database's `now()`, never a worker's clock, so clock
skew between machines cannot corrupt lease arithmetic.

## CLI

```
docket enqueue --queue NAME --payload JSON [--key K] [--delay 30s] [--priority N] [--max-attempts N]
docket work    --queue NAME [--concurrency N] [--lease 30s] [--grace 25s]
               [--reap-interval 5s] [--backoff-base 1s] [--backoff-max 5m]
docket status  ID
docket dlq     list [--queue NAME] [--limit N]
docket dlq     requeue ID
docket bench   [--jobs N] [--workers 1,2,4,8,16] [--batch N] [--mode skip-locked|naive|both]
docket migrate up | down | status
```

## Using it from Go

```go
w := worker.New(st, worker.Config{
    Queue:       "email",
    Concurrency: 8,
    Lease:       30 * time.Second,
}, func(ctx context.Context, job store.Job) error {
    var p EmailPayload
    if err := json.Unmarshal(job.Payload, &p); err != nil {
        return fmt.Errorf("bad payload: %w", worker.ErrPermanent) // no point retrying
    }
    return mailer.Send(ctx, p) // ctx is cancelled if the lease is lost
}, logger)

go reaper.Run(ctx, st, 5*time.Second, backoff.Config{}, logger)
w.Run(ctx) // returns after graceful shutdown when ctx is cancelled
```

## Tests

Everything runs against a real PostgreSQL — the logic under test *is* SQL, so a
mock would test nothing. `go test -race -p 1 ./...` locally; CI does the same
against a Postgres 17 service container.

The tests that carry the correctness claims:

| test | proves |
|---|---|
| `TestClaimConcurrentNoDuplicates` | 16 workers, 500 jobs, each claimed exactly once |
| `TestConcurrentReapersDoNotDoubleProcess` | two reapers at once never double-requeue |
| `TestHeartbeatKeepsLongJobAlive` | a job 4× longer than its lease is never stolen by a reaper sweeping every 50 ms |
| `TestReaperRecoversJobFromDeadWorker` | a worker that claims and vanishes has its job recovered and finished elsewhere |
| `TestGracefulShutdownFinishesInFlightJob` | shutdown waits for a running job rather than interrupting it |
| `TestShutdownReleasesJobWhenGraceExpires` | past the grace period, the job is handed back unowned and claimable |
| `TestCompleteRejectsStaleWorker` | a worker that lost its lease cannot overwrite the new owner's work |
| `TestWorkerSurvivesPanickingHandler` | a panicking job is retried; the worker keeps running |

Two more build the real binary and run it as separate processes, because a
`SIGKILL` gives a process no chance to run any cleanup code — which is the
case the lease design exists for:

| test | proves |
|---|---|
| `TestSIGKILLMidJobIsRecoveredByAnotherProcess` | a worker `SIGKILL`ed mid-job leaves an orphaned `running` row; a second process's reaper recovers it and the job completes, `attempts = 2` |
| `TestCrashAfterWorkBeforeAckIsRedelivered` | a worker that finishes the work and dies before recording success has the job redelivered — the at-least-once case, demonstrated rather than described |

## Benchmarks

`docket bench` seeds N jobs, runs W workers with a no-op handler, and reports
throughput and claim latency. Workers are goroutines, each claiming on its own
database connection — the contention is real, the run is deterministic, and it
works in CI. It runs the whole matrix twice: once with the real claim query and
once with `FOR UPDATE` alone, no `SKIP LOCKED`, so the difference is measured
rather than asserted.

Numbers below: Apple Silicon MacBook Pro, local PostgreSQL 17, single run
(expect ±10–15% between runs). Reproduce with `docket bench`.

**Batch 10** — 5,000 jobs, 10 claimed per round trip:

```
MODE          WORKERS   JOBS/SEC  CLAIM p50        p95        p99   SCALING
skip-locked         1       5269     0.25ms     0.51ms     0.73ms      100%
skip-locked         2       8211     0.34ms     0.57ms     0.70ms       78%
skip-locked         4      13104     0.44ms     0.78ms     2.40ms       62%
skip-locked         8      17607     0.61ms     1.83ms     3.32ms       42%
skip-locked        16      19078     1.29ms     3.78ms    10.30ms       23%
naive               1       6220     0.22ms     0.40ms     0.52ms      100%
naive               2       9576     0.31ms     0.49ms     0.57ms       77%
naive               4      14954     0.41ms     0.92ms     1.12ms       60%
naive               8      15847     0.70ms     3.22ms     7.41ms       32%
naive              16      16387     0.93ms    10.46ms    15.58ms       16%
```

**Batch 1** — 3,000 jobs, one claim per job (the highest-contention shape):

```
MODE          WORKERS   JOBS/SEC  CLAIM p50        p95        p99   SCALING
skip-locked         1       3261     0.16ms     0.25ms     0.33ms      100%
skip-locked         4       7756     0.26ms     0.39ms     0.52ms       59%
skip-locked        16      10137     0.62ms     1.39ms     2.28ms       19%
naive               1       3444     0.16ms     0.24ms     0.29ms      100%
naive               4       5000     0.35ms     1.04ms     1.68ms       36%
naive              16       4722     0.50ms     3.77ms    17.35ms        9%
```

What the numbers say:

- **`SKIP LOCKED` is what lets 16 workers be faster than 4.** At batch 1 the
  naive query *gets slower* going from 4 to 16 workers (5,000 → 4,700 jobs/s)
  while `SKIP LOCKED` keeps climbing (7,800 → 10,100). At 16 workers that is
  2.1× the throughput and a p99 claim latency of 2.3 ms against 17.4 ms.
- **Contention is proportional to how often workers claim.** With batches of
  10, each claim transaction is so short (~0.3 ms) that lock waits barely
  register until 8+ workers, and the gap shows up as tail latency (p99 10 ms
  vs 16 ms) more than throughput. Batch size matters as much as the locking
  clause.
- **Neither mode scales linearly.** Efficiency falls to ~20% at 16 workers
  because one PostgreSQL instance is the shared bottleneck: every claim and
  every completion is a write to the same table. That is the ceiling of a
  database-backed queue, and the reason this design targets thousands of jobs
  per second, not hundreds of thousands.
- **With one or two workers the naive query is marginally faster.** `SKIP
  LOCKED` costs a little and buys nothing when there is nobody to skip. It
  earns its keep only under contention — which is the only time it matters.

## Layout

```
cmd/docket/          CLI, plus the real-process fault-injection tests
internal/store/      every SQL statement lives here; nothing else touches the database
internal/worker/     claim loop, concurrency, heartbeat, outcome recording, graceful shutdown
internal/reaper/     expired-lease sweep
internal/backoff/    exponential backoff with full jitter
internal/bench/      benchmark harness
migrations/          schema, embedded into the binary
Dockerfile           two-stage build; ships only a static binary on alpine
compose.yaml         Postgres + migrate + worker
```

## Roadmap and non-goals

What might come next, and what was deliberately left out and why, are in
[FUTURE.md](FUTURE.md). The short version: gRPC and Prometheus metrics would
fit; workflows, exactly-once delivery, dashboards and non-Postgres backends
would not.
