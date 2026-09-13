# What Docket deliberately does not do

Scope discipline is a feature. These are the things that were considered and
left out on purpose, so that what *is* here could be finished and tested
properly.

## Out of scope, by design

**Exactly-once delivery.** Not possible over an unreliable network: a worker
that finishes the work and dies before recording success is indistinguishable
from one that never ran. Docket delivers at least once and asks handlers to be
idempotent. See the README for the reasoning.

**A Kafka competitor.** No partitions, consumer groups, log compaction, or
replication. Docket is a work queue, not an event log. If you need to replay
history, you need a log.

**Distributed consensus.** No Raft, no leader election. PostgreSQL is the
single coordination point, which removes an entire class of distributed-systems
bugs (clock skew, split brain) in exchange for a throughput ceiling. The
benchmarks in the README show where that ceiling is.

**Multi-tenancy and authentication.** Docket is a single-trust-domain
component. Put it behind whatever already authenticates your services.

**A web dashboard.** The CLI covers inspection (`status`, `dlq list`). A UI
would add a week and demonstrate nothing about the queue itself.

**Non-Postgres backends.** A Redis or SQLite backend behind the same interface
is feasible, but every guarantee in the README leans on Postgres specifics --
`SKIP LOCKED`, transactional enqueue, server-side `now()`. Supporting a second
backend would mean weakening the promises or maintaining two of everything.

## Possible future work

Things that would fit the design and might be worth doing, roughly in order of
value:

- **Batched heartbeats.** Today each running job renews its own lease on its
  own timer. With high concurrency that is many small round trips; one
  `UPDATE ... WHERE id = ANY($1)` per worker per tick would do.
- **Prometheus metrics.** Queue depth, oldest pending age, throughput, retry
  rate, dead-letter count. The `store` package already has the queries.
- **gRPC API** so producers in other languages can enqueue.
- **Cron-style recurring jobs.** A `schedules` table and a small loop that
  enqueues on time. Straightforward, but a separate feature.
- **Per-queue rate limits.** A token bucket in front of `Claim`.
- **Job dependencies** (run B after A). This is the start of a workflow
  engine, which is a different project.
- **Partitioning `jobs` by state** for very large backlogs, so completed rows
  can be dropped or archived cheaply.
