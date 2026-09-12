package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Claim atomically takes up to `limit` runnable jobs from a queue and marks
// them as owned by workerID for `lease` duration.
//
// This is the heart of the queue. Many workers call it at the same moment
// against the same table, and no two may ever receive the same job. The
// guarantee comes from a single clause -- FOR UPDATE SKIP LOCKED -- explained
// inline below.
func (s *Store) Claim(ctx context.Context, queue string, limit int, workerID string, lease time.Duration) ([]Job, error) {
	if limit <= 0 {
		limit = 1
	}

	// Step 1 (the CTE, "candidates"): find the next jobs to run, and lock
	//   those rows so nobody else can touch them for the rest of this query.
	//
	//   FOR UPDATE      -> lock the selected rows.
	//   SKIP LOCKED     -> if a row is ALREADY locked by another worker's
	//                      in-progress claim, don't wait for it -- pretend
	//                      it isn't there and move on to the next row.
	//
	//   Without SKIP LOCKED, worker 2 would block until worker 1's
	//   transaction finished, then worker 3 behind that, and so on: sixteen
	//   workers would form a queue of their own and run one at a time.
	//   With it, each worker grabs whatever is free and walks away.
	//
	// Step 2 (the UPDATE): flip those rows to 'running' and stamp the lease.
	//   Because steps 1 and 2 are one statement, they are one transaction:
	//   the lock from step 1 holds until step 2 commits. There is no gap in
	//   which another worker could see the row as still pending.
	const q = `
		WITH candidates AS (
			SELECT id
			FROM jobs
			WHERE queue = $1
			  AND state = 'pending'
			  AND run_at <= now()
			ORDER BY priority DESC, run_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET state            = 'running',
		    worker_id        = $3,
		    lease_expires_at = now() + $4::interval,
		    attempts         = attempts + 1,
		    updated_at       = now()
		FROM candidates c
		WHERE j.id = c.id
		RETURNING j.*`

	rows, err := s.pool.Query(ctx, q, queue, limit, workerID, lease)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}

	// CollectRows reads every returned row into a Job, matching columns to
	// struct fields by the db:"..." tags. It closes rows when done.
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[Job])
	if err != nil {
		return nil, fmt.Errorf("scan claimed jobs: %w", err)
	}
	return jobs, nil
}
