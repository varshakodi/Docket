package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// The methods in this file exist for the benchmark harness and are not part
// of the queue's normal operation.

// SeedJobs inserts n empty jobs onto a queue in a single statement. Far
// faster than n calls to Enqueue, which is what you want when setting up a
// run of 20,000 jobs.
func (s *Store) SeedJobs(ctx context.Context, queue string, n int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (queue, payload)
		SELECT $1, '{}'::jsonb FROM generate_series(1, $2)`, queue, n)
	if err != nil {
		return fmt.Errorf("seed %d jobs: %w", n, err)
	}
	return nil
}

// DeleteQueue removes every job on a queue, whatever its state.
func (s *Store) DeleteQueue(ctx context.Context, queue string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM jobs WHERE queue = $1`, queue); err != nil {
		return fmt.Errorf("delete queue %q: %w", queue, err)
	}
	return nil
}

// ClaimWithoutSkipLocked is Claim with the SKIP LOCKED clause removed. It is
// deliberately the wrong way to build a queue, kept so the benchmark can
// measure exactly what SKIP LOCKED buys.
//
// With plain FOR UPDATE, a worker whose chosen rows are locked by another
// worker's in-progress claim BLOCKS until that claim commits, then re-checks
// its rows, finds they are no longer pending, and returns fewer than it asked
// for -- sometimes none, even though pending jobs remain. Every worker ends
// up waiting on every other worker.
func (s *Store) ClaimWithoutSkipLocked(ctx context.Context, queue string, limit int, workerID string, lease time.Duration) ([]Job, error) {
	if limit <= 0 {
		limit = 1
	}
	const q = `
		WITH candidates AS (
			SELECT id
			FROM jobs
			WHERE queue = $1
			  AND state = 'pending'
			  AND run_at <= now()
			ORDER BY priority DESC, run_at ASC
			LIMIT $2
			FOR UPDATE
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
		return nil, fmt.Errorf("claim jobs (naive): %w", err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[Job])
	if err != nil {
		return nil, fmt.Errorf("scan claimed jobs (naive): %w", err)
	}
	return jobs, nil
}
