package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Complete marks a job as succeeded.
//
// Note the WHERE clause: the job must still be 'running' AND still owned by
// this worker. If our lease expired while we were working and another worker
// has since claimed the job, worker_id no longer matches, zero rows update,
// and we return ErrLeaseLost. Without that guard a slow, stale worker could
// mark a job done that a fresh worker is halfway through -- a lost update.
func (s *Store) Complete(ctx context.Context, id int64, workerID string) error {
	const q = `
		UPDATE jobs
		SET state            = 'succeeded',
		    completed_at     = now(),
		    updated_at       = now(),
		    lease_expires_at = NULL,
		    worker_id        = NULL
		WHERE id = $1
		  AND state = 'running'
		  AND worker_id = $2`

	tag, err := s.pool.Exec(ctx, q, id, workerID)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail records that an attempt failed and decides what happens next.
//
// If attempts are exhausted the job goes to 'dead' (the dead-letter queue --
// a holding area for jobs that gave up, kept for inspection rather than
// deleted). Otherwise it returns to 'pending', scheduled retryAfter from now.
// The caller chooses retryAfter; the store doesn't know about backoff policy.
//
// The same ownership guard as Complete applies.
func (s *Store) Fail(ctx context.Context, id int64, workerID, reason string, retryAfter time.Duration) error {
	const q = `
		UPDATE jobs
		SET state = CASE WHEN attempts >= max_attempts THEN 'dead'::job_state
		                 ELSE 'pending'::job_state END,
		    run_at           = now() + $4::interval,
		    last_error       = $3,
		    lease_expires_at = NULL,
		    worker_id        = NULL,
		    updated_at       = now()
		WHERE id = $1
		  AND state = 'running'
		  AND worker_id = $2`

	tag, err := s.pool.Exec(ctx, q, id, workerID, reason, retryAfter)
	if err != nil {
		return fmt.Errorf("fail job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Get fetches a single job by ID.
func (s *Store) Get(ctx context.Context, id int64) (Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT * FROM jobs WHERE id = $1`, id)
	if err != nil {
		return Job{}, fmt.Errorf("get job %d: %w", id, err)
	}
	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[Job])
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("scan job %d: %w", id, err)
	}
	return job, nil
}
