package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ExtendLease pushes a running job's deadline further into the future. This
// is the "heartbeat": a worker on a long job calls it periodically to say
// "still alive, still working".
//
// It only succeeds if this worker still owns the job. If the lease already
// expired and the reaper handed the job to someone else, we get ErrLeaseLost
// -- and the correct response is to stop working on it immediately.
func (s *Store) ExtendLease(ctx context.Context, id int64, workerID string, lease time.Duration) error {
	const q = `
		UPDATE jobs
		SET lease_expires_at = now() + $3::interval,
		    updated_at       = now()
		WHERE id = $1
		  AND state = 'running'
		  AND worker_id = $2`

	tag, err := s.pool.Exec(ctx, q, id, workerID, lease)
	if err != nil {
		return fmt.Errorf("extend lease on job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ReapExpired finds running jobs whose lease has passed -- meaning the worker
// that claimed them stopped heartbeating, almost certainly because it died --
// and returns them to the queue. Jobs out of attempts go to 'dead' instead.
//
// delayFor decides how long to wait before the job is retried, given its
// attempt count. It is passed in so this package stays free of retry policy.
//
// Safe to run from many processes at once: the UPDATE re-checks that the
// lease is still expired, so if another reaper (or a late heartbeat) got
// there first, this one simply touches zero rows for that job.
func (s *Store) ReapExpired(ctx context.Context, limit int, delayFor func(attempts int) time.Duration) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, attempts
		FROM jobs
		WHERE state = 'running' AND lease_expires_at < now()
		LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("find expired leases: %w", err)
	}

	type expired struct {
		id       int64
		attempts int
	}
	var found []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.attempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired lease: %w", err)
		}
		found = append(found, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read expired leases: %w", err)
	}

	const requeue = `
		UPDATE jobs
		SET state = CASE WHEN attempts >= max_attempts THEN 'dead'::job_state
		                 ELSE 'pending'::job_state END,
		    run_at           = now() + $2::interval,
		    last_error       = 'lease expired: worker stopped responding',
		    lease_expires_at = NULL,
		    worker_id        = NULL,
		    updated_at       = now()
		WHERE id = $1
		  AND state = 'running'
		  AND lease_expires_at < now()`

	reaped := 0
	for _, e := range found {
		tag, err := s.pool.Exec(ctx, requeue, e.id, delayFor(e.attempts))
		if err != nil {
			return reaped, fmt.Errorf("requeue job %d: %w", e.id, err)
		}
		reaped += int(tag.RowsAffected())
	}
	return reaped, nil
}

// ListDead returns jobs in the dead-letter queue, newest first.
// An empty queue name means all queues.
func (s *Store) ListDead(ctx context.Context, queue string, limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT * FROM jobs
		WHERE state = 'dead' AND ($1 = '' OR queue = $1)
		ORDER BY updated_at DESC
		LIMIT $2`, queue, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[Job])
	if err != nil {
		return nil, fmt.Errorf("scan dead jobs: %w", err)
	}
	return jobs, nil
}

// Requeue moves a dead job back to pending with a fresh attempt budget.
// Used when a human has fixed whatever made it fail.
func (s *Store) Requeue(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET state      = 'pending',
		    attempts   = 0,
		    run_at     = now(),
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $1 AND state = 'dead'`, id)
	if err != nil {
		return fmt.Errorf("requeue job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("job %d is not in the dead-letter queue", id)
	}
	return nil
}
