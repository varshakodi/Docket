package store

import (
	"context"
	"fmt"
)

// ReleaseAll hands every job this worker is running back to the queue,
// immediately claimable. Used on graceful shutdown when the grace period
// runs out: rather than leave jobs stranded until their lease expires,
// give them up now so another worker picks them up in milliseconds.
//
// The attempt already counted at claim time stays counted.
func (s *Store) ReleaseAll(ctx context.Context, workerID string) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET state            = 'pending',
		    run_at           = now(),
		    lease_expires_at = NULL,
		    worker_id        = NULL,
		    updated_at       = now()
		WHERE state = 'running' AND worker_id = $1`, workerID)
	if err != nil {
		return 0, fmt.Errorf("release jobs for %s: %w", workerID, err)
	}
	return int(tag.RowsAffected()), nil
}

// MarkDead sends a job straight to the dead-letter queue, skipping any
// remaining attempts. For failures that retrying cannot fix.
// Same ownership guard as Complete and Fail.
func (s *Store) MarkDead(ctx context.Context, id int64, workerID, reason string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET state            = 'dead',
		    last_error       = $3,
		    lease_expires_at = NULL,
		    worker_id        = NULL,
		    updated_at       = now()
		WHERE id = $1
		  AND state = 'running'
		  AND worker_id = $2`, id, workerID, reason)
	if err != nil {
		return fmt.Errorf("mark job %d dead: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}
