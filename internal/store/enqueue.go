package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// EnqueueParams describes a job to add to the queue.
type EnqueueParams struct {
	Queue          string
	Payload        json.RawMessage
	Priority       int
	MaxAttempts    int       // 0 means use the default (5)
	RunAt          time.Time // zero means "as soon as possible"
	IdempotencyKey string    // "" means no deduplication
}

// EnqueueResult is what Enqueue hands back.
type EnqueueResult struct {
	ID int64
	// Deduplicated is true when a job with the same idempotency key already
	// existed, so no new job was created and ID refers to the existing one.
	Deduplicated bool
}

// Enqueue adds a job. It returns only after the row is committed, so a
// returned ID is a promise: the job exists and will survive a crash.
func (s *Store) Enqueue(ctx context.Context, p EnqueueParams) (EnqueueResult, error) {
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 5
	}
	if p.RunAt.IsZero() {
		p.RunAt = time.Now()
	}
	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage("{}")
	}

	// A NULL key means "no dedupe". An empty string would be a real key that
	// every keyless job shared, which is the opposite of what we want.
	var key *string
	if p.IdempotencyKey != "" {
		key = &p.IdempotencyKey
	}

	// ON CONFLICT ... DO NOTHING: if a job with this (queue, key) already
	// exists, insert nothing and return no rows. That "no rows" case is how
	// we detect a duplicate below.
	const insert = `
		INSERT INTO jobs (queue, payload, priority, max_attempts, run_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id`

	var id int64
	err := s.pool.QueryRow(ctx, insert,
		p.Queue, p.Payload, p.Priority, p.MaxAttempts, p.RunAt, key,
	).Scan(&id)

	if err == nil {
		return EnqueueResult{ID: id}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return EnqueueResult{}, fmt.Errorf("insert job: %w", err)
	}

	// No row came back, so the insert was skipped: the key already exists.
	// Look up the job it collided with and return that instead.
	const lookup = `SELECT id FROM jobs WHERE queue = $1 AND idempotency_key = $2`
	if err := s.pool.QueryRow(ctx, lookup, p.Queue, key).Scan(&id); err != nil {
		return EnqueueResult{}, fmt.Errorf("look up existing job: %w", err)
	}
	return EnqueueResult{ID: id, Deduplicated: true}, nil
}
