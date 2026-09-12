package store

import (
	"encoding/json"
	"errors"
	"time"
)

// State is where a job is in its lifecycle. Mirrors the job_state enum in
// the database, so the two can never disagree about what values exist.
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
)

// Job is one row of the jobs table, as Go sees it.
//
// The `db:"..."` tags tell the database driver which column fills which field.
// Pointer fields (*time.Time, *string) are columns that may be NULL: a nil
// pointer means "no value", which a plain time.Time or string cannot express.
type Job struct {
	ID             int64           `db:"id"`
	Queue          string          `db:"queue"`
	Payload        json.RawMessage `db:"payload"`
	State          State           `db:"state"`
	Priority       int             `db:"priority"`
	Attempts       int             `db:"attempts"`
	MaxAttempts    int             `db:"max_attempts"`
	RunAt          time.Time       `db:"run_at"`
	LeaseExpiresAt *time.Time      `db:"lease_expires_at"`
	WorkerID       *string         `db:"worker_id"`
	IdempotencyKey *string         `db:"idempotency_key"`
	LastError      *string         `db:"last_error"`
	CreatedAt      time.Time       `db:"created_at"`
	UpdatedAt      time.Time       `db:"updated_at"`
	CompletedAt    *time.Time      `db:"completed_at"`
}

// ErrLeaseLost is returned when a worker tries to finish a job it no longer
// owns -- its lease expired and another worker has since claimed the job.
// The right response is to stop, not to retry: someone else is handling it.
var ErrLeaseLost = errors.New("lease lost: job is no longer owned by this worker")

// ErrNotFound is returned when a job ID does not exist.
var ErrNotFound = errors.New("job not found")
