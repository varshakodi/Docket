package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ListFilter narrows a job listing. Empty fields mean "any".
type ListFilter struct {
	Queue string
	State State
	Limit int // default 50, max 500
}

// ListJobs returns the most recently updated jobs matching the filter.
func (s *Store) ListJobs(ctx context.Context, f ListFilter) ([]Job, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > 500 {
		f.Limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT * FROM jobs
		WHERE ($1 = '' OR queue = $1)
		  AND ($2 = '' OR state::text = $2)
		ORDER BY updated_at DESC, id DESC
		LIMIT $3`, f.Queue, string(f.State), f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[Job])
	if err != nil {
		return nil, fmt.Errorf("scan jobs: %w", err)
	}
	return jobs, nil
}
