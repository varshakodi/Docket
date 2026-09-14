package store

import (
	"context"
	"fmt"
)

// QueueStat is how many jobs a queue has in one state.
type QueueStat struct {
	Queue string
	State State
	Count int64
}

// PendingAge is how long a queue's oldest runnable job has been waiting.
type PendingAge struct {
	Queue   string
	Seconds float64
}

// QueueStats reports the backlog: job counts per queue and state, and the
// age of the oldest job that is ready to run but not yet claimed.
func (s *Store) QueueStats(ctx context.Context) ([]QueueStat, []PendingAge, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue, state, count(*)
		FROM jobs
		GROUP BY queue, state
		ORDER BY queue, state`)
	if err != nil {
		return nil, nil, fmt.Errorf("count jobs: %w", err)
	}
	var depths []QueueStat
	for rows.Next() {
		var d QueueStat
		if err := rows.Scan(&d.Queue, &d.State, &d.Count); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan queue stat: %w", err)
		}
		depths = append(depths, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read queue stats: %w", err)
	}

	// "Ready to run" means pending AND its run_at has arrived. A job
	// scheduled for tomorrow is not falling behind.
	rows, err = s.pool.Query(ctx, `
		SELECT queue, extract(epoch FROM now() - min(run_at))::float8
		FROM jobs
		WHERE state = 'pending' AND run_at <= now()
		GROUP BY queue
		ORDER BY queue`)
	if err != nil {
		return nil, nil, fmt.Errorf("oldest pending: %w", err)
	}
	defer rows.Close()
	var ages []PendingAge
	for rows.Next() {
		var a PendingAge
		if err := rows.Scan(&a.Queue, &a.Seconds); err != nil {
			return nil, nil, fmt.Errorf("scan pending age: %w", err)
		}
		ages = append(ages, a)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read pending ages: %w", err)
	}
	return depths, ages, nil
}
