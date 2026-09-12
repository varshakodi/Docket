// Package store is the only package that talks SQL to PostgreSQL.
//
// Everything above it -- the worker, the CLI, the API -- goes through the
// methods here. Keeping all SQL in one place means there is exactly one file
// to read to understand how the queue uses the database.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store holds a pool of database connections.
//
// Opening a connection to Postgres is slow (a network handshake plus
// authentication), so we open a handful up front and reuse them. Every query
// borrows one from the pool and returns it when done.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to the database and verifies it answers.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases every connection in the pool.
func (s *Store) Close() {
	s.pool.Close()
}
