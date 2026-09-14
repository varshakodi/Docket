// Package reaper recovers jobs whose worker died.
//
// A worker that is killed mid-job never gets to release its claim. Nothing
// in the database changes -- except that time passes and the job's lease
// deadline goes by. The reaper is the loop that notices, and hands the job
// back to the queue so someone else can run it.
package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/varshakodi/docket/internal/backoff"
	"github.com/varshakodi/docket/internal/metrics"
	"github.com/varshakodi/docket/internal/store"
)

// Run sweeps for expired leases every `interval` until ctx is cancelled.
//
// It runs inside every worker process rather than as a separate service.
// That is deliberate: there is no "the reaper" that could itself die and
// leave the system stuck. The sweep is safe to run from many processes at
// once (see store.ReapExpired), so redundancy is free.
func Run(ctx context.Context, s *store.Store, interval time.Duration, bo backoff.Config, log *slog.Logger) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		n, err := s.ReapExpired(ctx, 100, bo.Delay)
		if err != nil {
			if ctx.Err() == nil { // don't log the shutdown itself as an error
				log.Error("reap failed", "err", err)
			}
			continue
		}
		if n > 0 {
			metrics.LeasesReaped.Add(float64(n))
			log.Warn("recovered jobs from dead workers", "count", n)
		}
	}
}
