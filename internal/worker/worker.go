// Package worker claims jobs and runs them.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/varshakodi/docket/internal/backoff"
	"github.com/varshakodi/docket/internal/store"
)

// Handler is the function that does the actual work for one job.
//
// Return nil for success. Return an error to have the job retried (or sent
// to the dead-letter queue once attempts run out). The ctx is cancelled if
// the worker loses its lease on the job, so long-running handlers should
// check it and stop.
type Handler func(ctx context.Context, job store.Job) error

// Config tunes one worker.
type Config struct {
	Queue        string
	WorkerID     string
	Lease        time.Duration // how long a claim lasts without a heartbeat; default 30s
	PollInterval time.Duration // how long to wait when the queue is empty; default 500ms
	Backoff      backoff.Config
}

// Worker pulls jobs from one queue and runs a Handler on each.
type Worker struct {
	cfg    Config
	store  *store.Store
	handle Handler
	log    *slog.Logger
}

// New builds a worker. Zero-valued config fields get sensible defaults.
func New(s *store.Store, cfg Config, h Handler, log *slog.Logger) *Worker {
	if cfg.Queue == "" {
		cfg.Queue = "default"
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if log == nil {
		log = slog.Default()
	}
	return &Worker{
		cfg:    cfg,
		store:  s,
		handle: h,
		log:    log.With("worker", cfg.WorkerID, "queue", cfg.Queue),
	}
}

// Run claims and executes jobs until ctx is cancelled. One job at a time
// for now; concurrency within a worker comes later.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started")
	for {
		if ctx.Err() != nil {
			w.log.Info("worker stopped")
			return nil
		}

		jobs, err := w.store.Claim(ctx, w.cfg.Queue, 1, w.cfg.WorkerID, w.cfg.Lease)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("claim failed", "err", err)
			w.sleep(ctx, w.cfg.PollInterval)
			continue
		}
		if len(jobs) == 0 {
			w.sleep(ctx, w.cfg.PollInterval)
			continue
		}
		for _, j := range jobs {
			w.runJob(ctx, j)
		}
	}
}

// runJob executes one job: heartbeat in the background, run the handler,
// then record the outcome.
func (w *Worker) runJob(ctx context.Context, j store.Job) {
	log := w.log.With("job", j.ID, "attempt", j.Attempts)

	// jobCtx is what the handler sees. We cancel it if the lease is lost, so
	// a handler that respects its context stops promptly instead of finishing
	// work that another worker has already been handed.
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat: extend the lease at a third of its length, so we get a few
	// chances to renew before it expires even if one heartbeat is slow.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		t := time.NewTicker(w.cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				err := w.store.ExtendLease(jobCtx, j.ID, w.cfg.WorkerID, w.cfg.Lease)
				if errors.Is(err, store.ErrLeaseLost) {
					log.Warn("lease lost mid-job; abandoning it to its new owner")
					cancel()
					return
				}
				if err != nil && jobCtx.Err() == nil {
					log.Warn("heartbeat failed", "err", err)
				}
			}
		}
	}()

	start := time.Now()
	err := w.safeHandle(jobCtx, j)
	cancel() // stop the heartbeat
	<-heartbeatDone

	// The parent ctx may be cancelled (Ctrl-C) while we were mid-job. We
	// still want to record what happened, so the final write ignores that
	// cancellation. context.WithoutCancel keeps the values, drops the
	// cancel.
	recordCtx := context.WithoutCancel(ctx)

	if err == nil {
		if cerr := w.store.Complete(recordCtx, j.ID, w.cfg.WorkerID); cerr != nil {
			log.Warn("could not record success", "err", cerr)
			return
		}
		log.Info("job succeeded", "took", time.Since(start).Round(time.Millisecond))
		return
	}

	delay := w.cfg.Backoff.Delay(j.Attempts)
	ferr := w.store.Fail(recordCtx, j.ID, w.cfg.WorkerID, err.Error(), delay)
	switch {
	case errors.Is(ferr, store.ErrLeaseLost):
		log.Warn("job failed but lease was already lost; outcome discarded", "err", err)
	case ferr != nil:
		log.Warn("could not record failure", "err", ferr)
	case j.Attempts >= j.MaxAttempts:
		log.Error("job failed permanently; moved to dead-letter queue", "err", err)
	default:
		log.Warn("job failed; will retry", "err", err, "retry_in", delay.Round(time.Millisecond))
	}
}

// safeHandle runs the handler and converts a panic into an ordinary error,
// so one bad job cannot take down the whole worker.
func (w *Worker) safeHandle(ctx context.Context, j store.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return w.handle(ctx, j)
}

func (w *Worker) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
