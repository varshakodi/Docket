// Package worker claims jobs and runs them.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varshakodi/docket/internal/backoff"
	"github.com/varshakodi/docket/internal/store"
)

// Handler is the function that does the actual work for one job.
//
// Return nil for success. Return an error to have the job retried, or wrap
// ErrPermanent to send it straight to the dead-letter queue. The ctx is
// cancelled if the worker loses its lease on the job or is forced to stop,
// so long-running handlers should watch it and return promptly.
//
// Handlers MUST be safe to run more than once for the same job. Docket
// promises at-least-once delivery: if a worker dies after finishing the work
// but before recording success, the job will run again. Make the work
// idempotent -- for example by keying any external side effect on job.ID --
// so a repeat has no additional effect.
type Handler func(ctx context.Context, job store.Job) error

// ErrPermanent marks a failure that retrying cannot fix -- a malformed
// payload, a deleted record. Wrap it (fmt.Errorf("...: %w", ErrPermanent))
// and the job goes to the dead-letter queue immediately, without burning
// through its remaining attempts.
var ErrPermanent = errors.New("permanent failure")

// Config tunes one worker process.
type Config struct {
	Queue         string
	WorkerID      string
	Concurrency   int           // jobs run at the same time; default 1
	Lease         time.Duration // how long a claim lasts without a heartbeat; default 30s
	PollInterval  time.Duration // wait when the queue is empty; default 500ms
	ShutdownGrace time.Duration // how long to let in-flight jobs finish on shutdown; default 25s
	Backoff       backoff.Config
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
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 25 * time.Second
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

// Run claims and executes jobs until ctx is cancelled, then shuts down
// gracefully:
//
//  1. stop claiming new jobs;
//  2. wait up to ShutdownGrace for in-flight jobs to finish;
//  3. if any are still running, hand them back to the queue so another
//     worker picks them up immediately rather than after the lease expires.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency)

	// Handlers get their own context, deliberately NOT derived from ctx.
	// Cancelling ctx means "stop taking new work" -- it must not yank the
	// rug from under jobs that are halfway done. hardStop is pulled only if
	// the grace period runs out.
	jobsCtx, hardStop := context.WithCancel(context.Background())
	defer hardStop()

	// A semaphore: a channel with Concurrency slots. Holding a slot means
	// "one job is running". Sending blocks when all slots are taken, which
	// is how we avoid claiming more than we can run.
	sem := make(chan struct{}, w.cfg.Concurrency)
	var inflight sync.WaitGroup

claimLoop:
	for {
		select {
		case sem <- struct{}{}: // got a slot
		case <-ctx.Done():
			break claimLoop
		}

		// Claim as many jobs as we have free slots for, in one round trip.
		// We hold one slot already; the rest are whatever is free right now.
		// Slots can only be freed (never taken) by anyone but this loop, so
		// this count is a safe lower bound.
		free := 1 + (cap(sem) - len(sem))
		jobs, err := w.store.Claim(ctx, w.cfg.Queue, free, w.cfg.WorkerID, w.cfg.Lease)
		if err != nil {
			<-sem
			if ctx.Err() != nil {
				break claimLoop
			}
			w.log.Error("claim failed", "err", err)
			w.sleep(ctx, w.cfg.PollInterval)
			continue
		}
		if len(jobs) == 0 {
			<-sem
			w.sleep(ctx, w.cfg.PollInterval)
			continue
		}

		for i, j := range jobs {
			if i > 0 {
				sem <- struct{}{} // cannot block: see `free` above
			}
			inflight.Add(1)
			go func(j store.Job) {
				defer inflight.Done()
				defer func() { <-sem }()
				w.runJob(jobsCtx, j)
			}(j)
		}
	}

	return w.shutdown(hardStop, &inflight)
}

// shutdown implements steps 2 and 3 of Run's contract.
func (w *Worker) shutdown(hardStop context.CancelFunc, inflight *sync.WaitGroup) error {
	w.log.Info("shutting down: no longer claiming; waiting for in-flight jobs",
		"grace", w.cfg.ShutdownGrace)

	done := make(chan struct{})
	go func() {
		inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.log.Info("all in-flight jobs finished; worker stopped")
		return nil
	case <-time.After(w.cfg.ShutdownGrace):
	}

	// Grace period is up. Release our jobs FIRST, then cancel the handlers.
	// Order matters: once released, the ownership guards in the store make
	// any late Complete/Fail from those handlers a harmless no-op, so a
	// handler winding down cannot overwrite the job's fresh 'pending' state.
	w.log.Warn("grace period expired; releasing in-flight jobs for another worker")
	n, err := w.store.ReleaseAll(context.Background(), w.cfg.WorkerID)
	if err != nil {
		w.log.Error("could not release jobs; they will be recovered when the lease expires", "err", err)
	} else {
		w.log.Warn("released jobs", "count", n)
	}
	hardStop()

	// Handlers that respect their context return quickly now. Bound the wait
	// anyway -- a handler that ignores ctx must not hang the process forever.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		w.log.Error("some handlers ignored cancellation; exiting anyway")
	}
	return err
}

// runJob executes one job: heartbeat in the background, run the handler,
// then record the outcome.
func (w *Worker) runJob(ctx context.Context, j store.Job) {
	log := w.log.With("job", j.ID, "attempt", j.Attempts)

	// jobCtx is what the handler sees. It is cancelled if the lease is lost
	// (by the heartbeat goroutine) or on hard stop (via its parent).
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var leaseLost atomic.Bool

	// Heartbeat: renew the lease at a third of its length, so we get several
	// chances to renew before it expires even if one round trip is slow.
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
					leaseLost.Store(true)
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

	// Record the outcome with a context that cannot be cancelled: even if
	// we are shutting down, what happened to this job must be written.
	recordCtx := context.WithoutCancel(ctx)

	switch {
	case err == nil:
		if cerr := w.store.Complete(recordCtx, j.ID, w.cfg.WorkerID); cerr != nil {
			log.Warn("finished, but could not record success; job will run again", "err", cerr)
			return
		}
		log.Info("job succeeded", "took", time.Since(start).Round(time.Millisecond))

	case ctx.Err() != nil:
		// Hard stop during shutdown. The job was already released; nothing
		// to record.
		log.Warn("stopped mid-job for shutdown; released for another worker")

	case leaseLost.Load():
		// Someone else owns it now. Any write we make would be rejected.
		log.Warn("abandoned: another worker took over after our lease expired")

	case errors.Is(err, ErrPermanent):
		if derr := w.store.MarkDead(recordCtx, j.ID, w.cfg.WorkerID, err.Error()); derr != nil {
			log.Warn("could not record permanent failure", "err", derr)
			return
		}
		log.Error("permanent failure; sent to dead-letter queue without retry", "err", err)

	default:
		delay := w.cfg.Backoff.Delay(j.Attempts)
		ferr := w.store.Fail(recordCtx, j.ID, w.cfg.WorkerID, err.Error(), delay)
		switch {
		case errors.Is(ferr, store.ErrLeaseLost):
			log.Warn("failed, but lease was already lost; outcome discarded", "err", err)
		case ferr != nil:
			log.Warn("could not record failure", "err", ferr)
		case j.Attempts >= j.MaxAttempts:
			log.Error("failed on final attempt; sent to dead-letter queue", "err", err)
		default:
			log.Warn("failed; will retry", "err", err, "retry_in", delay.Round(time.Millisecond))
		}
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
