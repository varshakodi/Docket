package worker

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varshakodi/docket/internal/store"
)

func TestConcurrencyRunsJobsInParallel(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-parallel-" + t.Name()

	const concurrency = 4
	const jobs = 8
	const each = 200 * time.Millisecond

	var running, peak atomic.Int32
	w := New(s, Config{Queue: queue, Concurrency: concurrency, PollInterval: 10 * time.Millisecond},
		func(ctx context.Context, j store.Job) error {
			n := running.Add(1)
			defer running.Add(-1)
			for { // record the highest number seen running at once
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(each)
			return nil
		}, quietLog())

	var ids []int64
	for i := 0; i < jobs; i++ {
		res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
		ids = append(ids, res.ID)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	go w.Run(ctx)
	for _, id := range ids {
		waitFor(t, s, id, 5*time.Second, store.StateSucceeded)
	}
	elapsed := time.Since(start)

	// Serial would take 8 x 200ms = 1.6s. With 4 lanes: ~2 rounds = ~400ms.
	if elapsed > time.Second {
		t.Fatalf("took %v; jobs are not running in parallel", elapsed)
	}
	if got := peak.Load(); got != concurrency {
		t.Fatalf("peak concurrency %d, want exactly %d", got, concurrency)
	}
}

func TestGracefulShutdownFinishesInFlightJob(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-graceful-" + t.Name()

	const jobDuration = 400 * time.Millisecond
	w := New(s, Config{Queue: queue, PollInterval: 10 * time.Millisecond, ShutdownGrace: 5 * time.Second},
		func(ctx context.Context, j store.Job) error {
			select {
			case <-time.After(jobDuration):
				return nil
			case <-ctx.Done():
				return ctx.Err() // must NOT happen: graceful shutdown lets us finish
			}
		}, quietLog())

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runDone := make(chan struct{})
	go func() { w.Run(runCtx); close(runDone) }()

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	waitFor(t, s, res.ID, 2*time.Second, store.StateRunning)

	stopped := time.Now()
	stop() // "shut down" while the job is mid-flight

	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
	if time.Since(stopped) < jobDuration/2 {
		t.Fatal("Run returned before the in-flight job could have finished")
	}
	j, _ := s.Get(ctx, res.ID)
	if j.State != store.StateSucceeded || j.Attempts != 1 {
		t.Fatalf("after graceful shutdown: state=%s attempts=%d, want succeeded/1", j.State, j.Attempts)
	}
}

func TestShutdownReleasesJobWhenGraceExpires(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-release-" + t.Name()

	// A handler that never finishes on its own -- only stops when told.
	w := New(s, Config{Queue: queue, PollInterval: 10 * time.Millisecond, ShutdownGrace: 200 * time.Millisecond},
		func(ctx context.Context, j store.Job) error {
			<-ctx.Done()
			return ctx.Err()
		}, quietLog())

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runDone := make(chan struct{})
	go func() { w.Run(runCtx); close(runDone) }()

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	waitFor(t, s, res.ID, 2*time.Second, store.StateRunning)

	stop()
	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after grace period")
	}

	// The job went back to the queue -- pending, unowned, claimable now,
	// with no failure recorded (nothing was wrong with the job).
	j, _ := s.Get(ctx, res.ID)
	if j.State != store.StatePending || j.WorkerID != nil || j.LeaseExpiresAt != nil || j.LastError != nil {
		t.Fatalf("after forced release: state=%s worker=%v lease=%v err=%v", j.State, j.WorkerID, j.LeaseExpiresAt, j.LastError)
	}
	if j.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1 (the claim counted)", j.Attempts)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-permanent-" + t.Name()

	w := New(s, Config{Queue: queue, PollInterval: 10 * time.Millisecond},
		func(ctx context.Context, j store.Job) error {
			return fmt.Errorf("payload has no recipient: %w", ErrPermanent)
		}, quietLog())

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go w.Run(ctx)

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, MaxAttempts: 5})
	j := waitFor(t, s, res.ID, 3*time.Second, store.StateDead)
	if j.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1: a permanent error must not be retried", j.Attempts)
	}
}
