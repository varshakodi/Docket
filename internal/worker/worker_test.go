package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/varshakodi/docket/internal/backoff"
	"github.com/varshakodi/docket/internal/reaper"
	"github.com/varshakodi/docket/internal/store"
)

func testDSN() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/docket_test?sslmode=disable"
}

func TestMain(m *testing.M) {
	if err := store.Migrate(testDSN(), "up"); err != nil {
		panic("migrate: " + err.Error())
	}
	os.Exit(m.Run())
}

func newStore(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	s, err := store.New(ctx, testDSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(s.Close)
	return s, ctx
}

// waitFor polls until the job reaches one of the wanted states, or times out.
func waitFor(t *testing.T, s *store.Store, id int64, timeout time.Duration, want ...store.State) store.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		for _, w := range want {
			if j.State == w {
				return j
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	j, _ := s.Get(context.Background(), id)
	t.Fatalf("job %d did not reach %v within %v (state=%s attempts=%d)", id, want, timeout, j.State, j.Attempts)
	return j
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestWorkerRunsHandlerAndCompletes(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-ok-" + t.Name()

	got := make(chan int64, 1)
	w := New(s, Config{Queue: queue, PollInterval: 20 * time.Millisecond}, func(_ context.Context, j store.Job) error {
		got <- j.ID
		return nil
	}, quietLog())

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go w.Run(ctx)

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	select {
	case id := <-got:
		if id != res.ID {
			t.Fatalf("handler got job %d, want %d", id, res.ID)
		}
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
	j := waitFor(t, s, res.ID, 2*time.Second, store.StateSucceeded)
	if j.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", j.Attempts)
	}
}

func TestWorkerRetriesThenDeadLetters(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-fail-" + t.Name()

	w := New(s, Config{
		Queue:        queue,
		PollInterval: 20 * time.Millisecond,
		Backoff:      backoff.Config{Base: time.Millisecond, Max: 5 * time.Millisecond},
	}, func(context.Context, store.Job) error {
		return errors.New("always broken")
	}, quietLog())

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go w.Run(ctx)

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, MaxAttempts: 3})
	j := waitFor(t, s, res.ID, 4*time.Second, store.StateDead)
	if j.Attempts != 3 || j.LastError == nil || *j.LastError != "always broken" {
		t.Fatalf("dead job: attempts=%d err=%v", j.Attempts, j.LastError)
	}
}

func TestWorkerSurvivesPanickingHandler(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-panic-" + t.Name()

	calls := 0
	w := New(s, Config{
		Queue:        queue,
		PollInterval: 20 * time.Millisecond,
		Backoff:      backoff.Config{Base: time.Millisecond, Max: 5 * time.Millisecond},
	}, func(context.Context, store.Job) error {
		calls++
		if calls == 1 {
			panic("boom")
		}
		return nil
	}, quietLog())

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go w.Run(ctx)

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	// First attempt panics -> retried -> second attempt succeeds. The worker
	// itself must keep running through the panic.
	j := waitFor(t, s, res.ID, 4*time.Second, store.StateSucceeded)
	if j.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2 (one panic, one success)", j.Attempts)
	}
}

// The heartbeat test: a job that takes far longer than its lease must NOT be
// stolen by the reaper, because the worker keeps renewing the lease.
func TestHeartbeatKeepsLongJobAlive(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-heartbeat-" + t.Name()

	const lease = 300 * time.Millisecond
	const jobDuration = 1200 * time.Millisecond // 4x the lease

	w := New(s, Config{Queue: queue, Lease: lease, PollInterval: 20 * time.Millisecond}, func(ctx context.Context, j store.Job) error {
		select {
		case <-time.After(jobDuration):
			return nil
		case <-ctx.Done():
			return ctx.Err() // would only happen if the lease were lost
		}
	}, quietLog())

	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	go w.Run(ctx)
	// An aggressive reaper sweeping every 50ms, looking for a chance to steal it.
	go reaper.Run(ctx, s, 50*time.Millisecond, backoff.Config{}, quietLog())

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	j := waitFor(t, s, res.ID, 5*time.Second, store.StateSucceeded, store.StateDead)
	if j.State != store.StateSucceeded {
		t.Fatalf("job ended %s", j.State)
	}
	if j.Attempts != 1 {
		t.Fatalf("job was reaped and re-run: attempts=%d, want 1", j.Attempts)
	}
}

// The inverse: with no heartbeat (a dead worker), the reaper DOES recover the
// job and a live worker finishes it.
func TestReaperRecoversJobFromDeadWorker(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-recover-" + t.Name()

	res, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	// Claim directly, with a short lease, and never heartbeat -- a dead worker.
	if _, err := s.Claim(ctx, queue, 1, "ghost", 100*time.Millisecond); err != nil {
		t.Fatalf("claim: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	go reaper.Run(ctx, s, 50*time.Millisecond, backoff.Config{Base: time.Millisecond, Max: time.Millisecond}, quietLog())
	w := New(s, Config{Queue: queue, PollInterval: 20 * time.Millisecond}, func(context.Context, store.Job) error {
		return nil
	}, quietLog())
	go w.Run(ctx)

	j := waitFor(t, s, res.ID, 5*time.Second, store.StateSucceeded)
	if j.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2 (ghost's attempt + the real one)", j.Attempts)
	}
}
