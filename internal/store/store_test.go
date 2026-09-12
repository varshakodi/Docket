package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// These tests run against a real PostgreSQL, never a mock. The thing under
// test IS the SQL -- mocking it would test nothing.
//
// Locally they use docket_test; in CI, DATABASE_URL points at the service
// container. Each test starts by emptying the jobs table.

func testDSN() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/docket_test?sslmode=disable"
}

func TestMain(m *testing.M) {
	if err := Migrate(testDSN(), "up"); err != nil {
		panic("migrate test database: " + err.Error())
	}
	os.Exit(m.Run())
}

func newTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	s, err := New(ctx, testDSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(ctx, `TRUNCATE jobs RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s, ctx
}

func TestEnqueueClaimComplete(t *testing.T) {
	s, ctx := newTestStore(t)

	res, err := s.Enqueue(ctx, EnqueueParams{Queue: "email", Payload: json.RawMessage(`{"to":"a@b.c"}`)})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if res.Deduplicated {
		t.Fatal("first enqueue should not be deduplicated")
	}

	jobs, err := s.Claim(ctx, "email", 10, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(jobs))
	}
	j := jobs[0]
	if j.ID != res.ID || j.State != StateRunning || j.Attempts != 1 {
		t.Fatalf("unexpected claimed job: id=%d state=%s attempts=%d", j.ID, j.State, j.Attempts)
	}
	if j.WorkerID == nil || *j.WorkerID != "worker-1" || j.LeaseExpiresAt == nil {
		t.Fatal("claimed job should carry worker id and lease expiry")
	}

	// Nothing left to claim: the one job is running, not pending.
	again, err := s.Claim(ctx, "email", 10, "worker-2", 30*time.Second)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second claim returned %d jobs, want 0", len(again))
	}

	if err := s.Complete(ctx, j.ID, "worker-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != StateSucceeded || got.CompletedAt == nil || got.WorkerID != nil {
		t.Fatalf("after complete: state=%s completed_at=%v worker=%v", got.State, got.CompletedAt, got.WorkerID)
	}
}

func TestEnqueueDeduplicatesByKey(t *testing.T) {
	s, ctx := newTestStore(t)

	first, err := s.Enqueue(ctx, EnqueueParams{Queue: "email", IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	second, err := s.Enqueue(ctx, EnqueueParams{Queue: "email", IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if second.ID != first.ID || !second.Deduplicated {
		t.Fatalf("second enqueue: id=%d dedup=%v, want id=%d dedup=true", second.ID, second.Deduplicated, first.ID)
	}

	// Same key on a DIFFERENT queue is a different job.
	other, err := s.Enqueue(ctx, EnqueueParams{Queue: "sms", IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatalf("other-queue enqueue: %v", err)
	}
	if other.ID == first.ID || other.Deduplicated {
		t.Fatal("same key on a different queue should create a new job")
	}
}

func TestClaimIgnoresFutureJobs(t *testing.T) {
	s, ctx := newTestStore(t)

	if _, err := s.Enqueue(ctx, EnqueueParams{Queue: "q", RunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	jobs, err := s.Claim(ctx, "q", 10, "w", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("claimed a job scheduled an hour from now")
	}
}

// TestClaimConcurrentNoDuplicates is the SKIP LOCKED correctness test.
//
// Sixteen workers hammer one queue at the same time. Every job must be
// claimed exactly once: never twice, never zero times. Run with -race.
func TestClaimConcurrentNoDuplicates(t *testing.T) {
	s, ctx := newTestStore(t)

	const totalJobs = 500
	const workers = 16

	for i := 0; i < totalJobs; i++ {
		if _, err := s.Enqueue(ctx, EnqueueParams{Queue: "load"}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var (
		mu     sync.Mutex
		seen   = make(map[int64]int) // job id -> how many times claimed
		wg     sync.WaitGroup
		errsCh = make(chan error, workers)
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			for {
				jobs, err := s.Claim(ctx, "load", 5, workerID, time.Minute)
				if err != nil {
					errsCh <- err
					return
				}
				if len(jobs) == 0 {
					return // queue drained
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}("worker-" + string(rune('A'+w)))
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		t.Fatalf("worker error: %v", err)
	}

	if len(seen) != totalJobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), totalJobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d was claimed %d times", id, n)
		}
	}
}

func TestCompleteRejectsStaleWorker(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	if _, err := s.Claim(ctx, "q", 1, "worker-A", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Simulate the lease being reaped and re-claimed by another worker.
	if _, err := s.pool.Exec(ctx, `UPDATE jobs SET worker_id = 'worker-B' WHERE id = $1`, res.ID); err != nil {
		t.Fatalf("reassign: %v", err)
	}

	err := s.Complete(ctx, res.ID, "worker-A")
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale complete returned %v, want ErrLeaseLost", err)
	}
	// The job must still be running under worker-B, untouched.
	got, _ := s.Get(ctx, res.ID)
	if got.State != StateRunning || got.WorkerID == nil || *got.WorkerID != "worker-B" {
		t.Fatalf("stale worker modified the job: state=%s worker=%v", got.State, got.WorkerID)
	}
}

func TestFailRetriesThenDies(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q", MaxAttempts: 2})

	// Attempt 1 fails -> back to pending.
	jobs, _ := s.Claim(ctx, "q", 1, "w", time.Minute)
	if err := s.Fail(ctx, jobs[0].ID, "w", "boom", 0); err != nil {
		t.Fatalf("fail 1: %v", err)
	}
	got, _ := s.Get(ctx, res.ID)
	if got.State != StatePending || got.Attempts != 1 || got.LastError == nil || *got.LastError != "boom" {
		t.Fatalf("after first failure: state=%s attempts=%d err=%v", got.State, got.Attempts, got.LastError)
	}

	// Attempt 2 fails -> attempts exhausted -> dead.
	jobs, _ = s.Claim(ctx, "q", 1, "w", time.Minute)
	if len(jobs) != 1 {
		t.Fatalf("job should be claimable again, got %d", len(jobs))
	}
	if err := s.Fail(ctx, jobs[0].ID, "w", "boom again", 0); err != nil {
		t.Fatalf("fail 2: %v", err)
	}
	got, _ = s.Get(ctx, res.ID)
	if got.State != StateDead || got.Attempts != 2 {
		t.Fatalf("after second failure: state=%s attempts=%d, want dead/2", got.State, got.Attempts)
	}

	// Dead jobs are not claimable.
	jobs, _ = s.Claim(ctx, "q", 1, "w", time.Minute)
	if len(jobs) != 0 {
		t.Fatal("claimed a dead job")
	}
}
