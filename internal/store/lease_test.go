package store

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func noDelay(int) time.Duration { return 0 }

func TestReapExpiredReturnsJobToPending(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	// A 1ms lease expires almost immediately -- simulating a worker that
	// claimed the job and then died without ever heartbeating.
	if _, err := s.Claim(ctx, "q", 1, "dead-worker", time.Millisecond); err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	n, err := s.ReapExpired(ctx, 100, noDelay)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}

	got, _ := s.Get(ctx, res.ID)
	if got.State != StatePending || got.WorkerID != nil || got.LeaseExpiresAt != nil {
		t.Fatalf("after reap: state=%s worker=%v lease=%v", got.State, got.WorkerID, got.LeaseExpiresAt)
	}
	if got.Attempts != 1 || got.LastError == nil {
		t.Fatalf("reap should keep attempts=1 and record an error; got attempts=%d err=%v", got.Attempts, got.LastError)
	}

	// And it is claimable again, by a different worker, as attempt 2.
	jobs, _ := s.Claim(ctx, "q", 1, "fresh-worker", time.Minute)
	if len(jobs) != 1 || jobs[0].Attempts != 2 {
		t.Fatalf("re-claim: got %d jobs, attempts=%v", len(jobs), jobs)
	}
}

func TestReapExpiredKillsExhaustedJob(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q", MaxAttempts: 1})
	s.Claim(ctx, "q", 1, "w", time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	if _, err := s.ReapExpired(ctx, 100, noDelay); err != nil {
		t.Fatalf("reap: %v", err)
	}
	got, _ := s.Get(ctx, res.ID)
	if got.State != StateDead {
		t.Fatalf("job with max_attempts=1 should be dead after reap, got %s", got.State)
	}
}

func TestReapIgnoresLiveLeases(t *testing.T) {
	s, ctx := newTestStore(t)

	s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	s.Claim(ctx, "q", 1, "w", time.Minute) // healthy lease, far in the future

	n, err := s.ReapExpired(ctx, 100, noDelay)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 0 {
		t.Fatalf("reaped a live lease")
	}
}

func TestExtendLeaseOnlyForOwner(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	jobs, _ := s.Claim(ctx, "q", 1, "worker-A", time.Second)
	before := *jobs[0].LeaseExpiresAt

	// Someone who doesn't own the job can't extend it.
	if err := s.ExtendLease(ctx, res.ID, "worker-B", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("non-owner extend returned %v, want ErrLeaseLost", err)
	}

	// The owner can, and the deadline moves forward.
	if err := s.ExtendLease(ctx, res.ID, "worker-A", time.Minute); err != nil {
		t.Fatalf("owner extend: %v", err)
	}
	got, _ := s.Get(ctx, res.ID)
	if !got.LeaseExpiresAt.After(before) {
		t.Fatalf("lease did not move forward: before=%v after=%v", before, got.LeaseExpiresAt)
	}
}

// Two reapers running at once must not double-process. Each expired job is
// requeued exactly once between them.
func TestConcurrentReapersDoNotDoubleProcess(t *testing.T) {
	s, ctx := newTestStore(t)

	const n = 60
	for i := 0; i < n; i++ {
		s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	}
	if _, err := s.Claim(ctx, "q", n, "dead-worker", time.Millisecond); err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	counts := make([]int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := s.ReapExpired(ctx, 100, noDelay)
			if err != nil {
				t.Errorf("reaper %d: %v", i, err)
			}
			counts[i] = c
		}(i)
	}
	wg.Wait()

	if counts[0]+counts[1] != n {
		t.Fatalf("reapers processed %d + %d = %d jobs, want exactly %d", counts[0], counts[1], counts[0]+counts[1], n)
	}
}

func TestRequeueDeadJob(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q", MaxAttempts: 1})
	jobs, _ := s.Claim(ctx, "q", 1, "w", time.Minute)
	s.Fail(ctx, jobs[0].ID, "w", "broken", 0)

	dead, _ := s.ListDead(ctx, "", 10)
	if len(dead) != 1 || dead[0].ID != res.ID {
		t.Fatalf("ListDead returned %v", dead)
	}

	if err := s.Requeue(ctx, res.ID); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	got, _ := s.Get(ctx, res.ID)
	if got.State != StatePending || got.Attempts != 0 || got.LastError != nil {
		t.Fatalf("after requeue: state=%s attempts=%d err=%v", got.State, got.Attempts, got.LastError)
	}

	// Requeueing something that isn't dead is an error.
	if err := s.Requeue(ctx, res.ID); err == nil {
		t.Fatal("requeue of a pending job should fail")
	}
}
