package store

import (
	"errors"
	"testing"
	"time"
)

func TestReleaseAllOnlyTouchesOwnJobs(t *testing.T) {
	s, ctx := newTestStore(t)

	for i := 0; i < 3; i++ {
		s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	}
	mine, _ := s.Claim(ctx, "q", 2, "worker-A", time.Minute)
	theirs, _ := s.Claim(ctx, "q", 1, "worker-B", time.Minute)

	n, err := s.ReleaseAll(ctx, "worker-A")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if n != 2 {
		t.Fatalf("released %d, want 2", n)
	}

	for _, j := range mine {
		got, _ := s.Get(ctx, j.ID)
		if got.State != StatePending || got.WorkerID != nil || got.Attempts != 1 {
			t.Fatalf("released job %d: state=%s worker=%v attempts=%d", j.ID, got.State, got.WorkerID, got.Attempts)
		}
	}
	got, _ := s.Get(ctx, theirs[0].ID)
	if got.State != StateRunning || *got.WorkerID != "worker-B" {
		t.Fatal("worker-A's release touched worker-B's job")
	}

	// Released jobs are claimable right now, not after a lease timeout.
	again, _ := s.Claim(ctx, "q", 10, "worker-C", time.Minute)
	if len(again) != 2 {
		t.Fatalf("re-claim got %d jobs, want 2", len(again))
	}
}

func TestMarkDeadSkipsRemainingAttempts(t *testing.T) {
	s, ctx := newTestStore(t)

	res, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q", MaxAttempts: 5})
	s.Claim(ctx, "q", 1, "w", time.Minute)

	if err := s.MarkDead(ctx, res.ID, "w", "payload is garbage"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	got, _ := s.Get(ctx, res.ID)
	if got.State != StateDead || got.Attempts != 1 || *got.LastError != "payload is garbage" {
		t.Fatalf("after MarkDead: state=%s attempts=%d err=%v", got.State, got.Attempts, got.LastError)
	}

	// Ownership guard, same as Complete/Fail.
	res2, _ := s.Enqueue(ctx, EnqueueParams{Queue: "q"})
	s.Claim(ctx, "q", 1, "owner", time.Minute)
	if err := s.MarkDead(ctx, res2.ID, "impostor", "x"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("non-owner MarkDead returned %v, want ErrLeaseLost", err)
	}
}
