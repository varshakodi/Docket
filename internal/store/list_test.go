package store

import (
	"testing"
	"time"
)

func TestListJobsFilters(t *testing.T) {
	s, ctx := newTestStore(t)

	s.Enqueue(ctx, EnqueueParams{Queue: "a"})
	s.Enqueue(ctx, EnqueueParams{Queue: "a"})
	s.Enqueue(ctx, EnqueueParams{Queue: "b"})
	claimed, _ := s.Claim(ctx, "a", 1, "w", time.Minute)

	all, err := s.ListJobs(ctx, ListFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("unfiltered list has %d jobs, want 3", len(all))
	}
	// Most recently updated first: the claimed job was touched last.
	if all[0].ID != claimed[0].ID {
		t.Fatalf("first job is %d, want the just-claimed %d", all[0].ID, claimed[0].ID)
	}

	onlyA, _ := s.ListJobs(ctx, ListFilter{Queue: "a"})
	if len(onlyA) != 2 {
		t.Fatalf("queue a has %d jobs, want 2", len(onlyA))
	}
	running, _ := s.ListJobs(ctx, ListFilter{State: StateRunning})
	if len(running) != 1 || running[0].ID != claimed[0].ID {
		t.Fatalf("running filter returned %v", running)
	}
	limited, _ := s.ListJobs(ctx, ListFilter{Limit: 1})
	if len(limited) != 1 {
		t.Fatalf("limit 1 returned %d", len(limited))
	}
}
