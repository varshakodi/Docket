package store

import (
	"testing"
	"time"
)

func TestQueueStats(t *testing.T) {
	s, ctx := newTestStore(t)

	for i := 0; i < 3; i++ {
		s.Enqueue(ctx, EnqueueParams{Queue: "a"})
	}
	s.Claim(ctx, "a", 1, "w", time.Minute) // a: 2 pending, 1 running
	s.Enqueue(ctx, EnqueueParams{Queue: "b", RunAt: time.Now().Add(time.Hour)})

	depths, ages, err := s.QueueStats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}

	got := map[string]int64{}
	for _, d := range depths {
		got[d.Queue+"/"+string(d.State)] = d.Count
	}
	want := map[string]int64{"a/pending": 2, "a/running": 1, "b/pending": 1}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}

	// Queue a has runnable pending jobs, so it has an age. Queue b's only job
	// is scheduled an hour out -- not "falling behind", so no age reported.
	if len(ages) != 1 || ages[0].Queue != "a" || ages[0].Seconds < 0 {
		t.Fatalf("pending ages = %+v, want exactly queue a with age >= 0", ages)
	}
}
