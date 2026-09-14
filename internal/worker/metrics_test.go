package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/varshakodi/docket/internal/backoff"
	"github.com/varshakodi/docket/internal/metrics"
	"github.com/varshakodi/docket/internal/store"
)

func TestMetricsRecordOutcomes(t *testing.T) {
	s, ctx := newStore(t)
	queue := "w-metrics-" + t.Name() // unique label keeps this test's counts separate

	w := New(s, Config{
		Queue:        queue,
		PollInterval: 10 * time.Millisecond,
		Backoff:      backoff.Config{Base: time.Millisecond, Max: time.Millisecond},
	}, func(_ context.Context, j store.Job) error {
		if string(j.Payload) == `{"fail": true}` {
			return errors.New("asked to fail")
		}
		return nil
	}, quietLog())

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go w.Run(ctx)

	ok1, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	ok2, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	bad, _ := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, Payload: json.RawMessage(`{"fail": true}`), MaxAttempts: 2})

	waitFor(t, s, ok1.ID, 3*time.Second, store.StateSucceeded)
	waitFor(t, s, ok2.ID, 3*time.Second, store.StateSucceeded)
	waitFor(t, s, bad.ID, 3*time.Second, store.StateDead)

	check := func(outcome string, want float64) {
		t.Helper()
		if got := testutil.ToFloat64(metrics.JobsCompleted.WithLabelValues(queue, outcome)); got != want {
			t.Fatalf("jobs_completed{outcome=%q} = %v, want %v", outcome, got, want)
		}
	}
	check("succeeded", 2)
	check("retried", 1) // the bad job's first attempt
	check("dead", 1)    // its second

	if got := testutil.ToFloat64(metrics.InFlight.WithLabelValues(queue)); got != 0 {
		t.Fatalf("in_flight = %v after everything finished, want 0", got)
	}
}
