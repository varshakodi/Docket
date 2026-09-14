// Package metrics publishes what the queue is doing in Prometheus format.
//
// Prometheus is the standard monitoring tool for services like this: it
// fetches ("scrapes") a plain-text page of numbers from each process every
// few seconds and stores them as time series, so you can graph and alert on
// them. This package defines the numbers and serves the page.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/varshakodi/docket/internal/store"
)

// Two kinds of metric are used here.
//
// A counter only ever goes up (jobs completed so far). Prometheus turns it
// into a rate: "jobs per second over the last minute".
//
// A gauge is a value that goes up and down (jobs currently waiting). It is
// read as-is.
//
// A histogram counts observations into buckets (how many claims took under
// 1 ms, under 2 ms, ...) so percentiles like p99 can be computed later.

var (
	// JobsCompleted counts every job this worker finished handling, labelled
	// by how it ended: succeeded, retried, dead, released, abandoned, or
	// unrecorded (the outcome could not be written to the database).
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "docket_jobs_completed_total",
		Help: "Jobs this worker finished handling, by outcome.",
	}, []string{"queue", "outcome"})

	// JobDuration is how long handlers take, from claim to outcome.
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "docket_job_duration_seconds",
		Help:    "Time spent running a job's handler.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16), // 1ms .. ~32s
	}, []string{"queue"})

	// ClaimLatency is the round-trip time of the claim query. If this climbs,
	// workers are contending or the database is struggling.
	ClaimLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "docket_claim_latency_seconds",
		Help:    "Round-trip time of the claim query.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1ms .. ~0.8s
	})

	// InFlight is how many jobs this worker is running right now.
	InFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "docket_jobs_in_flight",
		Help: "Jobs currently being handled by this worker.",
	}, []string{"queue"})

	// LeasesReaped counts jobs recovered from workers that stopped
	// responding. A steady non-zero rate means workers are dying.
	LeasesReaped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "docket_leases_reaped_total",
		Help: "Jobs recovered from expired leases (worker presumed dead).",
	})

	// QueueDepth is the backlog: how many jobs are in each state per queue.
	// This is the number to alert on.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "docket_queue_depth",
		Help: "Number of jobs per queue and state.",
	}, []string{"queue", "state"})

	// OldestPendingAge answers "are we falling behind?": how long the oldest
	// runnable job has been waiting. Depth alone can't tell you that -- a
	// deep queue draining fast is fine; a shallow one where the head is an
	// hour old is not.
	OldestPendingAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "docket_oldest_pending_age_seconds",
		Help: "Age of the oldest job that is ready to run but not yet claimed.",
	}, []string{"queue"})
)

// Handler serves the metrics page.
func Handler() http.Handler {
	return promhttp.Handler()
}

// Serve exposes /metrics on addr until ctx is cancelled.
func Serve(ctx context.Context, addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("metrics available", "url", "http://"+addr+"/metrics")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server stopped", "err", err)
	}
}

// StatsSource is anything that can report queue depths -- in practice, the
// store. Declared as an interface so this package can be tested without a
// database.
type StatsSource interface {
	QueueStats(ctx context.Context) ([]store.QueueStat, []store.PendingAge, error)
}

// PollQueueStats refreshes the queue-depth gauges every interval until ctx is
// cancelled. The worker-side metrics above update themselves as jobs run;
// these two need a query, because the backlog lives in the database, not in
// any one worker's memory.
func PollQueueStats(ctx context.Context, src StatsSource, interval time.Duration, log *slog.Logger) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		depths, ages, err := src.QueueStats(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("could not read queue stats", "err", err)
			}
			continue
		}
		RecordQueueStats(depths, ages)
	}
}

// RecordQueueStats replaces the queue-depth gauges with a fresh snapshot.
//
// The vectors are reset first so that a (queue, state) pair that has emptied
// out disappears rather than reporting its last non-zero value forever.
func RecordQueueStats(depths []store.QueueStat, ages []store.PendingAge) {
	QueueDepth.Reset()
	for _, d := range depths {
		QueueDepth.WithLabelValues(d.Queue, string(d.State)).Set(float64(d.Count))
	}
	OldestPendingAge.Reset()
	for _, a := range ages {
		OldestPendingAge.WithLabelValues(a.Queue).Set(a.Seconds)
	}
}
