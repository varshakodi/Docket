// Package bench measures how the queue's throughput and claim latency change
// as workers are added -- and how they change without SKIP LOCKED.
package bench

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varshakodi/docket/internal/store"
)

// Mode selects which claim query the workers use.
type Mode string

const (
	ModeSkipLocked Mode = "skip-locked" // the real thing
	ModeNaive      Mode = "naive"       // FOR UPDATE without SKIP LOCKED
)

// Options configures a benchmark run.
type Options struct {
	Queue   string
	Jobs    int           // jobs per run
	Batch   int           // jobs claimed per round trip
	Workers []int         // worker counts to try, e.g. 1,2,4,8,16
	Modes   []Mode        // which claim strategies to compare
	Sleep   time.Duration // simulated work per job; 0 = pure queue overhead
}

// Result is the measurement for one (mode, workers) combination.
type Result struct {
	Mode       Mode
	Workers    int
	Jobs       int
	Elapsed    time.Duration
	Throughput float64 // jobs per second
	Claims     int     // claim round trips made
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
}

type claimFunc func(ctx context.Context, queue string, limit int, workerID string, lease time.Duration) ([]store.Job, error)

// Run executes every (mode, workers) combination and calls report after each.
func Run(ctx context.Context, s *store.Store, opt Options, report func(Result)) ([]Result, error) {
	if opt.Queue == "" {
		opt.Queue = "bench"
	}
	if opt.Batch <= 0 {
		opt.Batch = 10
	}
	var results []Result
	for _, mode := range opt.Modes {
		for _, w := range opt.Workers {
			r, err := runOne(ctx, s, opt, mode, w)
			if err != nil {
				return results, fmt.Errorf("%s with %d workers: %w", mode, w, err)
			}
			results = append(results, r)
			if report != nil {
				report(r)
			}
		}
	}
	return results, nil
}

func runOne(ctx context.Context, s *store.Store, opt Options, mode Mode, workers int) (Result, error) {
	claim := s.Claim
	if mode == ModeNaive {
		claim = s.ClaimWithoutSkipLocked
	}

	if err := s.DeleteQueue(ctx, opt.Queue); err != nil {
		return Result{}, err
	}
	if err := s.SeedJobs(ctx, opt.Queue, opt.Jobs); err != nil {
		return Result{}, err
	}

	var (
		completed atomic.Int64
		mu        sync.Mutex
		latencies []time.Duration
		wg        sync.WaitGroup
		errCh     = make(chan error, workers)
	)

	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			var local []time.Duration
			for {
				t0 := time.Now()
				jobs, err := claim(ctx, opt.Queue, opt.Batch, workerID, time.Minute)
				local = append(local, time.Since(t0))
				if err != nil {
					errCh <- err
					return
				}
				if len(jobs) == 0 {
					// Are we actually done, or did a naive claim come back
					// empty because another worker consumed the rows it had
					// chosen while it sat blocked? Only stop when every job
					// is accounted for.
					if completed.Load() >= int64(opt.Jobs) {
						break
					}
					time.Sleep(time.Millisecond)
					continue
				}
				for _, j := range jobs {
					if opt.Sleep > 0 {
						time.Sleep(opt.Sleep)
					}
					if err := s.Complete(ctx, j.ID, workerID); err != nil {
						errCh <- err
						return
					}
					completed.Add(1)
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(fmt.Sprintf("bench-%s-%d", mode, w))
	}
	wg.Wait()
	elapsed := time.Since(start)

	close(errCh)
	for err := range errCh {
		return Result{}, err
	}
	if got := completed.Load(); got != int64(opt.Jobs) {
		return Result{}, fmt.Errorf("completed %d of %d jobs", got, opt.Jobs)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return Result{
		Mode:       mode,
		Workers:    workers,
		Jobs:       opt.Jobs,
		Elapsed:    elapsed,
		Throughput: float64(opt.Jobs) / elapsed.Seconds(),
		Claims:     len(latencies),
		P50:        percentile(latencies, 0.50),
		P95:        percentile(latencies, 0.95),
		P99:        percentile(latencies, 0.99),
	}, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

// PrintTable writes results as a table. The last column is scaling
// efficiency: throughput relative to the 1-worker run of the same mode,
// divided by the worker count. 100% means perfectly linear.
func PrintTable(w io.Writer, results []Result) {
	base := map[Mode]float64{}
	for _, r := range results {
		if r.Workers == 1 {
			base[r.Mode] = r.Throughput
		}
	}
	fmt.Fprintf(w, "%-12s %8s %10s %10s %10s %10s %9s\n",
		"MODE", "WORKERS", "JOBS/SEC", "CLAIM p50", "p95", "p99", "SCALING")
	for _, r := range results {
		scaling := "-"
		if b, ok := base[r.Mode]; ok && b > 0 && r.Workers > 0 {
			scaling = fmt.Sprintf("%.0f%%", 100*r.Throughput/(b*float64(r.Workers)))
		}
		fmt.Fprintf(w, "%-12s %8d %10.0f %10s %10s %10s %9s\n",
			r.Mode, r.Workers, r.Throughput, ms(r.P50), ms(r.P95), ms(r.P99), scaling)
	}
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
}
