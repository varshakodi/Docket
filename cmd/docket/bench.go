package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/varshakodi/docket/internal/bench"
	"github.com/varshakodi/docket/internal/store"
)

// docket bench [--jobs 5000] [--workers 1,2,4,8,16] [--batch 10] [--mode both]
func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	jobs := fs.Int("jobs", 5000, "jobs per run")
	workersFlag := fs.String("workers", "1,2,4,8,16", "comma-separated worker counts to try")
	batch := fs.Int("batch", 10, "jobs claimed per round trip")
	sleep := fs.Duration("sleep", 0, "simulated work per job (0 = measure pure queue overhead)")
	mode := fs.String("mode", "both", "skip-locked, naive, or both")
	queue := fs.String("queue", "bench", "queue to use (it is emptied before each run)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var workers []int
	maxWorkers := 0
	for _, part := range strings.Split(*workersFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid --workers value %q", part)
		}
		workers = append(workers, n)
		if n > maxWorkers {
			maxWorkers = n
		}
	}

	var modes []bench.Mode
	switch *mode {
	case "both":
		modes = []bench.Mode{bench.ModeSkipLocked, bench.ModeNaive}
	case "skip-locked":
		modes = []bench.Mode{bench.ModeSkipLocked}
	case "naive":
		modes = []bench.Mode{bench.ModeNaive}
	default:
		return fmt.Errorf("--mode must be skip-locked, naive or both")
	}

	// Every worker needs its own connection at the same time, so size the
	// pool to fit them all. Otherwise workers queue for a connection and we
	// would be measuring the pool, not the database.
	dsn := databaseURL()
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	dsn += fmt.Sprintf("%spool_max_conns=%d", sep, maxWorkers+4)

	ctx := context.Background()
	s, err := store.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer s.Close()

	fmt.Printf("docket bench: %d jobs per run, batch %d, sleep %v, modes %v, workers %v\n\n",
		*jobs, *batch, *sleep, modes, workers)

	results, err := bench.Run(ctx, s, bench.Options{
		Queue:   *queue,
		Jobs:    *jobs,
		Batch:   *batch,
		Workers: workers,
		Modes:   modes,
		Sleep:   *sleep,
	}, func(r bench.Result) {
		fmt.Printf("  %-12s %2d workers  %7.0f jobs/sec  in %v\n",
			r.Mode, r.Workers, r.Throughput, r.Elapsed.Round(time.Millisecond))
	})
	if err != nil {
		return err
	}

	fmt.Println()
	bench.PrintTable(os.Stdout, results)
	return nil
}
