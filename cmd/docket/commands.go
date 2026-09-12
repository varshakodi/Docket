package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/varshakodi/docket/internal/backoff"
	"github.com/varshakodi/docket/internal/reaper"
	"github.com/varshakodi/docket/internal/store"
	"github.com/varshakodi/docket/internal/worker"
)

func openStore(ctx context.Context) (*store.Store, error) {
	return store.New(ctx, databaseURL())
}

// docket enqueue --queue email --payload '{"to":"a@b.c"}' [--key k] [--delay 30s]
func cmdEnqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	queue := fs.String("queue", "default", "queue name")
	payload := fs.String("payload", "{}", "job payload as JSON")
	priority := fs.Int("priority", 0, "higher runs first")
	key := fs.String("key", "", "idempotency key (dedupes repeat enqueues)")
	delay := fs.Duration("delay", 0, "run no earlier than this far in the future")
	maxAttempts := fs.Int("max-attempts", 5, "give up after this many tries")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !json.Valid([]byte(*payload)) {
		return fmt.Errorf("--payload is not valid JSON")
	}

	ctx := context.Background()
	s, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	p := store.EnqueueParams{
		Queue:          *queue,
		Payload:        json.RawMessage(*payload),
		Priority:       *priority,
		MaxAttempts:    *maxAttempts,
		IdempotencyKey: *key,
	}
	if *delay > 0 {
		p.RunAt = time.Now().Add(*delay)
	}
	res, err := s.Enqueue(ctx, p)
	if err != nil {
		return err
	}
	if res.Deduplicated {
		fmt.Printf("job %d already exists for key %q (deduplicated)\n", res.ID, *key)
	} else {
		fmt.Printf("enqueued job %d on queue %q\n", res.ID, *queue)
	}
	return nil
}

// docket status <id>
func cmdStatus(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: docket status <job-id>")
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid job id %q", args[0])
	}

	ctx := context.Background()
	s, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	printJob(j)
	return nil
}

func printJob(j store.Job) {
	fmt.Printf("job %d\n", j.ID)
	fmt.Printf("  queue:     %s\n", j.Queue)
	fmt.Printf("  state:     %s\n", j.State)
	fmt.Printf("  attempts:  %d / %d\n", j.Attempts, j.MaxAttempts)
	fmt.Printf("  payload:   %s\n", j.Payload)
	fmt.Printf("  run_at:    %s\n", j.RunAt.Format(time.RFC3339))
	if j.WorkerID != nil {
		fmt.Printf("  worker:    %s (lease until %s)\n", *j.WorkerID, j.LeaseExpiresAt.Format(time.RFC3339))
	}
	if j.LastError != nil {
		fmt.Printf("  last err:  %s\n", *j.LastError)
	}
	if j.CompletedAt != nil {
		fmt.Printf("  completed: %s\n", j.CompletedAt.Format(time.RFC3339))
	}
}

// docket work --queue email [--lease 30s] [--reap-interval 5s]
func cmdWork(args []string) error {
	fs := flag.NewFlagSet("work", flag.ContinueOnError)
	queue := fs.String("queue", "default", "queue to pull from")
	lease := fs.Duration("lease", 30*time.Second, "how long a claim lasts without a heartbeat")
	poll := fs.Duration("poll", 500*time.Millisecond, "wait between checks when the queue is empty")
	reapEvery := fs.Duration("reap-interval", 5*time.Second, "how often to sweep for dead workers' jobs")
	boBase := fs.Duration("backoff-base", time.Second, "first retry delay (doubles each failure)")
	boMax := fs.Duration("backoff-max", 5*time.Minute, "longest retry delay")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Ctrl-C cancels ctx; the worker and reaper loops both watch it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	host, _ := os.Hostname()
	bo := backoff.Config{Base: *boBase, Max: *boMax}

	// The reaper runs inside every worker process. If this worker dies, any
	// other worker's reaper recovers its jobs -- there is no single point of
	// failure to keep alive.
	go reaper.Run(ctx, s, *reapEvery, bo, log)

	w := worker.New(s, worker.Config{
		Queue:        *queue,
		WorkerID:     fmt.Sprintf("%s-%d", host, os.Getpid()),
		Lease:        *lease,
		PollInterval: *poll,
		Backoff:      bo,
	}, demoHandler, log)

	return w.Run(ctx)
}

// demoHandler stands in for real handlers until job types are registered.
// It reads two optional payload fields so the queue's behaviour can be
// exercised from the command line:
//
//	{"sleep_ms": 5000}   pretend the job takes 5 seconds
//	{"fail": true}       pretend the job failed
func demoHandler(ctx context.Context, j store.Job) error {
	var p struct {
		SleepMS int  `json:"sleep_ms"`
		Fail    bool `json:"fail"`
	}
	_ = json.Unmarshal(j.Payload, &p) // missing fields just stay zero

	if p.SleepMS > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.Fail {
		return errors.New("payload asked this job to fail")
	}
	fmt.Printf("[job %d] handled payload %s\n", j.ID, j.Payload)
	return nil
}

// docket dlq list [--queue q]     |     docket dlq requeue <id>
func cmdDLQ(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: docket dlq list [--queue NAME] | docket dlq requeue <job-id>")
	}
	ctx := context.Background()
	s, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
		queue := fs.String("queue", "", "only this queue (default: all)")
		limit := fs.Int("limit", 50, "max rows")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		jobs, err := s.ListDead(ctx, *queue, *limit)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			fmt.Println("dead-letter queue is empty")
			return nil
		}
		fmt.Printf("%-6s %-12s %-9s %s\n", "ID", "QUEUE", "ATTEMPTS", "LAST ERROR")
		for _, j := range jobs {
			errText := ""
			if j.LastError != nil {
				errText = *j.LastError
			}
			fmt.Printf("%-6d %-12s %-9d %s\n", j.ID, j.Queue, j.Attempts, errText)
		}
		return nil

	case "requeue":
		if len(args) != 2 {
			return fmt.Errorf("usage: docket dlq requeue <job-id>")
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid job id %q", args[1])
		}
		if err := s.Requeue(ctx, id); err != nil {
			return err
		}
		fmt.Printf("job %d returned to the queue with a fresh attempt budget\n", id)
		return nil

	default:
		return fmt.Errorf("unknown dlq command %q (want list or requeue)", args[0])
	}
}
