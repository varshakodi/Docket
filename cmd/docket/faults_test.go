package main

// Fault-injection tests. These build the real docket binary, run it as a
// separate process, and kill it in ways the in-process tests cannot -- a
// SIGKILL gives the worker no chance to run any cleanup code at all.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varshakodi/docket/internal/store"
)

var binPath string

func testDSN() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/docket_test?sslmode=disable"
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "docket-test-bin")
	if err != nil {
		panic(err)
	}
	binPath = filepath.Join(dir, "docket")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic("build docket binary: " + err.Error())
	}
	if err := store.Migrate(testDSN(), "up"); err != nil {
		panic("migrate: " + err.Error())
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// startWorker launches `docket work` as a child process with short timings
// so the tests run in seconds rather than minutes.
func startWorker(t *testing.T, queue string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binPath, "work",
		"--queue", queue,
		"--lease", "1s",
		"--reap-interval", "200ms",
		"--poll", "50ms",
		"--backoff-base", "10ms",
		"--backoff-max", "50ms",
	)
	cmd.Env = append(os.Environ(), "DATABASE_URL="+testDSN())
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

func waitFor(t *testing.T, s *store.Store, id int64, timeout time.Duration, want store.State) store.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if j.State == want {
			return j
		}
		time.Sleep(25 * time.Millisecond)
	}
	j, _ := s.Get(context.Background(), id)
	t.Fatalf("job %d did not reach %s within %v (state=%s attempts=%d)", id, want, timeout, j.State, j.Attempts)
	return j
}

func uniqueQueue(t *testing.T) string {
	return fmt.Sprintf("fault-%s-%d", strings.ToLower(t.Name()), time.Now().UnixNano())
}

// A worker is SIGKILLed while running a job. No cleanup code runs. A second
// worker's reaper must notice the dead lease and the second worker must
// finish the job.
func TestSIGKILLMidJobIsRecoveredByAnotherProcess(t *testing.T) {
	ctx := context.Background()
	s, err := store.New(ctx, testDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	queue := uniqueQueue(t)

	res, err := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, Payload: json.RawMessage(`{"sleep_ms": 1500}`)})
	if err != nil {
		t.Fatal(err)
	}

	victim := startWorker(t, queue)
	waitFor(t, s, res.ID, 5*time.Second, store.StateRunning)

	// The job is mid-flight in the victim. Kill it dead.
	if err := victim.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = victim.Wait()

	// Nothing has touched the job. It is still 'running', owned by a corpse.
	j, _ := s.Get(ctx, res.ID)
	if j.State != store.StateRunning || j.WorkerID == nil {
		t.Fatalf("expected orphaned running job, got state=%s worker=%v", j.State, j.WorkerID)
	}

	startWorker(t, queue) // the rescuer

	j = waitFor(t, s, res.ID, 10*time.Second, store.StateSucceeded)
	if j.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2 (victim's + rescuer's)", j.Attempts)
	}
	if j.LastError == nil || !strings.Contains(*j.LastError, "lease expired") {
		t.Fatalf("expected the reaper to record the expired lease, got %v", j.LastError)
	}
}

// The at-least-once case: a worker finishes the work, then dies before it
// can record success. The queue cannot tell this apart from "never ran", so
// it must redeliver -- and does.
func TestCrashAfterWorkBeforeAckIsRedelivered(t *testing.T) {
	ctx := context.Background()
	s, err := store.New(ctx, testDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	queue := uniqueQueue(t)

	// The demo handler exits the process on the named attempt, after "doing
	// the work" but before Complete is called.
	res, err := s.Enqueue(ctx, store.EnqueueParams{Queue: queue, Payload: json.RawMessage(`{"crash_on_attempt": 1}`)})
	if err != nil {
		t.Fatal(err)
	}

	// Two workers: whichever claims first crashes; the other finishes.
	startWorker(t, queue)
	startWorker(t, queue)

	j := waitFor(t, s, res.ID, 10*time.Second, store.StateSucceeded)
	if j.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2 (crashed once, then redelivered)", j.Attempts)
	}
}
