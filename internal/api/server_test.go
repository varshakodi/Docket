package api

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/varshakodi/docket/api/docketv1"
	"github.com/varshakodi/docket/internal/store"
)

func testDSN() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/docket_test?sslmode=disable"
}

func TestMain(m *testing.M) {
	if err := store.Migrate(testDSN(), "up"); err != nil {
		panic("migrate: " + err.Error())
	}
	os.Exit(m.Run())
}

// newClient starts the gRPC server on an in-memory listener (no real
// network port) and returns a real client connected to it. Requests go
// through the full gRPC stack -- serialisation, status codes -- exactly as
// they would over TCP.
func newClient(t *testing.T) (docketv1.DocketClient, *store.Store) {
	t.Helper()
	ctx := context.Background()
	st, err := store.New(ctx, testDSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(st.Close)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	docketv1.RegisterDocketServer(srv, New(st))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return docketv1.NewDocketClient(conn), st
}

func uniqueQueue(t *testing.T) string {
	return fmt.Sprintf("api-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestEnqueueAndGetJob(t *testing.T) {
	c, _ := newClient(t)
	ctx := context.Background()
	queue := uniqueQueue(t)

	res, err := c.Enqueue(ctx, &docketv1.EnqueueRequest{Queue: queue, Payload: []byte(`{"to":"a@b.c"}`), Priority: 3})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if res.Deduplicated || res.JobId == 0 {
		t.Fatalf("unexpected response %+v", res)
	}

	got, err := c.GetJob(ctx, &docketv1.GetJobRequest{JobId: res.JobId})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	j := got.Job
	if j.Id != res.JobId || j.Queue != queue || j.State != docketv1.JobState_JOB_STATE_PENDING ||
		j.Priority != 3 || j.Attempts != 0 || j.MaxAttempts != 5 || string(j.Payload) != `{"to": "a@b.c"}` {
		t.Fatalf("unexpected job %+v", j)
	}
	if j.RunAt == nil || j.CreatedAt == nil || j.LeaseExpiresAt != nil || j.CompletedAt != nil {
		t.Fatalf("timestamps wrong on a pending job: %+v", j)
	}
}

func TestEnqueueDeduplicatesByKey(t *testing.T) {
	c, _ := newClient(t)
	ctx := context.Background()
	queue := uniqueQueue(t)

	first, _ := c.Enqueue(ctx, &docketv1.EnqueueRequest{Queue: queue, IdempotencyKey: "k"})
	second, err := c.Enqueue(ctx, &docketv1.EnqueueRequest{Queue: queue, IdempotencyKey: "k"})
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if second.JobId != first.JobId || !second.Deduplicated {
		t.Fatalf("second = %+v, want id %d deduplicated", second, first.JobId)
	}
}

func TestEnqueueRejectsInvalidJSON(t *testing.T) {
	c, _ := newClient(t)
	_, err := c.Enqueue(context.Background(), &docketv1.EnqueueRequest{Queue: uniqueQueue(t), Payload: []byte(`{not json`)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got %v, want InvalidArgument", err)
	}
}

func TestGetJobNotFound(t *testing.T) {
	c, _ := newClient(t)
	_, err := c.GetJob(context.Background(), &docketv1.GetJobRequest{JobId: 999999999})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestStatsFiltersByQueue(t *testing.T) {
	c, _ := newClient(t)
	ctx := context.Background()
	queue := uniqueQueue(t)

	for i := 0; i < 3; i++ {
		c.Enqueue(ctx, &docketv1.EnqueueRequest{Queue: queue})
	}
	c.Enqueue(ctx, &docketv1.EnqueueRequest{Queue: queue + "-other"})

	resp, err := c.Stats(ctx, &docketv1.StatsRequest{Queue: queue})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if len(resp.Depths) != 1 || resp.Depths[0].Queue != queue ||
		resp.Depths[0].State != docketv1.JobState_JOB_STATE_PENDING || resp.Depths[0].Count != 3 {
		t.Fatalf("stats = %+v, want one row: %s pending 3", resp.Depths, queue)
	}
}

func TestRequeueDead(t *testing.T) {
	c, st := newClient(t)
	ctx := context.Background()
	queue := uniqueQueue(t)

	res, _ := c.Enqueue(ctx, &docketv1.EnqueueRequest{Queue: queue, MaxAttempts: 1})

	// Not dead yet: refused.
	_, err := c.RequeueDead(ctx, &docketv1.RequeueDeadRequest{JobId: res.JobId})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("requeue of a pending job: got %v, want FailedPrecondition", err)
	}

	// Kill it through the store, then requeue through the API.
	jobs, _ := st.Claim(ctx, queue, 1, "w", time.Minute)
	st.Fail(ctx, jobs[0].ID, "w", "boom", 0)
	if _, err := c.RequeueDead(ctx, &docketv1.RequeueDeadRequest{JobId: res.JobId}); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	got, _ := c.GetJob(ctx, &docketv1.GetJobRequest{JobId: res.JobId})
	if got.Job.State != docketv1.JobState_JOB_STATE_PENDING || got.Job.Attempts != 0 {
		t.Fatalf("after requeue: %+v", got.Job)
	}
}
