package main

// The CLI normally talks to the database directly. With --server it talks to
// a docket-server over gRPC instead, which is how a machine without database
// credentials -- or a non-Go service -- would use the queue.

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/varshakodi/docket/api/docketv1"
)

func dial(addr string) (docketv1.DocketClient, *grpc.ClientConn, error) {
	// Insecure = no TLS. Fine inside a private network; put TLS termination
	// in front of it for anything else.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return docketv1.NewDocketClient(conn), conn, nil
}

func remoteEnqueue(addr, queue, payload string, priority, maxAttempts int, key string, delay time.Duration) error {
	c, conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &docketv1.EnqueueRequest{
		Queue:          queue,
		Payload:        []byte(payload),
		Priority:       int32(priority),
		MaxAttempts:    int32(maxAttempts),
		IdempotencyKey: key,
	}
	if delay > 0 {
		req.RunAt = timestamppb.New(time.Now().Add(delay))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := c.Enqueue(ctx, req)
	if err != nil {
		return err
	}
	if res.Deduplicated {
		fmt.Printf("job %d already exists for key %q (deduplicated)\n", res.JobId, key)
	} else {
		fmt.Printf("enqueued job %d on queue %q via %s\n", res.JobId, queue, addr)
	}
	return nil
}

func remoteStatus(addr string, id int64) error {
	c, conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := c.GetJob(ctx, &docketv1.GetJobRequest{JobId: id})
	if err != nil {
		return err
	}
	j := res.Job
	state := map[docketv1.JobState]string{
		docketv1.JobState_JOB_STATE_PENDING:   "pending",
		docketv1.JobState_JOB_STATE_RUNNING:   "running",
		docketv1.JobState_JOB_STATE_SUCCEEDED: "succeeded",
		docketv1.JobState_JOB_STATE_DEAD:      "dead",
	}[j.State]

	fmt.Printf("job %d\n", j.Id)
	fmt.Printf("  queue:     %s\n", j.Queue)
	fmt.Printf("  state:     %s\n", state)
	fmt.Printf("  attempts:  %d / %d\n", j.Attempts, j.MaxAttempts)
	fmt.Printf("  payload:   %s\n", j.Payload)
	fmt.Printf("  run_at:    %s\n", j.RunAt.AsTime().Local().Format(time.RFC3339))
	if j.WorkerId != "" {
		fmt.Printf("  worker:    %s (lease until %s)\n", j.WorkerId, j.LeaseExpiresAt.AsTime().Local().Format(time.RFC3339))
	}
	if j.LastError != "" {
		fmt.Printf("  last err:  %s\n", j.LastError)
	}
	if j.CompletedAt != nil {
		fmt.Printf("  completed: %s\n", j.CompletedAt.AsTime().Local().Format(time.RFC3339))
	}
	return nil
}
