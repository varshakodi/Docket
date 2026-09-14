// Package api implements the Docket gRPC service on top of the store.
//
// It is a thin translation layer: protobuf messages in, store calls, protobuf
// messages out. No queue logic lives here -- that stays in store and worker
// so the CLI, the Go client and this API all behave identically.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/varshakodi/docket/api/docketv1"
	"github.com/varshakodi/docket/internal/store"
)

// Server implements docketv1.DocketServer.
type Server struct {
	// Embedding this means new RPCs added to the .proto later return
	// "unimplemented" instead of failing to compile -- forward compatibility.
	docketv1.UnimplementedDocketServer
	store *store.Store
}

// New wraps a store as a gRPC service.
func New(s *store.Store) *Server {
	return &Server{store: s}
}

// Serve listens on addr until ctx is cancelled, then stops gracefully:
// in-flight RPCs finish, new connections are refused.
func Serve(ctx context.Context, addr string, s *store.Store, log *slog.Logger) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	docketv1.RegisterDocketServer(srv, New(s))
	// Reflection lets tools like grpcurl discover the service without the
	// .proto file. Costs nothing; very handy for poking at it by hand.
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Info("gRPC API listening", "addr", lis.Addr().String())
	return srv.Serve(lis)
}

func (s *Server) Enqueue(ctx context.Context, req *docketv1.EnqueueRequest) (*docketv1.EnqueueResponse, error) {
	if len(req.GetPayload()) > 0 && !json.Valid(req.GetPayload()) {
		return nil, status.Error(codes.InvalidArgument, "payload must be valid JSON")
	}
	p := store.EnqueueParams{
		Queue:          req.GetQueue(),
		Payload:        req.GetPayload(),
		Priority:       int(req.GetPriority()),
		MaxAttempts:    int(req.GetMaxAttempts()),
		IdempotencyKey: req.GetIdempotencyKey(),
	}
	if req.GetRunAt() != nil {
		p.RunAt = req.GetRunAt().AsTime()
	}

	res, err := s.store.Enqueue(ctx, p)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}
	return &docketv1.EnqueueResponse{JobId: res.ID, Deduplicated: res.Deduplicated}, nil
}

func (s *Server) GetJob(ctx context.Context, req *docketv1.GetJobRequest) (*docketv1.GetJobResponse, error) {
	j, err := s.store.Get(ctx, req.GetJobId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "job %d not found", req.GetJobId())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get job: %v", err)
	}
	return &docketv1.GetJobResponse{Job: toProto(j)}, nil
}

func (s *Server) Stats(ctx context.Context, req *docketv1.StatsRequest) (*docketv1.StatsResponse, error) {
	depths, _, err := s.store.QueueStats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stats: %v", err)
	}
	resp := &docketv1.StatsResponse{}
	for _, d := range depths {
		if req.GetQueue() != "" && d.Queue != req.GetQueue() {
			continue
		}
		resp.Depths = append(resp.Depths, &docketv1.QueueDepth{
			Queue: d.Queue,
			State: stateToProto(d.State),
			Count: d.Count,
		})
	}
	return resp, nil
}

func (s *Server) RequeueDead(ctx context.Context, req *docketv1.RequeueDeadRequest) (*docketv1.RequeueDeadResponse, error) {
	if err := s.store.Requeue(ctx, req.GetJobId()); err != nil {
		// Requeue fails only when the job is not dead (or does not exist).
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &docketv1.RequeueDeadResponse{}, nil
}

func toProto(j store.Job) *docketv1.Job {
	out := &docketv1.Job{
		Id:          j.ID,
		Queue:       j.Queue,
		Payload:     j.Payload,
		State:       stateToProto(j.State),
		Priority:    int32(j.Priority),
		Attempts:    int32(j.Attempts),
		MaxAttempts: int32(j.MaxAttempts),
		RunAt:       timestamppb.New(j.RunAt),
		CreatedAt:   timestamppb.New(j.CreatedAt),
	}
	if j.LeaseExpiresAt != nil {
		out.LeaseExpiresAt = timestamppb.New(*j.LeaseExpiresAt)
	}
	if j.WorkerID != nil {
		out.WorkerId = *j.WorkerID
	}
	if j.LastError != nil {
		out.LastError = *j.LastError
	}
	if j.CompletedAt != nil {
		out.CompletedAt = timestamppb.New(*j.CompletedAt)
	}
	return out
}

func stateToProto(s store.State) docketv1.JobState {
	switch s {
	case store.StatePending:
		return docketv1.JobState_JOB_STATE_PENDING
	case store.StateRunning:
		return docketv1.JobState_JOB_STATE_RUNNING
	case store.StateSucceeded:
		return docketv1.JobState_JOB_STATE_SUCCEEDED
	case store.StateDead:
		return docketv1.JobState_JOB_STATE_DEAD
	default:
		return docketv1.JobState_JOB_STATE_UNSPECIFIED
	}
}
