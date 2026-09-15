// Package dashboard serves a small read-mostly web page showing the state of
// the queue: depths, recent jobs, and the dead-letter queue with a requeue
// button. One HTML file, embedded in the binary; no build step.
package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/varshakodi/docket/internal/store"
)

//go:embed index.html
var static embed.FS

// Handler returns the dashboard's HTTP routes.
func Handler(s *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		page, _ := static.ReadFile("index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})
	mux.HandleFunc("GET /api/stats", handleStats(s))
	mux.HandleFunc("GET /api/jobs", handleJobs(s))
	mux.HandleFunc("POST /api/jobs/{id}/requeue", handleRequeue(s))
	return mux
}

// Serve runs the dashboard on addr until ctx is cancelled.
func Serve(ctx context.Context, addr string, s *store.Store, log *slog.Logger) {
	srv := &http.Server{Addr: addr, Handler: Handler(s), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("dashboard available", "url", "http://"+addr+"/")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("dashboard server stopped", "err", err)
	}
}

// The JSON shapes the page consumes. Kept separate from store.Job so the
// wire format can stay stable if the database row changes.

type statsResponse struct {
	Depths []depth `json:"depths"`
	Ages   []age   `json:"oldest_pending"`
	Now    string  `json:"now"`
}

type depth struct {
	Queue string `json:"queue"`
	State string `json:"state"`
	Count int64  `json:"count"`
}

type age struct {
	Queue   string  `json:"queue"`
	Seconds float64 `json:"seconds"`
}

type job struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	State          string          `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	Priority       int             `json:"priority"`
	Payload        json.RawMessage `json:"payload"`
	RunAt          time.Time       `json:"run_at"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

func toJSON(j store.Job) job {
	return job{
		ID: j.ID, Queue: j.Queue, State: string(j.State),
		Attempts: j.Attempts, MaxAttempts: j.MaxAttempts, Priority: j.Priority,
		Payload: j.Payload, RunAt: j.RunAt, LeaseExpiresAt: j.LeaseExpiresAt,
		WorkerID: j.WorkerID, LastError: j.LastError,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt, CompletedAt: j.CompletedAt,
	}
}

func handleStats(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		depths, ages, err := s.QueueStats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := statsResponse{Depths: []depth{}, Ages: []age{}, Now: time.Now().UTC().Format(time.RFC3339)}
		for _, d := range depths {
			resp.Depths = append(resp.Depths, depth{d.Queue, string(d.State), d.Count})
		}
		for _, a := range ages {
			resp.Ages = append(resp.Ages, age{a.Queue, a.Seconds})
		}
		writeJSON(w, resp)
	}
}

func handleJobs(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		jobs, err := s.ListJobs(r.Context(), store.ListFilter{
			Queue: q.Get("queue"),
			State: store.State(q.Get("state")),
			Limit: limit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]job, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, toJSON(j))
		}
		writeJSON(w, out)
	}
}

func handleRequeue(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid job id", http.StatusBadRequest)
			return
		}
		if err := s.Requeue(r.Context(), id); err != nil {
			// Requeue refuses anything that is not dead.
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
