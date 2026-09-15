package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

func newServer(t *testing.T) (*httptest.Server, *store.Store, string) {
	t.Helper()
	st, err := store.New(context.Background(), testDSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(st.Close)
	srv := httptest.NewServer(Handler(st))
	t.Cleanup(srv.Close)
	return srv, st, fmt.Sprintf("dash-%s-%d", t.Name(), time.Now().UnixNano())
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func TestServesPage(t *testing.T) {
	srv, _, _ := newServer(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	sb.Write(buf[:n])
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "text/html") || !strings.Contains(sb.String(), "DOCKET") {
		t.Fatalf("page: status=%d type=%s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
}

func TestStatsAndJobs(t *testing.T) {
	srv, st, queue := newServer(t)
	ctx := context.Background()

	st.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	st.Enqueue(ctx, store.EnqueueParams{Queue: queue})
	st.Claim(ctx, queue, 1, "w", time.Minute)

	var stats statsResponse
	getJSON(t, srv.URL+"/api/stats", &stats)
	counts := map[string]int64{}
	for _, d := range stats.Depths {
		if d.Queue == queue {
			counts[d.State] = d.Count
		}
	}
	if counts["pending"] != 1 || counts["running"] != 1 {
		t.Fatalf("stats for %s = %v", queue, counts)
	}

	var running []job
	getJSON(t, srv.URL+"/api/jobs?queue="+queue+"&state=running", &running)
	if len(running) != 1 || running[0].WorkerID == nil || *running[0].WorkerID != "w" {
		t.Fatalf("running jobs = %+v", running)
	}
}

func TestRequeue(t *testing.T) {
	srv, st, queue := newServer(t)
	ctx := context.Background()

	res, _ := st.Enqueue(ctx, store.EnqueueParams{Queue: queue, MaxAttempts: 1})
	url := fmt.Sprintf("%s/api/jobs/%d/requeue", srv.URL, res.ID)

	// Pending job: refused.
	resp, _ := http.Post(url, "", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("requeue pending: status %d, want 409", resp.StatusCode)
	}

	jobs, _ := st.Claim(ctx, queue, 1, "w", time.Minute)
	st.Fail(ctx, jobs[0].ID, "w", "boom", 0)

	resp, _ = http.Post(url, "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("requeue dead: status %d, want 204", resp.StatusCode)
	}
	j, _ := st.Get(ctx, res.ID)
	if j.State != store.StatePending || j.Attempts != 0 {
		t.Fatalf("after requeue: %s attempts=%d", j.State, j.Attempts)
	}

	resp, _ = http.Post(srv.URL+"/api/jobs/abc/requeue", "", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad id: status %d, want 400", resp.StatusCode)
	}
}
