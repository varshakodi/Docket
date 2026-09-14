package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/varshakodi/docket/internal/store"
)

func TestHandlerServesMetricsPage(t *testing.T) {
	LeasesReaped.Add(0) // make sure it exists even if nothing was reaped

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Result().Body)

	for _, want := range []string{"docket_leases_reaped_total", "docket_claim_latency_seconds"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("metrics page is missing %s", want)
		}
	}
}

func TestRecordQueueStatsReplacesSnapshot(t *testing.T) {
	RecordQueueStats(
		[]store.QueueStat{{Queue: "q", State: store.StatePending, Count: 7}, {Queue: "q", State: store.StateRunning, Count: 2}},
		[]store.PendingAge{{Queue: "q", Seconds: 12.5}},
	)
	if v := testutil.ToFloat64(QueueDepth.WithLabelValues("q", "pending")); v != 7 {
		t.Fatalf("pending depth = %v, want 7", v)
	}
	if v := testutil.ToFloat64(OldestPendingAge.WithLabelValues("q")); v != 12.5 {
		t.Fatalf("oldest pending age = %v, want 12.5", v)
	}

	// A later snapshot with the running jobs gone must not leave a stale 2.
	RecordQueueStats([]store.QueueStat{{Queue: "q", State: store.StatePending, Count: 1}}, nil)
	if n := testutil.CollectAndCount(QueueDepth); n != 1 {
		t.Fatalf("queue depth has %d series after reset, want 1", n)
	}
	if n := testutil.CollectAndCount(OldestPendingAge); n != 0 {
		t.Fatalf("oldest pending age has %d series after reset, want 0", n)
	}
}
