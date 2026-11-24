package outbox_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestNewMetricsStartsEverySeriesAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	outbox.NewMetrics(reg)

	tests := []struct {
		name   string
		series int
	}{
		{"outbox_published_total", 9}, // 3 channels x 3 priorities
		{"reaper_recovered_total", 6}, // 3 channels x {retry_scheduled, permanently_failed}
		{"outbox_skipped_total", 1},
		{"outbox_dispatch_errors_total", 1},
		{"outbox_dispatch_duration_seconds", 1},
		{"reaper_errors_total", 1},
	}
	for _, tt := range tests {
		n, err := testutil.GatherAndCount(reg, tt.name)
		if err != nil {
			t.Fatalf("GatherAndCount(%s): %v", tt.name, err)
		}
		if n != tt.series {
			t.Errorf("%s has %d series, want %d", tt.name, n, tt.series)
		}
	}
	if v, ok := metricValue(t, reg, "outbox_published_total", "channel", "sms", "priority", "high"); !ok || v != 0 {
		t.Errorf("outbox_published_total{sms,high} = %v (present %v), want 0", v, ok)
	}
}

func TestBacklogCollectorKeepsMetricsServedWhenCountFails(t *testing.T) {
	store := closedStore(t)
	reg := ops.NewRegistry()
	outbox.NewMetrics(reg)
	reg.MustRegister(outbox.NewBacklogCollector(store, discardLog()))
	mux := ops.NewMux(discardLog(), reg, nil)

	for scrape := 1; scrape <= 2; scrape++ {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("scrape %d: GET /metrics = %d, want 200; body %s", scrape, rec.Code, rec.Body)
		}
		body := rec.Body.String()
		if strings.Contains(body, "outbox_backlog{") {
			t.Errorf("scrape %d: outbox_backlog served although counting failed", scrape)
		}
		if !strings.Contains(body, "outbox_dispatch_errors_total 0") {
			t.Errorf("scrape %d: the other relay metrics are missing from the response", scrape)
		}
		if want := fmt.Sprintf("outbox_backlog_query_errors_total %d", scrape); !strings.Contains(body, want) {
			t.Errorf("scrape %d: response does not contain %q", scrape, want)
		}
	}
}

func TestFailedStepsCountAsErrorsExceptOnShutdown(t *testing.T) {
	store := closedStore(t)
	reg := prometheus.NewRegistry()
	metrics := outbox.NewMetrics(reg)
	relay := outbox.NewRelay(store, nil, metrics, discardLog())
	reaper := outbox.NewReaper(store, metrics, discardLog())

	if _, _, err := relay.Once(t.Context()); err == nil {
		t.Fatal("relay.Once on a closed pool succeeded, want an error")
	}
	if _, _, err := reaper.Once(t.Context()); err == nil {
		t.Fatal("reaper.Once on a closed pool succeeded, want an error")
	}

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := relay.Once(cancelled); err == nil {
		t.Fatal("relay.Once with a cancelled context succeeded, want an error")
	}
	if _, _, err := reaper.Once(cancelled); err == nil {
		t.Fatal("reaper.Once with a cancelled context succeeded, want an error")
	}

	for _, name := range []string{"outbox_dispatch_errors_total", "reaper_errors_total"} {
		if v, ok := metricValue(t, reg, name); !ok || v != 1 {
			t.Errorf("%s = %v (present %v), want 1 (the shutdown step is not an error)", name, v, ok)
		}
	}
}

// discardMetrics returns relay and reaper metrics registered with a throwaway registry.
func discardMetrics() *outbox.Metrics { return outbox.NewMetrics(prometheus.NewRegistry()) }

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// closedStore returns a store whose pool is already closed, so every query fails at once.
func closedStore(t *testing.T) *repository.Store {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), "postgres://gogogo@127.0.0.1:1/gogogo")
	if err != nil {
		t.Fatalf("creating pool: %v", err)
	}
	pool.Close()
	return repository.New(pool)
}

// metricValue returns the value of the series name{labels} in g (labels given as name, value pairs):
// the value of a counter or gauge, or the sample count of a histogram. ok is false if there is no
// such series.
func metricValue(t *testing.T, g prometheus.Gatherer, name string, labels ...string) (value float64, ok bool) {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	want := map[string]string{}
	for i := 0; i+1 < len(labels); i += 2 {
		want[labels[i]] = labels[i+1]
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	series:
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(want) {
				continue
			}
			for _, lp := range m.GetLabel() {
				if want[lp.GetName()] != lp.GetValue() {
					continue series
				}
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue(), true
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue(), true
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount()), true
			}
		}
	}
	return 0, false
}
