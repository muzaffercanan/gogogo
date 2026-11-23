package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/muzaffercanan/gogogo/internal/api"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
)

func TestCorrelationID(t *testing.T) {
	tests := []struct {
		name   string
		header string
		keep   bool
	}{
		{"UUID", "8b7a6c5d-4e3f-4a1b-9c8d-7e6f5a4b3c2d", true},
		{"trace-style ID", "00-4bf92f3577b34da6.a3ce929d_0e0e4736:01", true},
		{"128 characters", strings.Repeat("a", 128), true},
		{"missing", "", false},
		{"129 characters", strings.Repeat("a", 129), false},
		{"space", "abc def", false},
		{"slash", "abc/def", false},
		{"non-ASCII", "şifre", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var inContext string
			h := api.CorrelationID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				inContext = logging.CorrelationID(r.Context())
			}))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			if tt.header != "" {
				req.Header.Set(api.CorrelationHeader, tt.header)
			}
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			got := rec.Header().Get(api.CorrelationHeader)
			if got != inContext {
				t.Errorf("response header %q differs from the context's %q", got, inContext)
			}
			if tt.keep && got != tt.header {
				t.Errorf("correlation ID = %q, want the client's %q", got, tt.header)
			}
			if !tt.keep {
				if _, err := uuid.Parse(got); err != nil || got == tt.header {
					t.Errorf("correlation ID = %q, want a generated UUID instead of %q", got, tt.header)
				}
			}
		})
	}
}

func TestMaxBytes(t *testing.T) {
	var readErr error
	h := api.MaxBytes(func(*http.Request) int64 { return 8 })(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
	}))
	tests := []struct {
		body    string
		wantErr bool
	}{
		{"12345678", false},
		{"123456789", true},
	}
	for _, tt := range tests {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(tt.body)))
		var tooLarge *http.MaxBytesError
		if got := errors.As(readErr, &tooLarge); got != tt.wantErr {
			t.Errorf("body %q: read error = %v, want a *http.MaxBytesError: %v", tt.body, readErr, tt.wantErr)
		}
	}
}

func TestRecover(t *testing.T) {
	t.Run("before the response started", func(t *testing.T) {
		var logs bytes.Buffer
		h := api.Recover(logging.New(&logs, "info"))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("boom")
		}))
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/notifications", nil))

		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if rec.Code != http.StatusInternalServerError || rec.Header().Get("Content-Type") != "application/problem+json" ||
			body["type"] != "/problems/internal-error" {
			t.Errorf("response = %d %s %s, want a 500 internal-error problem", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
		}
		if !strings.Contains(logs.String(), `"panic":"boom"`) || !strings.Contains(logs.String(), "middleware_test.go") {
			t.Errorf("log %s does not hold the panic value and its stack", logs.String())
		}
	})

	t.Run("after the response started", func(t *testing.T) {
		h := api.Recover(logging.New(io.Discard, "info"))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("partial"))
			panic("boom")
		}))
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

		if rec.Code != http.StatusAccepted || rec.Body.String() != "partial" {
			t.Errorf("response = %d %q, want the handler's 202 \"partial\" untouched", rec.Code, rec.Body)
		}
	})

	t.Run("abort is re-raised", func(t *testing.T) {
		h := api.Recover(logging.New(io.Discard, "info"))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic(http.ErrAbortHandler)
		}))
		defer func() {
			if v := recover(); v != http.ErrAbortHandler { //nolint:errorlint // the panic value itself is compared
				t.Errorf("recovered %v, want http.ErrAbortHandler re-raised", v)
			}
		}()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	})
}

// chain builds the order the API uses: CorrelationID outside the middleware that read r.Pattern.
func chain(mux http.Handler, logs io.Writer, reg prometheus.Registerer) http.Handler {
	logger := logging.New(logs, "info")
	return api.CorrelationID(api.AccessLog(logger)(api.NewMetrics(reg).Middleware(api.Recover(logger)(mux))))
}

func testMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/notifications/{id}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /v1/notifications", func(http.ResponseWriter, *http.Request) { panic("boom") })
	return mux
}

func TestMetricsUseTheMatchedRoute(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := chain(testMux(), io.Discard, reg)
	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/v1/notifications/a"},
		{http.MethodGet, "/v1/notifications/b"},
		{http.MethodPost, "/v1/notifications"},
		{http.MethodGet, "/nowhere"},
	} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), req.method, req.path, nil))
	}

	want := `
# HELP http_requests_total HTTP requests answered, by method, route and status code.
# TYPE http_requests_total counter
http_requests_total{method="GET",route="GET /v1/notifications/{id}",status="200"} 2
http_requests_total{method="GET",route="unmatched",status="404"} 1
http_requests_total{method="POST",route="POST /v1/notifications",status="500"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "http_requests_total"); err != nil {
		t.Error(err)
	}
	if n := seriesCount(t, reg, "http_request_duration_seconds"); n != 3 {
		t.Errorf("%d duration series, want 3 (one per method and route)", n)
	}
}

func TestAccessLog(t *testing.T) {
	var logs bytes.Buffer
	h := chain(testMux(), &logs, prometheus.NewRegistry())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/notifications/a", nil)
	req.Header.Set(api.CorrelationHeader, "corr-1")

	h.ServeHTTP(httptest.NewRecorder(), req)

	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("decoding log %s: %v", logs.String(), err)
	}
	for key, want := range map[string]any{
		"msg": "request", "method": "GET", "route": "GET /v1/notifications/{id}", "path": "/v1/notifications/a",
		"status": float64(200), "bytes": float64(2), "correlation_id": "corr-1",
	} {
		if entry[key] != want {
			t.Errorf("log %s = %v, want %v", key, entry[key], want)
		}
	}
	if _, ok := entry["duration_ms"].(float64); !ok {
		t.Errorf("log has no numeric duration_ms: %v", entry)
	}
}

// seriesCount returns how many series the metric family name has in reg.
func seriesCount(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return len(f.GetMetric())
		}
	}
	return 0
}
