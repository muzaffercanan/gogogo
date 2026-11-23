package mockserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const validBody = `{"to":"+905551234567","channel":"sms","content":"hello"}`

func newTestServer(t *testing.T, cfg Config, roll float64) (*Server, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	fixed := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	s, err := New(cfg, reg, WithRoll(func() float64 { return roll }), WithClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, reg
}

func TestOutcomes(t *testing.T) {
	cfg := Config{RejectRate: 0.1, ThrottleRate: 0.2, FailureRate: 0.3, RetryAfter: 2 * time.Second}
	tests := []struct {
		name           string
		roll           float64
		wantCode       int
		wantResult     string
		wantRetryAfter string
	}{
		{name: "reject", roll: 0.05, wantCode: http.StatusBadRequest, wantResult: "rejected"},
		{name: "throttle", roll: 0.15, wantCode: http.StatusTooManyRequests, wantResult: "throttled", wantRetryAfter: "2"},
		{name: "failure", roll: 0.45, wantCode: http.StatusInternalServerError, wantResult: "failed"},
		{name: "accept", roll: 0.95, wantCode: http.StatusAccepted, wantResult: "accepted"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestServer(t, cfg, tt.roll)
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/any-path", strings.NewReader(validBody)))

			if rec.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tt.wantCode)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.wantRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tt.wantRetryAfter)
			}
			if got := testutil.ToFloat64(s.requests.WithLabelValues("sms", tt.wantResult)); got != 1 {
				t.Errorf("counter{sms,%s} = %v, want 1", tt.wantResult, got)
			}
		})
	}
}

func TestAcceptedResponseFollowsProviderContract(t *testing.T) {
	s, _ := newTestServer(t, Config{}, 0.5)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(validBody)))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202", rec.Code)
	}
	var got sendResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if got.MessageID == "" || got.Status != "accepted" {
		t.Errorf("unexpected body: %+v", got)
	}
	if _, err := time.Parse(time.RFC3339Nano, got.Timestamp); err != nil {
		t.Errorf("timestamp %q is not RFC 3339: %v", got.Timestamp, err)
	}
}

func TestInvalidRequests(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
	}{
		{name: "wrong method", method: http.MethodGet, body: "", wantCode: http.StatusMethodNotAllowed},
		{name: "malformed json", method: http.MethodPost, body: "{", wantCode: http.StatusBadRequest},
		{name: "missing recipient", method: http.MethodPost, body: `{"channel":"sms","content":"x"}`, wantCode: http.StatusBadRequest},
		{name: "unknown channel", method: http.MethodPost, body: `{"to":"a","channel":"fax","content":"x"}`, wantCode: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestServer(t, Config{}, 0.5)
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), tt.method, "/", strings.NewReader(tt.body)))
			if rec.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tt.wantCode)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "defaults", cfg: Config{}, wantErr: false},
		{name: "rates sum to one", cfg: Config{RejectRate: 0.5, FailureRate: 0.5}, wantErr: false},
		{name: "rates above one", cfg: Config{RejectRate: 0.6, FailureRate: 0.6}, wantErr: true},
		{name: "negative rate", cfg: Config{ThrottleRate: -0.1}, wantErr: true},
		{name: "negative latency", cfg: Config{Latency: -time.Second}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, prometheus.NewRegistry())
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
