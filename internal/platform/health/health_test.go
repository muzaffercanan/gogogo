package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ok(context.Context) error   { return nil }
func fail(context.Context) error { return errors.New("connection refused") }

func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestReady(t *testing.T) {
	tests := []struct {
		name       string
		checks     map[string]Check
		wantCode   int
		wantStatus string
		wantChecks map[string]string
	}{
		{
			name:       "all dependencies up",
			checks:     map[string]Check{"postgres": ok, "redis": ok},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{"postgres": "up", "redis": "up"},
		},
		{
			name:       "one dependency down",
			checks:     map[string]Check{"postgres": ok, "redis": fail},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "unavailable",
			wantChecks: map[string]string{"postgres": "up", "redis": "down"},
		},
		{
			name:       "slow dependency times out",
			checks:     map[string]Check{"rabbitmq": blockUntilDone},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "unavailable",
			wantChecks: map[string]string{"rabbitmq": "down"},
		},
		{
			name:       "no dependencies",
			checks:     map[string]Check{},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(slog.New(slog.NewTextHandler(io.Discard, nil)), 50*time.Millisecond, tt.checks)
			rec := httptest.NewRecorder()
			h.Ready(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/ready", nil))

			if rec.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tt.wantCode)
			}
			var got Report
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatalf("decoding body: %v", err)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", got.Status, tt.wantStatus)
			}
			if len(got.Checks) != len(tt.wantChecks) {
				t.Fatalf("checks = %v, want %v", got.Checks, tt.wantChecks)
			}
			for name, want := range tt.wantChecks {
				if got.Checks[name] != want {
					t.Errorf("check %q = %q, want %q", name, got.Checks[name], want)
				}
			}
		})
	}
}

func TestLiveIgnoresDependencies(t *testing.T) {
	h := New(slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second, map[string]Check{"postgres": fail})
	rec := httptest.NewRecorder()
	h.Live(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health/live", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want %d", rec.Code, http.StatusOK)
	}
}
