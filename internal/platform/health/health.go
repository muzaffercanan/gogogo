// Package health serves liveness and readiness endpoints.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Check reports whether a dependency is usable. It must respect ctx cancellation.
type Check func(ctx context.Context) error

// Handler serves /health/live and /health/ready.
type Handler struct {
	log     *slog.Logger
	timeout time.Duration
	checks  map[string]Check
}

// New returns a Handler that runs the given readiness checks concurrently, each bounded by timeout.
func New(log *slog.Logger, timeout time.Duration, checks map[string]Check) *Handler {
	return &Handler{log: log, timeout: timeout, checks: checks}
}

// Report is the JSON body returned by the health endpoints.
type Report struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// Live reports that the process is running. It never checks dependencies.
func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, Report{Status: "ok"})
}

// Ready reports whether every dependency check passes. Failure details are logged, not returned.
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(h.checks))
	for name, check := range h.checks {
		go func() { results <- result{name: name, err: check(ctx)} }()
	}

	rep := Report{Status: "ok", Checks: make(map[string]string, len(h.checks))}
	code := http.StatusOK
	for range h.checks {
		res := <-results
		if res.err != nil {
			h.log.WarnContext(r.Context(), "readiness check failed", "check", res.name, "error", res.err)
			rep.Checks[res.name] = "down"
			rep.Status = "unavailable"
			code = http.StatusServiceUnavailable
			continue
		}
		rep.Checks[res.name] = "up"
	}
	writeJSON(w, code, rep)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
