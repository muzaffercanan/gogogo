// Package ops wires the operational endpoints every process exposes: health and metrics.
package ops

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/muzaffercanan/gogogo/internal/platform/health"
)

// readinessTimeout bounds how long all readiness checks may take together.
const readinessTimeout = 2 * time.Second

// NewRegistry returns a process-local Prometheus registry with Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// NewMux returns a mux serving /health/live, /health/ready and /metrics.
// Callers may register additional routes on the returned mux.
func NewMux(log *slog.Logger, reg *prometheus.Registry, checks map[string]health.Check) *http.ServeMux {
	h := health.New(log, readinessTimeout, checks)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", h.Live)
	mux.HandleFunc("GET /health/ready", h.Ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return mux
}
