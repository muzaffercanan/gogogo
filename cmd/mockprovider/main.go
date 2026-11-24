// Command mockprovider runs a fake notification provider with configurable latency and failure rates.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/muzaffercanan/gogogo/internal/platform/config"
	"github.com/muzaffercanan/gogogo/internal/platform/health"
	"github.com/muzaffercanan/gogogo/internal/platform/httpserver"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
	"github.com/muzaffercanan/gogogo/internal/provider/mockserver"
)

func main() {
	if err := run(); err != nil {
		logging.New(os.Stdout, "error").Error("exiting", "service", "mockprovider", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[config.MockProvider]()
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout, cfg.LogLevel).With("service", "mockprovider")

	tp, err := tracing.Setup(ctx, "mock-provider", log)
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: after the server has stopped.
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tracing.ShutdownTimeout)
		defer cancel()
		if err := tp.Shutdown(flushCtx); err != nil {
			log.Warn("flushing traces", "error", err)
		}
	}()

	reg := ops.NewRegistry()
	srv, err := mockserver.New(mockserver.Config{
		Latency:      cfg.Latency,
		Jitter:       cfg.Jitter,
		RejectRate:   cfg.RejectRate,
		ThrottleRate: cfg.ThrottleRate,
		FailureRate:  cfg.FailureRate,
		RetryAfter:   cfg.RetryAfter,
	}, reg)
	if err != nil {
		return err
	}

	mux := ops.NewMux(log, reg, map[string]health.Check{})
	// A server span per request continues the worker's trace, as a real provider's could.
	mux.Handle("POST /", otelhttp.NewHandler(srv, "mock-provider", otelhttp.WithTracerProvider(tp), otelhttp.WithPropagators(tracing.Propagator())))

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
