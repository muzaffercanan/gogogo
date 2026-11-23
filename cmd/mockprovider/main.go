// Command mockprovider runs a fake notification provider with configurable latency and failure rates.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/muzaffercanan/gogogo/internal/platform/config"
	"github.com/muzaffercanan/gogogo/internal/platform/health"
	"github.com/muzaffercanan/gogogo/internal/platform/httpserver"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
	"github.com/muzaffercanan/gogogo/internal/provider/mockserver"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mockprovider: %v\n", err)
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
	mux.Handle("POST /", srv)

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
