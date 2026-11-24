// Command api serves the notification HTTP API.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/api"
	"github.com/muzaffercanan/gogogo/internal/platform/config"
	"github.com/muzaffercanan/gogogo/internal/platform/health"
	"github.com/muzaffercanan/gogogo/internal/platform/httpserver"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
	"github.com/muzaffercanan/gogogo/internal/repository"
	"github.com/muzaffercanan/gogogo/internal/service"
)

func main() {
	if err := run(); err != nil {
		logging.New(os.Stdout, "error").Error("exiting", "service", "api", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[config.API]()
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout, cfg.LogLevel).With("service", "api")

	tp, err := tracing.Setup(ctx, "api", log)
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: after everything that makes spans has stopped.
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tracing.ShutdownTimeout)
		defer cancel()
		if err := tp.Shutdown(flushCtx); err != nil {
			log.Warn("flushing traces", "error", err)
		}
	}()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("creating postgres pool: %w", err)
	}
	defer db.Close()

	svc, err := service.New(repository.New(db), cfg.MaxAttempts, time.Now, tp)
	if err != nil {
		return err
	}

	reg := ops.NewRegistry()
	mux := ops.NewMux(log, reg, map[string]health.Check{"postgres": db.Ping})
	mux.Handle("/", api.NewHandler(svc, log, reg, tp))

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
