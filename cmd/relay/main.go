// Command relay moves due outbox rows to the message broker and recovers stuck deliveries.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/platform/config"
	"github.com/muzaffercanan/gogogo/internal/platform/health"
	"github.com/muzaffercanan/gogogo/internal/platform/httpserver"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func main() {
	if err := run(); err != nil {
		logging.New(os.Stdout, "error").Error("exiting", "service", "relay", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[config.Relay]()
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout, cfg.LogLevel).With("service", "relay")

	tp, err := tracing.Setup(ctx, "relay", log)
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

	mq := queue.NewSupervisor(cfg.AMQPURL, "relay", log)
	mqDone := make(chan struct{})
	go func() {
		defer close(mqDone)
		mq.Run(ctx)
	}()
	defer func() {
		stop() // also when the HTTP server fails to start
		<-mqDone
	}()

	store := repository.New(db)

	reg := ops.NewRegistry()
	metrics := outbox.NewMetrics(reg)
	reg.MustRegister(outbox.NewBacklogCollector(store, log))

	relay := outbox.NewRelay(store, queue.NewPublisher(mq), metrics, log)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay.Run(ctx)
	}()
	defer func() {
		stop()
		<-relayDone
	}()

	reaper := outbox.NewReaper(store, metrics, log)
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		reaper.Run(ctx)
	}()
	defer func() {
		stop()
		<-reaperDone
	}()

	mux := ops.NewMux(log, reg, map[string]health.Check{
		"postgres": db.Ping,
		"rabbitmq": mq.Check,
	})

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
