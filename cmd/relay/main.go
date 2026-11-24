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

	relay := outbox.NewRelay(repository.New(db), queue.NewPublisher(mq), log)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay.Run(ctx)
	}()
	defer func() {
		stop()
		<-relayDone
	}()

	reg := ops.NewRegistry()
	mux := ops.NewMux(log, reg, map[string]health.Check{
		"postgres": db.Ping,
		"rabbitmq": mq.Check,
	})

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
