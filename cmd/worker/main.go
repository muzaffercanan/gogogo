// Command worker consumes notifications per channel and delivers them through the provider.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/muzaffercanan/gogogo/internal/platform/config"
	"github.com/muzaffercanan/gogogo/internal/platform/health"
	"github.com/muzaffercanan/gogogo/internal/platform/httpserver"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
)

func main() {
	if err := run(); err != nil {
		logging.New(os.Stdout, "error").Error("exiting", "service", "worker", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load[config.Worker]()
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout, cfg.LogLevel).With("service", "worker")

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("creating postgres pool: %w", err)
	}
	defer db.Close()

	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		return fmt.Errorf("connecting to rabbitmq: %w", err)
	}
	defer func() { _ = conn.Close() }()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parsing redis url: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()

	reg := ops.NewRegistry()
	mux := ops.NewMux(log, reg, map[string]health.Check{
		"postgres": db.Ping,
		"rabbitmq": func(context.Context) error {
			if conn.IsClosed() {
				return errors.New("connection closed")
			}
			return nil
		},
		"redis": func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
	})

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
