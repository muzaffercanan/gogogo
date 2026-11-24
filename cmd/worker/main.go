// Command worker consumes notifications per channel and delivers them through the provider.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/muzaffercanan/gogogo/internal/platform/config"
	"github.com/muzaffercanan/gogogo/internal/platform/health"
	"github.com/muzaffercanan/gogogo/internal/platform/httpserver"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/ops"
	"github.com/muzaffercanan/gogogo/internal/provider"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/ratelimit"
	"github.com/muzaffercanan/gogogo/internal/repository"
	"github.com/muzaffercanan/gogogo/internal/worker"
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

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parsing redis url: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()

	limiter, err := ratelimit.New(rdb, cfg.RateLimit)
	if err != nil {
		return err
	}
	sender, err := provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout)
	if err != nil {
		return err
	}

	// The broker connection outlives the dispatchers: deliveries still in flight at shutdown may
	// publish to the dead-letter queue.
	mqCtx, stopMQ := context.WithCancel(context.Background())
	defer stopMQ()
	mq := queue.NewSupervisor(cfg.AMQPURL, "worker", log)
	mqDone := make(chan struct{})
	go func() {
		defer close(mqDone)
		mq.Run(mqCtx)
	}()
	defer func() {
		stopMQ()
		<-mqDone
	}()

	reg := ops.NewRegistry()
	metrics := worker.NewMetrics(reg)
	reg.MustRegister(queue.NewDepthCollector(mq, log))
	deliverer := worker.NewDeliverer(repository.New(db), sender, queue.NewPublisher(mq), worker.DefaultRetryPolicy(), metrics, log)
	opts := worker.Options{
		Prefetch:    cfg.Prefetch,
		Concurrency: cfg.Concurrency,
		Weights:     worker.Weights{High: cfg.WeightHigh, Normal: cfg.WeightNormal, Low: cfg.WeightLow},
	}
	var channels []*worker.Channel
	for _, c := range queue.Channels() {
		ch, err := worker.NewChannel(c, mq, limiter, deliverer, metrics, opts, log)
		if err != nil {
			return fmt.Errorf("setting up the %s channel: %w", c, err)
		}
		channels = append(channels, ch)
	}
	var running sync.WaitGroup
	for _, ch := range channels {
		running.Go(func() { ch.Run(ctx) })
	}
	defer func() {
		stop() // also when the HTTP server fails to start
		running.Wait()
	}()

	mux := ops.NewMux(log, reg, map[string]health.Check{
		"postgres":  db.Ping,
		"rabbitmq":  mq.Check,
		"redis":     func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		"ratelimit": metrics.CheckLimiter,
	})

	return httpserver.Run(ctx, log, cfg.HTTPAddr, mux, cfg.ShutdownTimeout)
}
