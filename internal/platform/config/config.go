// Package config loads process configuration from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Common holds settings shared by every process.
type Common struct {
	LogLevel        string        `env:"LOG_LEVEL"        envDefault:"info"`
	HTTPAddr        string        `env:"HTTP_ADDR"        envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
}

// API configures the HTTP API process.
type API struct {
	Common
	DatabaseURL string `env:"DATABASE_URL,required"`
	// MaxAttempts is the number of delivery attempts a new notification gets before it is marked
	// permanently_failed.
	MaxAttempts int `env:"MAX_ATTEMPTS" envDefault:"5"`
}

// Relay configures the outbox relay process.
type Relay struct {
	Common
	DatabaseURL string `env:"DATABASE_URL,required"`
	AMQPURL     string `env:"AMQP_URL,required"`
}

// Worker configures the channel worker process.
type Worker struct {
	Common
	DatabaseURL     string        `env:"DATABASE_URL,required"`
	AMQPURL         string        `env:"AMQP_URL,required"`
	RedisURL        string        `env:"REDIS_URL,required"`
	ProviderURL     string        `env:"PROVIDER_URL,required"`
	ProviderTimeout time.Duration `env:"PROVIDER_TIMEOUT" envDefault:"10s"`
	// RateLimit is the most sends each channel starts per second, across all workers (ADR 0003).
	RateLimit int `env:"RATE_LIMIT_PER_SECOND" envDefault:"100"`
	// Prefetch is how many messages of each work queue a worker holds unacknowledged: those waiting
	// for the priority picker and those whose sending transition has not committed (ADR 0015).
	Prefetch int `env:"WORKER_PREFETCH" envDefault:"20"`
	// Concurrency is how many sends of each channel a worker runs at once (ADR 0015).
	Concurrency int `env:"WORKER_CONCURRENCY" envDefault:"50"`
	// Priority weights while all priorities have messages waiting (ADR 0002). Each is at least 1.
	WeightHigh   int `env:"PRIORITY_WEIGHT_HIGH"   envDefault:"6"`
	WeightNormal int `env:"PRIORITY_WEIGHT_NORMAL" envDefault:"3"`
	WeightLow    int `env:"PRIORITY_WEIGHT_LOW"    envDefault:"1"`
}

// MockProvider configures the fake provider used for local runs and load tests.
type MockProvider struct {
	Common
	Latency      time.Duration `env:"MOCK_LATENCY"       envDefault:"50ms"`
	Jitter       time.Duration `env:"MOCK_JITTER"        envDefault:"50ms"`
	RejectRate   float64       `env:"MOCK_REJECT_RATE"   envDefault:"0"`
	ThrottleRate float64       `env:"MOCK_THROTTLE_RATE" envDefault:"0"`
	FailureRate  float64       `env:"MOCK_FAILURE_RATE"  envDefault:"0"`
	RetryAfter   time.Duration `env:"MOCK_RETRY_AFTER"   envDefault:"1s"`
}

// Load parses environment variables into a value of type T.
func Load[T any]() (T, error) {
	cfg, err := env.ParseAs[T]()
	if err != nil {
		return cfg, fmt.Errorf("loading config: %w", err)
	}
	return cfg, nil
}
