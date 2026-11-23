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
