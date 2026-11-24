// Package outbox moves due outbox rows from PostgreSQL to RabbitMQ and recovers stuck deliveries.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

const (
	// batchSize is the maximum number of due outbox rows claimed and published in one transaction.
	batchSize = 100
	// stepTimeout bounds one outbox transaction, including publishing and waiting for broker confirms.
	stepTimeout = 10 * time.Second
	// pollInterval is how often the relay checks the outbox when idle.
	pollInterval = time.Second
	// minBackoff and maxBackoff bound the full-jitter delay after a failed step.
	minBackoff = 200 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// Publisher publishes a batch of messages to RabbitMQ and waits for the broker to confirm them.
type Publisher interface {
	Publish(ctx context.Context, msgs []queue.Message) error
}

// MessageBody is the JSON body of a notification published to a work queue.
type MessageBody struct {
	NotificationID uuid.UUID `json:"notification_id"`
}

// Relay claims due outbox rows, marks their notifications queued and publishes them to RabbitMQ.
type Relay struct {
	store *repository.Store
	pub   Publisher
	log   *slog.Logger

	batchSize              int
	stepTimeout            time.Duration
	pollInterval           time.Duration
	minBackoff, maxBackoff time.Duration
}

// NewRelay returns a relay that reads due rows from store and publishes them through pub.
func NewRelay(store *repository.Store, pub Publisher, log *slog.Logger) *Relay {
	return &Relay{
		store:        store,
		pub:          pub,
		log:          log,
		batchSize:    batchSize,
		stepTimeout:  stepTimeout,
		pollInterval: pollInterval,
		minBackoff:   minBackoff,
		maxBackoff:   maxBackoff,
	}
}

// Once runs one outbox batch step: it claims up to batchSize due rows, moves their notifications to
// queued, publishes them with confirms and deletes the claimed rows, all within one transaction bounded
// by the step timeout.
func (r *Relay) Once(ctx context.Context) (claimed, published int, err error) {
	stepCtx, cancel := context.WithTimeout(ctx, r.stepTimeout)
	defer cancel()

	return r.store.DispatchOutbox(stepCtx, r.batchSize, func(ctx context.Context, items []repository.OutboxItem) error {
		msgs := make([]queue.Message, len(items))
		for i, it := range items {
			body, err := json.Marshal(MessageBody{NotificationID: it.NotificationID})
			if err != nil {
				return fmt.Errorf("encoding message body of notification %s: %w", it.NotificationID, err)
			}
			msgs[i] = queue.Message{
				Channel:  it.Channel,
				Priority: it.Priority,
				ID:       it.NotificationID.String(),
				Body:     body,
			}
		}
		return r.pub.Publish(ctx, msgs)
	})
}

// Run drains due outbox rows until ctx ends. It wakes immediately when PostgreSQL delivers a NOTIFY on
// the outbox channel and falls back to polling every pollInterval (which also picks up scheduled rows
// and retries whose available_at passes without a new NOTIFY). A full batch is followed at once by
// another step so a backlog drains without waiting; a failed step backs off with full jitter.
func (r *Relay) Run(ctx context.Context) {
	wake := make(chan struct{}, 1)
	listenDone := make(chan struct{})
	go func() {
		defer close(listenDone)
		r.listen(ctx, wake)
	}()
	defer func() { <-listenDone }()

	failures := 0
	for {
		claimed, _, err := r.Once(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			delay := r.backoff(failures)
			failures++
			r.log.Warn("outbox dispatch step failed", "attempt", failures, "retry_in_ms", delay.Milliseconds(), "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		failures = 0

		if claimed == r.batchSize {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-time.After(r.pollInterval):
		}
	}
}

// listen keeps a LISTEN outbox session open and signals wake whenever a notification arrives,
// reconnecting with backoff if the database connection drops.
func (r *Relay) listen(ctx context.Context, wake chan<- struct{}) {
	failures := 0
	for {
		connected := false
		err := r.store.ListenOutbox(ctx, func() {
			connected = true
			select {
			case wake <- struct{}{}:
			default:
			}
		})
		if ctx.Err() != nil {
			return
		}
		if connected {
			failures = 0
		}
		delay := r.backoff(failures)
		failures++
		r.log.Warn("outbox listener stopped", "attempt", failures, "retry_in_ms", delay.Milliseconds(), "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// backoff returns the delay after the given number of consecutive failed steps: full jitter over an
// exponentially growing, capped window.
func (r *Relay) backoff(failures int) time.Duration {
	window := r.maxBackoff
	if failures < 32 { // beyond that the shift overflows and the cap applies anyway
		window = min(r.maxBackoff, r.minBackoff<<failures)
	}
	return rand.N(window + 1) //nolint:gosec // G404: jitter needs no cryptographic randomness
}
