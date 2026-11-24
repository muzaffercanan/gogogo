package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/backoff"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// Limiter spaces the sends of a channel. *ratelimit.Limiter is one.
type Limiter interface {
	Wait(ctx context.Context, c domain.Channel) (time.Duration, error)
}

// Backoff while the limiter cannot be reached: nothing is sent without a token (ADR 0004).
const (
	limiterMinBackoff = 100 * time.Millisecond
	limiterMaxBackoff = 5 * time.Second
)

// Dispatcher delivers the notifications of one channel. It picks the next delivery (K-36), waits for a
// free sender, then for a rate-limit token, and hands the delivery to that sender; taking the sender
// first means a send starts as soon as its token is granted (K-39).
type Dispatcher struct {
	channel   domain.Channel
	picker    *Picker
	limiter   Limiter
	deliverer *Deliverer
	metrics   *Metrics
	senders   chan struct{} // one slot per send in flight
	log       *slog.Logger

	minBackoff, maxBackoff time.Duration
}

// NewDispatcher returns a dispatcher for channel c that runs at most concurrency sends at once and
// reports to metrics.
func NewDispatcher(c domain.Channel, picker *Picker, limiter Limiter, deliverer *Deliverer, metrics *Metrics, concurrency int, log *slog.Logger) (*Dispatcher, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("concurrency %d is below 1", concurrency)
	}
	return &Dispatcher{
		channel:    c,
		picker:     picker,
		limiter:    limiter,
		deliverer:  deliverer,
		metrics:    metrics,
		senders:    make(chan struct{}, concurrency),
		log:        log.With("channel", string(c)),
		minBackoff: limiterMinBackoff,
		maxBackoff: limiterMaxBackoff,
	}, nil
}

// Run dispatches until ctx ends, then waits for the sends in flight to finish. A delivery picked but
// not yet sent stays unacknowledged and goes back to its queue when the consumer stops.
func (dp *Dispatcher) Run(ctx context.Context) {
	var inFlight sync.WaitGroup
	defer inFlight.Wait()
	for {
		d, err := dp.picker.Next(ctx)
		if err != nil {
			return
		}
		select {
		case dp.senders <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if !dp.token(ctx) {
			<-dp.senders
			return
		}
		inFlight.Go(func() {
			defer func() { <-dp.senders }()
			dp.deliverer.Deliver(ctx, d)
		})
	}
}

// token waits for a rate-limit token. While the limiter cannot be reached it keeps trying with backoff
// and sends nothing (fail closed, ADR 0004); the channel then reports not ready and
// ratelimit_unavailable is 1. It reports false if ctx ended first.
func (dp *Dispatcher) token(ctx context.Context) bool {
	for failures := 0; ; failures++ {
		_, err := dp.limiter.Wait(ctx, dp.channel)
		if err == nil {
			dp.metrics.limiterRecovered(dp.channel)
			if failures > 0 {
				dp.log.InfoContext(ctx, "rate limiter reachable again; sending resumes")
			}
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		dp.metrics.limiterFailed(dp.channel)
		delay := backoff.FullJitter(failures, dp.minBackoff, dp.maxBackoff)
		if failures == 0 {
			dp.log.WarnContext(ctx, "rate limiter unavailable; sending stops until it is back", "error", err)
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
	}
}

// Options tunes the worker of one channel.
type Options struct {
	// Prefetch is how many messages of each work queue the consumer holds (K-35).
	Prefetch int
	// Concurrency is how many sends run at once (K-39).
	Concurrency int
	// Weights are the priorities' shares (ADR 0002).
	Weights Weights
}

// Channel bundles the consumer and dispatcher of one delivery channel.
type Channel struct {
	consumer   *queue.Consumer
	dispatcher *Dispatcher
}

// NewChannel wires a consumer of channel c's work queues to a picker and a dispatcher that sends
// through deliverer at the pace of limiter and reports to metrics.
func NewChannel(c domain.Channel, sup *queue.Supervisor, limiter Limiter, deliverer *Deliverer, metrics *Metrics, opts Options, log *slog.Logger) (*Channel, error) {
	consumer, err := queue.NewConsumer(sup, c, opts.Prefetch, log)
	if err != nil {
		return nil, err
	}
	picker, err := NewPicker(consumer, opts.Weights)
	if err != nil {
		return nil, err
	}
	dispatcher, err := NewDispatcher(c, picker, limiter, deliverer, metrics, opts.Concurrency, log)
	if err != nil {
		return nil, err
	}
	return &Channel{consumer: consumer, dispatcher: dispatcher}, nil
}

// Run consumes and dispatches until ctx ends. It returns once the sends in flight have finished and the
// consumer has closed its AMQP channel, which returns the deliveries not yet sent to their queues.
func (c *Channel) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { c.consumer.Run(ctx) })
	c.dispatcher.Run(ctx)
	wg.Wait()
}
