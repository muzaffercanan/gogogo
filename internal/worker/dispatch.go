package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/backoff"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
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

// Dispatcher delivers the notifications of one channel. It picks the next delivery (ADR 0002), waits until
// the circuit breaker lets it through (ADR 0009), then for a free sender, then for a rate-limit token, and
// hands the delivery to that sender; taking the sender first means a send starts as soon as its token
// is granted (ADR 0015). While the breaker is open it picks nothing more: the deliveries buffered so far
// stay unacknowledged with the worker.
//
// Each delivery gets a consumer span from the moment it is picked, under the relay's publish span its
// message names, so that its trace shows what it waited for: events mark when the breaker let it
// through, when a sender was free and when its rate-limit token was granted.
type Dispatcher struct {
	channel   domain.Channel
	picker    *Picker
	limiter   Limiter
	deliverer *Deliverer
	metrics   *Metrics
	breaker   *breaker
	senders   chan struct{} // one slot per send in flight
	tracer    trace.Tracer
	log       *slog.Logger

	minBackoff, maxBackoff time.Duration
}

// NewDispatcher returns a dispatcher for channel c that runs at most opts.Concurrency sends at once
// behind a circuit breaker with opts.Breaker, reports to metrics and traces each delivery with tp.
func NewDispatcher(c domain.Channel, picker *Picker, limiter Limiter, deliverer *Deliverer, metrics *Metrics, opts Options, tp trace.TracerProvider, log *slog.Logger) (*Dispatcher, error) {
	if opts.Concurrency < 1 {
		return nil, fmt.Errorf("concurrency %d is below 1", opts.Concurrency)
	}
	log = log.With("channel", string(c))
	settings := opts.Breaker
	if settings == (BreakerSettings{}) {
		settings = DefaultBreakerSettings()
	}
	b, err := newBreaker(c, settings, metrics, log)
	if err != nil {
		return nil, err
	}
	return &Dispatcher{
		channel:    c,
		picker:     picker,
		limiter:    limiter,
		deliverer:  deliverer,
		metrics:    metrics,
		breaker:    b,
		senders:    make(chan struct{}, opts.Concurrency),
		tracer:     tp.Tracer("github.com/muzaffercanan/gogogo/internal/worker"),
		log:        log,
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
		spanCtx, span := dp.processSpan(ctx, d)
		done, ok := dp.breaker.allow(spanCtx)
		if !ok {
			endProcess(span, OutcomeAbandoned)
			return
		}
		span.AddEvent("circuit breaker allowed")
		select {
		case dp.senders <- struct{}{}:
			span.AddEvent("sender acquired")
		case <-ctx.Done():
			done(OutcomeAbandoned)
			endProcess(span, OutcomeAbandoned)
			return
		}
		waited, ok := dp.token(spanCtx)
		if !ok {
			<-dp.senders
			done(OutcomeAbandoned)
			endProcess(span, OutcomeAbandoned)
			return
		}
		span.AddEvent("rate limit token granted", trace.WithAttributes(attribute.Float64("ratelimit.wait_ms", float64(waited.Microseconds())/1000)))
		inFlight.Go(func() {
			defer func() { <-dp.senders }()
			o := dp.deliverer.Deliver(spanCtx, d)
			done(o)
			endProcess(span, o)
		})
	}
}

// processSpan starts the consumer span of delivering d, under the span its message names.
func (dp *Dispatcher) processSpan(ctx context.Context, d queue.Delivery) (context.Context, trace.Span) {
	key := queue.QueueName(d.Channel, d.Priority)
	return dp.tracer.Start(tracing.WithTraceParent(ctx, d.TraceParent), "process "+key,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemRabbitMQ,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingOperationName("process"),
			semconv.MessagingDestinationName(key),
			semconv.MessagingMessageID(d.NotificationID),
			tracing.NotificationIDKey.String(d.NotificationID),
			tracing.NotificationChannelKey.String(string(d.Channel)),
			tracing.NotificationPriorityKey.String(string(d.Priority)),
		))
}

// endProcess ends the span of a delivery with its outcome; one in which the notification was not sent
// (other than a skip) is an error.
func endProcess(span trace.Span, o Outcome) {
	span.SetAttributes(tracing.OutcomeKey.String(string(o)))
	if o != OutcomeSent && o != OutcomeSkipped {
		span.SetStatus(codes.Error, string(o))
	}
	span.End()
}

// token waits for a rate-limit token and returns how long the limiter made it wait. While the limiter
// cannot be reached it keeps trying with backoff and sends nothing (fail closed, ADR 0004); the channel
// then reports not ready and ratelimit_unavailable is 1. It reports false if ctx ended first.
func (dp *Dispatcher) token(ctx context.Context) (time.Duration, bool) {
	for failures := 0; ; failures++ {
		waited, err := dp.limiter.Wait(ctx, dp.channel)
		if err == nil {
			dp.metrics.waitedForSlot(dp.channel, waited)
			dp.metrics.limiterRecovered(dp.channel)
			if failures > 0 {
				dp.log.InfoContext(ctx, "rate limiter reachable again; sending resumes")
			}
			return waited, true
		}
		if ctx.Err() != nil {
			return 0, false
		}
		dp.metrics.limiterFailed(dp.channel)
		delay := backoff.FullJitter(failures, dp.minBackoff, dp.maxBackoff)
		if failures == 0 {
			dp.log.WarnContext(ctx, "rate limiter unavailable; sending stops until it is back", "error", err)
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(delay):
		}
	}
}

// Options tunes the worker of one channel.
type Options struct {
	// Prefetch is how many messages of each work queue the consumer holds.
	Prefetch int
	// Concurrency is how many sends run at once (ADR 0015).
	Concurrency int
	// Weights are the priorities' shares (ADR 0002).
	Weights Weights
	// Breaker tunes the circuit breaker; the zero value means DefaultBreakerSettings.
	Breaker BreakerSettings
}

// Channel bundles the consumer and dispatcher of one delivery channel.
type Channel struct {
	consumer   *queue.Consumer
	dispatcher *Dispatcher
}

// NewChannel wires a consumer of channel c's work queues to a picker and a dispatcher that sends
// through deliverer at the pace of limiter, reports to metrics and traces with tp.
func NewChannel(c domain.Channel, sup *queue.Supervisor, limiter Limiter, deliverer *Deliverer, metrics *Metrics, opts Options, tp trace.TracerProvider, log *slog.Logger) (*Channel, error) {
	consumer, err := queue.NewConsumer(sup, c, opts.Prefetch, log)
	if err != nil {
		return nil, err
	}
	picker, err := NewPicker(consumer, opts.Weights)
	if err != nil {
		return nil, err
	}
	dispatcher, err := NewDispatcher(c, picker, limiter, deliverer, metrics, opts, tp, log)
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
