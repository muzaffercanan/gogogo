package queue

import (
	"context"
	"fmt"
	"slices"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
)

// AMQP header keys that carry a notification's correlation ID and trace context from the relay to the
// worker.
const (
	CorrelationHeader = "correlation_id"
	TraceParentHeader = "traceparent"
)

// Headers returns an AMQP header table carrying correlationID under CorrelationHeader and traceparent
// under TraceParentHeader, leaving out the empty ones, or nil when both are empty.
func Headers(correlationID, traceparent string) amqp.Table {
	var h amqp.Table
	for key, value := range map[string]string{CorrelationHeader: correlationID, TraceParentHeader: traceparent} {
		if value == "" {
			continue
		}
		if h == nil {
			h = amqp.Table{}
		}
		h[key] = value
	}
	return h
}

// CorrelationID extracts the correlation ID from an AMQP delivery's headers, or returns "" if none is
// present.
func CorrelationID(headers amqp.Table) string {
	id, _ := headers[CorrelationHeader].(string)
	return id
}

// TraceParent extracts the W3C traceparent from an AMQP delivery's headers, or returns "" if none is
// present.
func TraceParent(headers amqp.Table) string {
	tp, _ := headers[TraceParentHeader].(string)
	return tp
}

// WithCorrelationID returns a copy of ctx carrying the correlation ID from headers (if any), so records
// logged with the returned context include a correlation_id attribute.
func WithCorrelationID(ctx context.Context, headers amqp.Table) context.Context {
	if id := CorrelationID(headers); id != "" {
		return logging.WithCorrelationID(ctx, id)
	}
	return ctx
}

// Message is a notification to route to the work queue of its channel and priority.
type Message struct {
	Channel  domain.Channel
	Priority domain.Priority
	// ID is the notification ID, sent as the AMQP message-id.
	ID string
	// Body is the JSON payload.
	Body []byte
	// Headers are optional AMQP headers.
	Headers amqp.Table
}

// Publisher publishes messages over a Supervisor's connection and waits for the broker to confirm them.
type Publisher struct {
	sup *Supervisor
}

// NewPublisher returns a publisher that uses the connection sup keeps open.
func NewPublisher(sup *Supervisor) *Publisher {
	return &Publisher{sup: sup}
}

// Publish sends every message as persistent and mandatory, then waits for each confirm. It returns nil
// only if the broker confirmed every message and routed each one to a queue. Any other outcome (a nack,
// an unroutable message, a lost connection, ctx ending) is an error, and the caller must treat the whole
// batch as unsent and publish it again later: some of the messages may have reached their queues, and
// consumers drop the duplicates. Without a connection Publish fails at once with ErrNotConnected.
//
// Each call uses a channel of its own, so calls may run concurrently.
func (p *Publisher) Publish(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	keys := make([]string, len(msgs))
	for i, m := range msgs {
		if !slices.Contains(Channels(), m.Channel) || !slices.Contains(Priorities(), m.Priority) {
			return fmt.Errorf("message %s: no queue for channel %q and priority %q", m.ID, m.Channel, m.Priority)
		}
		keys[i] = QueueName(m.Channel, m.Priority)
	}
	return p.publish(ctx, Exchange, keys, msgs)
}

// PublishDeadLetter sends m to the dead-letter queue of its channel, as a work queue's reject would,
// with the same guarantees as Publish. The worker uses it for deliveries it acknowledged before they
// failed for good (ADR 0015).
func (p *Publisher) PublishDeadLetter(ctx context.Context, m Message) error {
	if !slices.Contains(Channels(), m.Channel) {
		return fmt.Errorf("message %s: no dead-letter queue for channel %q", m.ID, m.Channel)
	}
	return p.publish(ctx, DeadLetterExchange, []string{string(m.Channel)}, []Message{m})
}

// publish sends msgs[i] to exchange with routing key keys[i] and waits for every confirm.
func (p *Publisher) publish(ctx context.Context, exchange string, keys []string, msgs []Message) error {
	ch, err := p.sup.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enabling publisher confirms: %w", err)
	}
	// The broker returns an unroutable message before confirming it, and the client hands both over in
	// that order, so every return has arrived once the last confirm has. One slot per message keeps the
	// client from ever blocking on this channel.
	returns := ch.NotifyReturn(make(chan amqp.Return, len(msgs)))

	confirms := make([]*amqp.DeferredConfirmation, len(msgs))
	for i, m := range msgs {
		confirms[i], err = ch.PublishWithDeferredConfirmWithContext(ctx, exchange, keys[i], true, false, amqp.Publishing{
			Headers:      m.Headers,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    m.ID,
			Body:         m.Body,
		})
		if err != nil {
			return fmt.Errorf("publishing message %s: %w", m.ID, err)
		}
	}

	for i, m := range msgs {
		acked, err := confirms[i].WaitContext(ctx)
		if err != nil {
			return fmt.Errorf("waiting for the confirm of message %s: %w", m.ID, err)
		}
		if !acked { // a nack, or the channel closed before the confirm
			return fmt.Errorf("broker did not accept message %s", m.ID)
		}
	}

	select {
	case r, ok := <-returns:
		if ok {
			return fmt.Errorf("message %s was not routed to a queue: %d %s", r.MessageId, r.ReplyCode, r.ReplyText)
		}
	default:
	}
	return nil
}
