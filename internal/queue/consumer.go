package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/backoff"
)

// ErrChannelClosed is returned when a delivery is acknowledged or rejected after the AMQP channel it
// arrived on has closed. The broker has already put the message back on its queue and delivers it again.
var ErrChannelClosed = errors.New("the channel the delivery arrived on is closed; the broker delivers the message again")

// Delivery is a notification received from a work queue. Settle it exactly once, with Ack or Reject.
type Delivery struct {
	// NotificationID is the AMQP message-id the relay sets.
	NotificationID string
	// Channel and Priority are those of the work queue the message came from.
	Channel  domain.Channel
	Priority domain.Priority
	// CorrelationID comes from the correlation_id header, or is "" if the message has none.
	CorrelationID string
	Body          []byte
	// Redelivered reports that the broker delivered the message before, to this or another consumer,
	// without it being acknowledged.
	Redelivered bool

	acker amqp.Acknowledger
	tag   uint64
}

// Ack tells the broker the message is done with, which removes it from its queue.
func (d Delivery) Ack() error {
	if d.acker == nil {
		return fmt.Errorf("acknowledging message %s: not received from a broker", d.NotificationID)
	}
	return settled("acknowledging", d.NotificationID, d.acker.Ack(d.tag, false))
}

// Reject tells the broker the message cannot be delivered. The broker dead-letters it to the dead-letter
// queue of its channel.
func (d Delivery) Reject() error {
	if d.acker == nil {
		return fmt.Errorf("rejecting message %s: not received from a broker", d.NotificationID)
	}
	return settled("rejecting", d.NotificationID, d.acker.Reject(d.tag, false))
}

func settled(action, id string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, amqp.ErrClosed):
		return fmt.Errorf("%s message %s: %w", action, id, ErrChannelClosed)
	default:
		return fmt.Errorf("%s message %s: %w", action, id, err)
	}
}

// Consumer reconnect backoff (K-35): after a lost channel or a failed attempt, the consumer waits
// rand(0, min(5 s, 100 ms·2^failures)) before opening a channel again.
const (
	consumerMinBackoff = 100 * time.Millisecond
	consumerMaxBackoff = 5 * time.Second
)

// Consumer receives the three work queues of one delivery channel on an AMQP channel of its own and puts
// each message into the buffer of its priority. Create it with NewConsumer and start it with Run.
//
// Each buffer holds up to prefetch deliveries, the most the broker sends a consumer before some are
// acknowledged, so the consumer never waits for a reader. The buffers outlive AMQP channels: when a
// channel closes, the consumer discards the deliveries it left in them (the broker has put those back
// on their queues) and opens a new one on the Supervisor's current connection. The buffers are never
// closed.
type Consumer struct {
	sup      *Supervisor
	channel  domain.Channel
	prefetch int
	log      *slog.Logger

	minBackoff, maxBackoff time.Duration

	buffers map[domain.Priority]chan Delivery // one per priority, fixed at construction
}

// NewConsumer returns a consumer for the work queues of channel c that opens its AMQP channels
// through sup and lets the broker send up to prefetch unacknowledged messages from each queue.
func NewConsumer(sup *Supervisor, c domain.Channel, prefetch int, log *slog.Logger) (*Consumer, error) {
	if !slices.Contains(Channels(), c) {
		return nil, fmt.Errorf("no work queues for channel %q", c)
	}
	if prefetch < 1 || prefetch > math.MaxUint16 {
		return nil, fmt.Errorf("prefetch %d is outside [1, %d]", prefetch, math.MaxUint16)
	}
	buffers := make(map[domain.Priority]chan Delivery, len(Priorities()))
	for _, p := range Priorities() {
		buffers[p] = make(chan Delivery, prefetch)
	}
	return &Consumer{
		sup:        sup,
		channel:    c,
		prefetch:   prefetch,
		log:        log.With("channel", string(c)),
		minBackoff: consumerMinBackoff,
		maxBackoff: consumerMaxBackoff,
		buffers:    buffers,
	}, nil
}

// Deliveries returns the buffer of the work queue with priority p. A delivery taken from it may be
// stale: if its AMQP channel has closed since, Ack and Reject fail with ErrChannelClosed.
func (c *Consumer) Deliveries(p domain.Priority) <-chan Delivery {
	return c.buffers[p]
}

// Run consumes until ctx ends, then closes its AMQP channel, which returns every unacknowledged message
// to its queue. Whenever the channel cannot be opened or stops (the connection drops, the broker closes
// the channel or cancels a consumer), Run discards what the channel left in the buffers and tries again
// after a backoff, without giving up.
func (c *Consumer) Run(ctx context.Context) {
	failures := 0
	for {
		started, err := c.consume(ctx)
		c.discardBuffered()
		if ctx.Err() != nil {
			return
		}
		if started {
			failures = 0
		}
		delay := backoff.FullJitter(failures, c.minBackoff, c.maxBackoff)
		failures++
		level := slog.LevelWarn
		if errors.Is(err, ErrNotConnected) {
			level = slog.LevelDebug // the supervisor already reports the outage
		}
		c.log.Log(ctx, level, "not consuming work queues", "attempt", failures, "retry_in_ms", delay.Milliseconds(), "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// consume opens an AMQP channel, starts one consumer per work queue and forwards deliveries until ctx
// ends or the channel stops. started reports whether every consumer started.
func (c *Consumer) consume(ctx context.Context) (started bool, err error) {
	ch, err := c.sup.Channel()
	if err != nil {
		return false, err
	}
	defer func() { _ = ch.Close() }()

	// The client closes every delivery stream of a channel that shuts down, after sending the reason
	// here; one slot keeps it from waiting for us.
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return false, fmt.Errorf("setting prefetch: %w", err)
	}
	priorities := Priorities() // three, matching the select below
	streams := make([]<-chan amqp.Delivery, len(priorities))
	for i, p := range priorities {
		name := QueueName(c.channel, p)
		streams[i], err = ch.Consume(name, "", false, false, false, false, nil)
		if err != nil {
			return false, fmt.Errorf("consuming %s: %w", name, err)
		}
	}
	c.log.Info("consuming work queues", "prefetch", c.prefetch)

	for {
		var (
			d  amqp.Delivery
			ok bool
			i  int
		)
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case d, ok = <-streams[0]:
			i = 0
		case d, ok = <-streams[1]:
			i = 1
		case d, ok = <-streams[2]:
			i = 2
		}
		if !ok {
			// Either the channel shut down, or the broker cancelled this one consumer (for example
			// because its queue was deleted). Both end this channel.
			select {
			case amqpErr := <-closed:
				if amqpErr == nil { // closed without an error: the connection was closed on purpose
					return true, errors.New("channel closed")
				}
				return true, fmt.Errorf("channel closed: %w", amqpErr)
			default:
				return true, fmt.Errorf("broker cancelled the consumer of %s", QueueName(c.channel, priorities[i]))
			}
		}
		delivery := Delivery{
			NotificationID: d.MessageId,
			Channel:        c.channel,
			Priority:       priorities[i],
			CorrelationID:  CorrelationID(d.Headers),
			Body:           d.Body,
			Redelivered:    d.Redelivered,
			acker:          d.Acknowledger,
			tag:            d.DeliveryTag,
		}
		select {
		case c.buffers[priorities[i]] <- delivery:
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
}

// discardBuffered drops the deliveries a closed channel left in the buffers: the broker has put them
// back on their queues, so they would only fail to settle. Readers may take some of them concurrently;
// those fail with ErrChannelClosed.
func (c *Consumer) discardBuffered() {
	for _, b := range c.buffers {
	drain:
		for {
			select {
			case <-b:
			default:
				break drain
			}
		}
	}
}
