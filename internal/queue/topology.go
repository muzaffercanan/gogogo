// Package queue connects the service to RabbitMQ: the exchanges and queues notifications travel
// through, a supervisor that keeps the connection open, and a publisher that waits for confirms.
package queue

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

const (
	// Exchange receives every notification. The routing key is the name of the work queue, so each
	// message lands in exactly one of them.
	Exchange = "notifications"
	// DeadLetterExchange receives the messages that work queues reject. The routing key is the
	// channel, which selects that channel's dead-letter queue.
	DeadLetterExchange = "notifications.dlx"
)

// QueueName returns the work queue for a channel and a priority, for example "sms.high".
func QueueName(c domain.Channel, p domain.Priority) string {
	return string(c) + "." + string(p)
}

// DeadLetterQueueName returns the dead-letter queue of a channel, for example "sms.dlq".
func DeadLetterQueueName(c domain.Channel) string {
	return string(c) + ".dlq"
}

// Channels lists the delivery channels the topology has queues for.
func Channels() []domain.Channel {
	return []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
}

// Priorities lists the priorities each channel has a work queue for, highest first.
func Priorities() []domain.Priority {
	return []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}
}

// Declare creates the two exchanges, the nine work queues and the three dead-letter queues, and binds
// them. Every queue is a durable quorum queue. Declaring is idempotent, so every process runs it on
// every connect: it changes nothing on a broker that already has this topology, and it fails with
// PRECONDITION_FAILED (closing ch) if a queue of the same name exists with other arguments.
//
// The broker calls are not cancellable; ctx is checked between them.
func Declare(ctx context.Context, ch *amqp.Channel) error {
	for _, name := range []string{Exchange, DeadLetterExchange} {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ch.ExchangeDeclare(name, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declaring exchange %s: %w", name, err)
		}
	}

	for _, c := range Channels() {
		dlq := DeadLetterQueueName(c)
		if err := declareQueue(ctx, ch, dlq, DeadLetterExchange, string(c), nil); err != nil {
			return err
		}
		for _, p := range Priorities() {
			name := QueueName(c, p)
			deadLetter := amqp.Table{
				"x-dead-letter-exchange":    DeadLetterExchange,
				"x-dead-letter-routing-key": string(c),
			}
			if err := declareQueue(ctx, ch, name, Exchange, name, deadLetter); err != nil {
				return err
			}
		}
	}
	return nil
}

// declareQueue declares a durable quorum queue with the extra arguments and binds it to exchange.
func declareQueue(ctx context.Context, ch *amqp.Channel, name, exchange, key string, extra amqp.Table) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	args := amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeQuorum}
	for k, v := range extra {
		args[k] = v
	}
	if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
		return fmt.Errorf("declaring queue %s: %w", name, err)
	}
	if err := ch.QueueBind(name, key, exchange, false, nil); err != nil {
		return fmt.Errorf("binding queue %s to %s: %w", name, exchange, err)
	}
	return nil
}
