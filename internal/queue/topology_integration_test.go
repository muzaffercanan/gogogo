//go:build integration

package queue_test

import (
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/queue"
)

// allQueues returns the nine work queues followed by the three dead-letter queues.
func allQueues() []string {
	var names []string
	for _, c := range queue.Channels() {
		for _, p := range queue.Priorities() {
			names = append(names, queue.QueueName(c, p))
		}
	}
	for _, c := range queue.Channels() {
		names = append(names, queue.DeadLetterQueueName(c))
	}
	return names
}

func TestDeclareRoutesEachMessageToOneWorkQueue(t *testing.T) {
	_, ch := declared(t)

	for _, c := range queue.Channels() {
		for _, p := range queue.Priorities() {
			name := queue.QueueName(c, p)
			publish(t, ch, name, name)
		}
	}

	for _, c := range queue.Channels() {
		for _, p := range queue.Priorities() {
			name := queue.QueueName(c, p)
			if d := receive(t, ch, name); string(d.Body) != name {
				t.Errorf("%s received %q, want %q", name, d.Body, name)
			}
			assertEmpty(t, ch, name)
		}
		assertEmpty(t, ch, queue.DeadLetterQueueName(c))
	}
}

func TestRejectedMessagesGoToTheirChannelsDeadLetterQueue(t *testing.T) {
	_, ch := declared(t)

	for _, c := range queue.Channels() {
		for _, p := range queue.Priorities() {
			name := queue.QueueName(c, p)
			publish(t, ch, name, name)
			if err := receive(t, ch, name).Nack(false, false); err != nil {
				t.Fatalf("rejecting from %s: %v", name, err)
			}

			dlq := queue.DeadLetterQueueName(c)
			d := receive(t, ch, dlq)
			if string(d.Body) != name {
				t.Errorf("%s received %q, want the message rejected from %s", dlq, d.Body, name)
			}
			if err := d.Ack(false); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, c := range queue.Channels() {
		assertEmpty(t, ch, queue.DeadLetterQueueName(c))
	}
}

func TestDeclareCreatesQuorumQueues(t *testing.T) {
	conn, _ := declared(t)

	// Redeclaring with another queue type is refused only if the existing queue's type differs.
	for _, name := range allQueues() {
		ch := openChannel(t, conn) // a refused declare closes the channel
		_, err := ch.QueueDeclare(name, true, false, false, false, amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeClassic})
		if amqpCode(err) != amqp.PreconditionFailed {
			t.Errorf("redeclaring %s as a classic queue = %v, want 406 PRECONDITION_FAILED", name, err)
		}
	}
}

func TestDeclareIsIdempotent(t *testing.T) {
	conn, ch := declared(t)
	name := queue.QueueName("sms", "high")
	publish(t, ch, name, "before the second declare")

	if err := queue.Declare(t.Context(), openChannel(t, conn)); err != nil {
		t.Fatalf("declaring the topology again: %v", err)
	}

	if d := receive(t, ch, name); string(d.Body) != "before the second declare" {
		t.Errorf("%s received %q after the second declare, want the earlier message", name, d.Body)
	}
}

func TestDeclareFailsOnAConflictingQueue(t *testing.T) {
	conn := dial(t, mq.NewVHost(t))
	name := queue.QueueName("email", "low")
	if _, err := openChannel(t, conn).QueueDeclare(name, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring a classic %s: %v", name, err)
	}

	err := queue.Declare(t.Context(), openChannel(t, conn))

	if amqpCode(err) != amqp.PreconditionFailed || !strings.Contains(err.Error(), name) {
		t.Errorf("Declare over a classic %s = %v, want a 406 PRECONDITION_FAILED naming the queue", name, err)
	}
}
