//go:build integration

package queue_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// topology returns a channel on the virtual host of url, after declaring the topology there.
func topology(t *testing.T, url string) *amqp.Channel {
	t.Helper()
	ch := openChannel(t, dial(t, url))
	if err := queue.Declare(t.Context(), ch); err != nil {
		t.Fatalf("declaring topology: %v", err)
	}
	return ch
}

// startConsumer runs a consumer of the sms work queues through s with a short backoff and stops it
// when the test ends. The returned channel is closed when Run returns.
func startConsumer(t *testing.T, s *queue.Supervisor, prefetch int) (*queue.Consumer, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	c, err := queue.NewConsumer(s, domain.ChannelSMS, prefetch, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatal(err)
	}
	queue.SetConsumerBackoff(c, 10*time.Millisecond, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after its context was cancelled")
		}
	})
	return c, cancel, done
}

// consuming starts a supervisor and an sms consumer on a fresh virtual host and returns them with a
// channel for publishing and inspecting queues there.
func consuming(t *testing.T, prefetch int) (*queue.Consumer, *amqp.Channel) {
	t.Helper()
	url := mq.NewVHost(t)
	ch := topology(t, url)
	s, _, _ := supervise(t, url)
	c, _, _ := startConsumer(t, s, prefetch)
	return c, ch
}

// sentTraceParent is the trace context send puts in every message.
const sentTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

// send publishes a notification message the way the relay does: persistent, with the notification ID as
// the message-id and the correlation ID and a trace context in the headers.
func send(t *testing.T, ch *amqp.Channel, c domain.Channel, p domain.Priority, id, correlationID string) {
	t.Helper()
	err := ch.PublishWithContext(t.Context(), queue.Exchange, queue.QueueName(c, p), true, false, amqp.Publishing{
		Headers:      queue.Headers(correlationID, sentTraceParent),
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    id,
		Body:         []byte(`{"id":"` + id + `"}`),
	})
	if err != nil {
		t.Fatalf("publishing %s: %v", id, err)
	}
}

// next waits up to ten seconds for a delivery in the buffer of priority p.
func next(t *testing.T, c *queue.Consumer, p domain.Priority) queue.Delivery {
	t.Helper()
	select {
	case d := <-c.Deliveries(p):
		return d
	case <-time.After(10 * time.Second):
		t.Fatalf("no delivery arrived in the %s buffer", p)
		return queue.Delivery{}
	}
}

// assertNoDelivery fails the test if the buffer of priority p yields a delivery within wait.
func assertNoDelivery(t *testing.T, c *queue.Consumer, p domain.Priority, wait time.Duration) {
	t.Helper()
	select {
	case d := <-c.Deliveries(p):
		t.Errorf("the %s buffer yielded %s, want nothing", p, d.NotificationID)
	case <-time.After(wait):
	}
}

// readyCount returns the number of messages in name that wait for a consumer (not counting
// unacknowledged ones).
func readyCount(t *testing.T, ch *amqp.Channel, name string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspecting %s: %v", name, err)
	}
	return q.Messages
}

func TestConsumerPutsEachWorkQueueIntoTheBufferOfItsPriority(t *testing.T) {
	c, ch := consuming(t, 5)
	for _, p := range queue.Priorities() {
		send(t, ch, domain.ChannelSMS, p, "sms-"+string(p), "corr-"+string(p))
	}
	send(t, ch, domain.ChannelEmail, domain.PriorityHigh, "email-high", "")

	for _, p := range queue.Priorities() {
		d := next(t, c, p)
		want := "sms-" + string(p)
		if d.NotificationID != want || d.Channel != domain.ChannelSMS || d.Priority != p {
			t.Errorf("%s buffer: got %s on %s/%s, want %s on sms/%s", p, d.NotificationID, d.Channel, d.Priority, want, p)
		}
		if d.CorrelationID != "corr-"+string(p) {
			t.Errorf("%s: correlation ID %q, want %q", want, d.CorrelationID, "corr-"+string(p))
		}
		if d.TraceParent != sentTraceParent {
			t.Errorf("%s: traceparent %q, want %q", want, d.TraceParent, sentTraceParent)
		}
		if string(d.Body) != `{"id":"`+want+`"}` {
			t.Errorf("%s: body %s", want, d.Body)
		}
		if d.Redelivered {
			t.Errorf("%s: marked redelivered on its first delivery", want)
		}
		if err := d.Ack(); err != nil {
			t.Errorf("acknowledging %s: %v", want, err)
		}
	}
	for _, p := range queue.Priorities() {
		assertNoDelivery(t, c, p, 100*time.Millisecond)
	}
	// Other channels' queues are left to their own consumers.
	if d := receive(t, ch, "email.high"); d.MessageId != "email-high" {
		t.Errorf("email.high holds %s, want email-high", d.MessageId)
	}
}

func TestConsumerHoldsAtMostPrefetchUnacknowledgedMessagesPerQueue(t *testing.T) {
	c, ch := consuming(t, 2)
	for _, id := range []string{"h1", "h2", "h3", "h4"} {
		send(t, ch, domain.ChannelSMS, domain.PriorityHigh, id, "")
	}
	send(t, ch, domain.ChannelSMS, domain.PriorityLow, "l1", "")

	high := c.Deliveries(domain.PriorityHigh)
	waitFor(t, "two high messages are buffered and two wait in sms.high", func() bool {
		return len(high) == 2 && readyCount(t, ch, "sms.high") == 2
	})
	// Each consumer has its own prefetch, so a full high queue does not hold back low.
	if d := next(t, c, domain.PriorityLow); d.NotificationID != "l1" {
		t.Errorf("low buffer yielded %s, want l1", d.NotificationID)
	}
	time.Sleep(200 * time.Millisecond)
	if n, ready := len(high), readyCount(t, ch, "sms.high"); n != 2 || ready != 2 {
		t.Fatalf("with nothing acknowledged: %d buffered, %d ready; want 2 and 2", n, ready)
	}

	// Taking a delivery does not make room; acknowledging it does.
	d := next(t, c, domain.PriorityHigh)
	time.Sleep(200 * time.Millisecond)
	if n := len(high); n != 1 {
		t.Fatalf("after taking one unacknowledged delivery, %d buffered; want 1", n)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("acknowledging %s: %v", d.NotificationID, err)
	}
	waitFor(t, "the next high message is buffered", func() bool {
		return len(high) == 2 && readyCount(t, ch, "sms.high") == 1
	})
}

func TestRejectedDeliveryGoesToTheDeadLetterQueueOfItsChannel(t *testing.T) {
	c, ch := consuming(t, 5)
	send(t, ch, domain.ChannelSMS, domain.PriorityNormal, "n1", "")

	if err := next(t, c, domain.PriorityNormal).Reject(); err != nil {
		t.Fatalf("rejecting: %v", err)
	}
	if d := receive(t, ch, "sms.dlq"); d.MessageId != "n1" {
		t.Errorf("sms.dlq holds %s, want n1", d.MessageId)
	}
	assertNoDelivery(t, c, domain.PriorityNormal, 200*time.Millisecond)
}

func TestConsumerResumesAfterTheConnectionDrops(t *testing.T) {
	url := mq.NewVHost(t)
	ch := topology(t, url)
	p, proxied := newProxy(t, url, true)
	s, _, _ := supervise(t, proxied)
	c, _, _ := startConsumer(t, s, 5)

	send(t, ch, domain.ChannelSMS, domain.PriorityHigh, "a", "")
	send(t, ch, domain.ChannelSMS, domain.PriorityHigh, "b", "")
	taken := next(t, c, domain.PriorityHigh) // a; b stays in the buffer
	high := c.Deliveries(domain.PriorityHigh)
	waitFor(t, "the second message is buffered", func() bool { return len(high) == 1 })

	p.set(false)
	waitFor(t, "the supervisor notices the drop", notReady(s))
	// Nothing reads the buffer meanwhile, so only the consumer can empty it: the copy of b the closed
	// channel left there could not be acknowledged any more.
	waitFor(t, "the consumer discards the delivery the closed channel left", func() bool { return len(high) == 0 })
	p.set(true)

	// The broker delivers both again on the new channel.
	got := map[string]bool{}
	for range 2 {
		d := next(t, c, domain.PriorityHigh)
		if !d.Redelivered {
			t.Errorf("%s is not marked redelivered: a delivery from the closed channel was handed out", d.NotificationID)
		}
		if err := d.Ack(); err != nil {
			t.Errorf("acknowledging %s: %v", d.NotificationID, err)
		}
		got[d.NotificationID] = true
	}
	if !got["a"] || !got["b"] {
		t.Errorf("after reconnecting got %v, want a and b", got)
	}
	if err := taken.Ack(); !errors.Is(err, queue.ErrChannelClosed) {
		t.Errorf("acknowledging a delivery from the closed channel: %v, want ErrChannelClosed", err)
	}
	assertNoDelivery(t, c, domain.PriorityHigh, 200*time.Millisecond)
}

func TestConsumerReopensItsChannelAfterTheBrokerClosesIt(t *testing.T) {
	url := mq.NewVHost(t)
	ch := topology(t, url)
	s, _, _ := supervise(t, url)
	c, _, _ := startConsumer(t, s, 5)

	send(t, ch, domain.ChannelSMS, domain.PriorityNormal, "a", "")
	d := next(t, c, domain.PriorityNormal)
	if err := d.Ack(); err != nil {
		t.Fatalf("acknowledging a: %v", err)
	}
	// Acknowledging the same delivery twice is a channel error: the broker closes the channel
	// (PRECONDITION_FAILED, unknown delivery tag). The close arrives asynchronously, and until it does
	// the client still sends on the channel, so wait for it before publishing b; otherwise b could
	// arrive and be "acknowledged" on the closing channel without the consumer ever opening a new one.
	_ = d.Ack()
	waitFor(t, "the broker closes the channel", func() bool { return errors.Is(d.Ack(), queue.ErrChannelClosed) })

	// From here on b can only be consumed on a channel the consumer opened after the error. Usually
	// that is a new channel on the same connection. But the broker may still deliver b on the channel
	// it is closing, and amqp091-go closes the whole connection when a message arrives on a channel it
	// has already closed; b then comes again, marked redelivered, once the supervisor has reconnected.
	// Either way the consumer must pick up again.
	send(t, ch, domain.ChannelSMS, domain.PriorityNormal, "b", "")
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := next(t, c, domain.PriorityNormal)
		if got.NotificationID != "b" {
			t.Fatalf("got %s, want b", got.NotificationID)
		}
		err := got.Ack()
		if err == nil {
			break
		}
		if !errors.Is(err, queue.ErrChannelClosed) || time.Now().After(deadline) {
			t.Fatalf("acknowledging b: %v", err)
		}
	}
}

func TestConsumerResumesAfterTheBrokerCancelsOneOfItsConsumers(t *testing.T) {
	c, ch := consuming(t, 5)
	send(t, ch, domain.ChannelSMS, domain.PriorityLow, "before", "")
	if err := next(t, c, domain.PriorityLow).Ack(); err != nil {
		t.Fatal(err)
	}

	// Deleting a queue makes the broker cancel its consumers. Starting a consumer on the missing queue
	// fails until the queue is declared again.
	if _, err := ch.QueueDelete("sms.low", false, false, false); err != nil {
		t.Fatalf("deleting sms.low: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := queue.Declare(t.Context(), ch); err != nil {
		t.Fatalf("declaring the topology again: %v", err)
	}

	send(t, ch, domain.ChannelSMS, domain.PriorityLow, "after", "")
	send(t, ch, domain.ChannelSMS, domain.PriorityHigh, "high", "")
	if d := next(t, c, domain.PriorityLow); d.NotificationID != "after" {
		t.Errorf("low buffer yielded %s, want after", d.NotificationID)
	} else if err := d.Ack(); err != nil {
		t.Errorf("acknowledging after: %v", err)
	}
	if d := next(t, c, domain.PriorityHigh); d.NotificationID != "high" {
		t.Errorf("high buffer yielded %s, want high", d.NotificationID)
	}
}

func TestConsumerReturnsUnacknowledgedMessagesWhenItsContextEnds(t *testing.T) {
	url := mq.NewVHost(t)
	ch := topology(t, url)
	s, _, _ := supervise(t, url)
	c, stop, done := startConsumer(t, s, 5)

	send(t, ch, domain.ChannelSMS, domain.PriorityHigh, "a", "")
	d := next(t, c, domain.PriorityHigh)

	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
	waitFor(t, "a is back in sms.high", func() bool { return readyCount(t, ch, "sms.high") == 1 })
	if err := d.Ack(); !errors.Is(err, queue.ErrChannelClosed) {
		t.Errorf("acknowledging after the consumer stopped: %v, want ErrChannelClosed", err)
	}
}
