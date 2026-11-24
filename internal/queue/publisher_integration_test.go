//go:build integration

package queue_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// connectedPublisher returns a publisher on a fresh virtual host, once its supervisor is ready, and
// the URL of that virtual host.
func connectedPublisher(t *testing.T) (*queue.Publisher, string) {
	t.Helper()
	vhost := mq.NewVHost(t)
	s, _, _ := supervise(t, vhost)
	waitFor(t, "the supervisor is ready", ready(s))
	return queue.NewPublisher(s), vhost
}

func message(c domain.Channel, p domain.Priority) queue.Message {
	name := queue.QueueName(c, p)
	return queue.Message{
		Channel:  c,
		Priority: p,
		ID:       "id-" + name,
		Body:     []byte(`{"queue":"` + name + `"}`),
		Headers:  amqp.Table{"correlation_id": "corr-" + name},
	}
}

func TestPublishRoutesConfirmedMessagesToTheirQueues(t *testing.T) {
	pub, vhost := connectedPublisher(t)
	var msgs []queue.Message
	for _, c := range queue.Channels() {
		for _, p := range queue.Priorities() {
			msgs = append(msgs, message(c, p))
		}
	}

	if err := pub.Publish(t.Context(), msgs); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}

	ch := openChannel(t, dial(t, vhost))
	for _, want := range msgs {
		name := queue.QueueName(want.Channel, want.Priority)
		d := receive(t, ch, name)
		if d.MessageId != want.ID || string(d.Body) != string(want.Body) || d.Headers["correlation_id"] != want.Headers["correlation_id"] {
			t.Errorf("%s received id %q body %s headers %v, want id %q body %s headers %v",
				name, d.MessageId, d.Body, d.Headers, want.ID, want.Body, want.Headers)
		}
		if d.DeliveryMode != amqp.Persistent || d.ContentType != "application/json" {
			t.Errorf("%s received delivery mode %d, content type %q; want persistent JSON", name, d.DeliveryMode, d.ContentType)
		}
		assertEmpty(t, ch, name)
	}
}

func TestPublishFailsWhenAMessageCannotBeRouted(t *testing.T) {
	pub, vhost := connectedPublisher(t)
	gone := message(domain.ChannelPush, domain.PriorityLow)
	name := queue.QueueName(gone.Channel, gone.Priority)
	if _, err := openChannel(t, dial(t, vhost)).QueueDelete(name, false, false, false); err != nil {
		t.Fatalf("deleting %s: %v", name, err)
	}

	err := pub.Publish(t.Context(), []queue.Message{message(domain.ChannelSMS, domain.PriorityHigh), gone})

	// The broker confirms an unroutable message too; only the mandatory flag's return reveals it.
	if err == nil || !strings.Contains(err.Error(), gone.ID) || !strings.Contains(err.Error(), "not routed") {
		t.Errorf("Publish with %s deleted = %v, want an error saying %s was not routed", name, err, gone.ID)
	}
}

func TestPublishFailsWhenTheBrokerRejectsAMessage(t *testing.T) {
	pub, vhost := connectedPublisher(t)
	full := message(domain.ChannelEmail, domain.PriorityNormal)
	mq.SetPolicy(t, vhost, "full", `^email\.normal$`, `{"max-length":0,"overflow":"reject-publish"}`)

	// The policy takes effect asynchronously; until then the publish succeeds.
	var err error
	waitFor(t, "a publish to the full queue fails", func() bool {
		err = pub.Publish(t.Context(), []queue.Message{full})
		return err != nil
	})
	if !strings.Contains(err.Error(), "did not accept message "+full.ID) {
		t.Errorf("Publish to a full reject-publish queue = %v, want a nack for %s", err, full.ID)
	}
}

func TestPublishFailsAtOnceWithoutAConnection(t *testing.T) {
	_, url := newProxy(t, mq.NewVHost(t), false)
	s, _, _ := supervise(t, url)
	pub := queue.NewPublisher(s)

	start := time.Now()
	err := pub.Publish(t.Context(), []queue.Message{message(domain.ChannelSMS, domain.PriorityLow)})

	if !errors.Is(err, queue.ErrNotConnected) {
		t.Errorf("Publish while disconnected = %v, want ErrNotConnected", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Publish while disconnected took %v, want it to fail at once", elapsed)
	}
}

func TestPublishStopsWithItsContext(t *testing.T) {
	pub, _ := connectedPublisher(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := pub.Publish(ctx, []queue.Message{message(domain.ChannelSMS, domain.PriorityNormal)})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Publish with a cancelled context = %v, want context.Canceled", err)
	}
}
