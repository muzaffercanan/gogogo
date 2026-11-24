//go:build integration

package queue_test

import (
	"errors"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/testutil/rabbittest"
)

var mq *rabbittest.Server

func TestMain(m *testing.M) { os.Exit(rabbittest.Main(m, &mq)) }

// dial connects to url and closes the connection when the test ends.
func dial(t *testing.T, url string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("connecting to %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// openChannel opens a channel on conn.
func openChannel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening channel: %v", err)
	}
	return ch
}

// declared returns a channel on a fresh virtual host that already has the topology.
func declared(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn := dial(t, mq.NewVHost(t))
	ch := openChannel(t, conn)
	if err := queue.Declare(t.Context(), ch); err != nil {
		t.Fatalf("declaring topology: %v", err)
	}
	return conn, ch
}

// publish sends body to the notifications exchange with the routing key.
func publish(t *testing.T, ch *amqp.Channel, key, body string) {
	t.Helper()
	if err := ch.PublishWithContext(t.Context(), queue.Exchange, key, false, false, amqp.Publishing{Body: []byte(body)}); err != nil {
		t.Fatalf("publishing to %s: %v", key, err)
	}
}

// receive waits up to five seconds for a message in queueName and returns it unacknowledged. Routing
// and dead-lettering happen asynchronously, so a message may arrive shortly after the call that sent it.
func receive(t *testing.T, ch *amqp.Channel, queueName string) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d, ok, err := ch.Get(queueName, false)
		if err != nil {
			t.Fatalf("getting from %s: %v", queueName, err)
		}
		if ok {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message arrived in %s", queueName)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// assertEmpty fails the test if queueName holds a message.
func assertEmpty(t *testing.T, ch *amqp.Channel, queueName string) {
	t.Helper()
	d, ok, err := ch.Get(queueName, false)
	if err != nil {
		t.Fatalf("getting from %s: %v", queueName, err)
	}
	if ok {
		t.Errorf("%s holds %q, want it empty", queueName, d.Body)
	}
}

// amqpCode returns the AMQP reply code of err, or 0 if err is not a broker error.
func amqpCode(err error) int {
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		return amqpErr.Code
	}
	return 0
}
