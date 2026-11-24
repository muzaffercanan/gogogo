//go:build integration

package rabbittest_test

import (
	"errors"
	"os"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/testutil/rabbittest"
)

var mq *rabbittest.Server

func TestMain(m *testing.M) { os.Exit(rabbittest.Main(m, &mq)) }

func channel(t *testing.T, url string) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("connecting to %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening channel: %v", err)
	}
	return ch
}

func TestVHostsAreIsolated(t *testing.T) {
	first := channel(t, mq.NewVHost(t))
	second := channel(t, mq.NewVHost(t))

	if _, err := first.QueueDeclare("only-in-first", true, false, false, false, nil); err != nil {
		t.Fatalf("declaring a queue in the first virtual host: %v", err)
	}

	_, err := second.QueueDeclarePassive("only-in-first", true, false, false, false, nil)
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.NotFound {
		t.Errorf("passive declare in the second virtual host = %v, want 404 NOT_FOUND", err)
	}
}
