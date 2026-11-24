//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/testutil/pgtest"
	"github.com/muzaffercanan/gogogo/internal/testutil/rabbittest"
	"github.com/muzaffercanan/gogogo/internal/testutil/tcretry"
)

var (
	pg *pgtest.Server
	mq *rabbittest.Server
)

func TestMain(m *testing.M) { os.Exit(testMain(m)) }

func testMain(m *testing.M) int {
	ctx := context.Background()
	var err error
	pg, err = tcretry.Start(ctx, pgtest.Start)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		return 1
	}
	mq, err = tcretry.Start(ctx, rabbittest.Start)
	if err != nil {
		_ = pg.Close()
		fmt.Fprintf(os.Stderr, "rabbittest: %v\n", err)
		return 1
	}
	code := m.Run()
	if err := mq.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "rabbittest: %v\n", err)
	}
	if err := pg.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
	}
	return code
}

func dial(t *testing.T, url string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("connecting to %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func openChannel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening channel: %v", err)
	}
	return ch
}

func connectedPublisher(t *testing.T) (*queue.Publisher, string) {
	t.Helper()
	vhost := mq.NewVHost(t)
	s := supervise(t, vhost)
	waitFor(t, "the supervisor is ready", func() bool { return s.Check(context.Background()) == nil })
	return queue.NewPublisher(s), vhost
}

func supervise(t *testing.T, url string) *queue.Supervisor {
	t.Helper()
	s := queue.NewSupervisor(url, t.Name(), slog.New(slog.NewTextHandler(t.Output(), nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("supervisor Run did not return after its context was cancelled")
		}
	})
	return s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

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

// proxy forwards TCP connections to the broker and can be switched off to simulate an outage.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	up    bool
	conns []net.Conn
}

func newProxy(t *testing.T, brokerURL string, up bool) (*proxy, string) {
	t.Helper()
	u, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: u.Host, up: up}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.set(false)
	})
	u.Host = ln.Addr().String()
	return p, u.String()
}

func (p *proxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if !p.up {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		broker, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		p.conns = append(p.conns, client, broker)
		p.mu.Unlock()
		go pipe(broker, client)
		go pipe(client, broker)
	}
}

func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

func (p *proxy) set(up bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up = up
	if !up {
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
	}
}
