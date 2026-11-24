//go:build integration

package queue_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/queue"
)

// proxy forwards TCP connections to the broker. Switched off, it drops every open connection and
// refuses new ones, which is how a broker restart or a network cut looks to a client.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	up    bool
	conns []net.Conn
	live  atomic.Int64 // client connections not yet closed by either side
}

// newProxy starts a proxy in front of the broker of brokerURL and returns it with the URL that
// reaches the same virtual host through it.
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
		p.live.Add(1)
		p.mu.Unlock()
		go func() {
			pipe(broker, client)
			p.live.Add(-1)
		}()
		go pipe(client, broker)
	}
}

func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

// set switches the proxy on or off; switching it off closes every connection through it.
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

// supervise starts a supervisor for url with a short backoff and stops it when the test ends. The
// returned channel is closed when Run returns.
func supervise(t *testing.T, url string) (*queue.Supervisor, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return superviseWithBackoff(t, url, 10*time.Millisecond, 200*time.Millisecond)
}

func superviseWithBackoff(t *testing.T, url string, minBackoff, maxBackoff time.Duration) (*queue.Supervisor, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	s := queue.NewSupervisor(url, t.Name(), slog.New(slog.NewTextHandler(t.Output(), nil)))
	queue.SetBackoff(s, minBackoff, maxBackoff)
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
			t.Error("Run did not return after its context was cancelled")
		}
	})
	return s, cancel, done
}

// waitFor polls cond for up to ten seconds.
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

func ready(s *queue.Supervisor) func() bool {
	return func() bool { return s.Check(context.Background()) == nil }
}

func notReady(s *queue.Supervisor) func() bool {
	return func() bool { return errors.Is(s.Check(context.Background()), queue.ErrNotConnected) }
}

// queueExists reports whether name exists in the virtual host of a channel from s.
func queueExists(t *testing.T, s *queue.Supervisor, name string) bool {
	t.Helper()
	ch, err := s.Channel()
	if err != nil {
		t.Fatalf("opening a channel: %v", err)
	}
	defer ch.Close()
	_, err = ch.QueueDeclarePassive(name, true, false, false, false, nil)
	return err == nil
}

func TestSupervisorConnectsAndDeclaresTheTopology(t *testing.T) {
	s, _, _ := supervise(t, mq.NewVHost(t))

	waitFor(t, "the supervisor is ready", ready(s))

	for _, name := range allQueues() {
		if !queueExists(t, s, name) {
			t.Errorf("queue %s does not exist after connecting", name)
		}
	}
}

// The broker being down when the process starts must not be fatal.
func TestSupervisorWaitsForABrokerThatIsDownAtStart(t *testing.T) {
	p, url := newProxy(t, mq.NewVHost(t), false)
	s, _, _ := supervise(t, url)

	time.Sleep(500 * time.Millisecond) // several failed attempts at this backoff
	if err := s.Check(context.Background()); !errors.Is(err, queue.ErrNotConnected) {
		t.Fatalf("Check while the broker is down = %v, want ErrNotConnected", err)
	}
	if _, err := s.Channel(); !errors.Is(err, queue.ErrNotConnected) {
		t.Fatalf("Channel while the broker is down = %v, want ErrNotConnected", err)
	}

	p.set(true)
	waitFor(t, "the supervisor connects once the broker is up", ready(s))
}

func TestSupervisorReconnectsAndRedeclaresTheTopology(t *testing.T) {
	vhost := mq.NewVHost(t)
	p, url := newProxy(t, vhost, true)
	s, _, _ := supervise(t, url)
	waitFor(t, "the supervisor is ready", ready(s))

	// Delete a queue behind the supervisor's back, then cut its connection.
	name := queue.QueueName("push", "normal")
	if _, err := openChannel(t, dial(t, vhost)).QueueDelete(name, false, false, false); err != nil {
		t.Fatalf("deleting %s: %v", name, err)
	}
	p.set(false)
	waitFor(t, "the supervisor notices the lost connection", notReady(s))

	p.set(true)
	waitFor(t, "the supervisor reconnects", ready(s))
	if !queueExists(t, s, name) {
		t.Errorf("queue %s was not declared again after reconnecting", name)
	}
}

func TestSupervisorRetriesWhileTheTopologyCannotBeDeclared(t *testing.T) {
	vhost := mq.NewVHost(t)
	admin := openChannel(t, dial(t, vhost))
	name := queue.QueueName("email", "low")
	if _, err := admin.QueueDeclare(name, true, false, false, false, nil); err != nil {
		t.Fatalf("declaring a classic %s: %v", name, err)
	}
	s, _, _ := supervise(t, vhost)

	time.Sleep(500 * time.Millisecond)
	if err := s.Check(context.Background()); !errors.Is(err, queue.ErrNotConnected) {
		t.Fatalf("Check while %s conflicts = %v, want ErrNotConnected", name, err)
	}

	if _, err := admin.QueueDelete(name, false, false, false); err != nil {
		t.Fatalf("deleting the conflicting %s: %v", name, err)
	}
	waitFor(t, "the supervisor declares the topology once the conflict is gone", ready(s))
}

func TestSupervisorStopsWithItsContext(t *testing.T) {
	t.Run("while connected", func(t *testing.T) {
		p, url := newProxy(t, mq.NewVHost(t), true)
		s, cancel, done := supervise(t, url)
		waitFor(t, "the supervisor is ready", ready(s))

		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after its context was cancelled")
		}
		if _, err := s.Channel(); !errors.Is(err, queue.ErrNotConnected) {
			t.Errorf("Channel after Run returned = %v, want ErrNotConnected", err)
		}
		waitFor(t, "the supervisor's connection is closed", func() bool { return p.live.Load() == 0 })
	})

	t.Run("while backing off", func(t *testing.T) {
		_, url := newProxy(t, mq.NewVHost(t), false)
		_, cancel, done := superviseWithBackoff(t, url, time.Hour, time.Hour)
		time.Sleep(300 * time.Millisecond) // the first attempt fails at once; the backoff is all but certain to exceed this

		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after its context was cancelled during a backoff")
		}
	})
}
