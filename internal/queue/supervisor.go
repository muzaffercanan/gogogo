package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrNotConnected is returned while the supervisor has no open connection to RabbitMQ.
var ErrNotConnected = errors.New("not connected to rabbitmq")

// dialTimeout bounds the TCP connect and the AMQP handshake of one attempt.
const dialTimeout = 10 * time.Second

// Supervisor keeps one connection to RabbitMQ open with the topology declared. amqp091-go does not
// reconnect by itself unless its experimental recovery is enabled, which this service does not use
// (ADR 0013), so Run dials, declares, waits for the connection to close and starts over, backing off
// between failed attempts, until its context ends. Components open channels on the current connection
// with Channel and treat ErrNotConnected as a transient failure.
type Supervisor struct {
	url  string
	name string // shown as the connection name in the management UI
	log  *slog.Logger

	minBackoff, maxBackoff time.Duration

	mu   sync.Mutex
	conn *amqp.Connection // nil while disconnected
}

// NewSupervisor returns a supervisor for the broker at url. name identifies the process (relay,
// worker) in the broker's connection list.
func NewSupervisor(url, name string, log *slog.Logger) *Supervisor {
	return &Supervisor{
		url:        url,
		name:       name,
		log:        log,
		minBackoff: 500 * time.Millisecond,
		maxBackoff: 15 * time.Second,
	}
}

// Run connects and keeps reconnecting until ctx ends, then closes the connection. A failed attempt
// (dial or declare) is retried after rand(0, min(max, min·2^failures)), without giving up; this
// includes the first attempt, so the process does not depend on the broker being up before it.
func (s *Supervisor) Run(ctx context.Context) {
	failures := 0
	for {
		conn, err := s.connect(ctx)
		if err != nil {
			delay := s.backoff(failures)
			failures++
			s.log.Warn("rabbitmq connection attempt failed", "attempt", failures, "retry_in_ms", delay.Milliseconds(), "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		failures = 0

		closed := conn.NotifyClose(make(chan *amqp.Error, 1))
		s.setConn(conn)
		s.log.Info("connected to rabbitmq")

		select {
		case <-ctx.Done():
			s.setConn(nil)
			if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				s.log.Warn("closing rabbitmq connection", "error", err)
			}
			return
		case err := <-closed:
			s.setConn(nil)
			s.log.Warn("rabbitmq connection lost", "error", err)
		}
	}
}

// connect dials the broker and declares the topology on a channel of its own.
func (s *Supervisor) connect(ctx context.Context) (*amqp.Connection, error) {
	props := amqp.NewConnectionProperties()
	props.SetClientConnectionName(s.name)
	conn, err := amqp.DialConfig(s.url, amqp.Config{Properties: props, Dial: amqp.DefaultDial(dialTimeout)})
	if err != nil {
		return nil, fmt.Errorf("dialing: %w", err)
	}
	ch, err := conn.Channel()
	if err == nil {
		err = Declare(ctx, ch)
		// A failed declare has already closed the channel.
		_ = ch.Close()
	}
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("declaring topology: %w", err)
	}
	return conn, nil
}

// backoff returns the delay after the given number of consecutive failures.
func (s *Supervisor) backoff(failures int) time.Duration {
	return jitteredBackoff(failures, s.minBackoff, s.maxBackoff)
}

// jitteredBackoff returns rand(0, min(maxBackoff, minBackoff·2^failures)): full jitter over an
// exponentially growing, capped window.
func jitteredBackoff(failures int, minBackoff, maxBackoff time.Duration) time.Duration {
	window := maxBackoff
	if failures < 32 { // beyond that the shift overflows and the cap applies anyway
		window = min(maxBackoff, minBackoff<<failures)
	}
	return rand.N(window + 1) //nolint:gosec // G404: jitter needs no cryptographic randomness
}

func (s *Supervisor) setConn(conn *amqp.Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = conn
}

// Channel opens a channel on the current connection. It returns ErrNotConnected while there is none.
// The caller closes the channel; it stops working if the connection is lost.
func (s *Supervisor) Channel() (*amqp.Channel, error) {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return nil, ErrNotConnected
	}
	ch, err := conn.Channel()
	if errors.Is(err, amqp.ErrClosed) {
		return nil, ErrNotConnected
	}
	if err != nil {
		return nil, fmt.Errorf("opening channel: %w", err)
	}
	return ch, nil
}

// Check reports whether the supervisor is connected with the topology declared, for readiness.
func (s *Supervisor) Check(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || s.conn.IsClosed() {
		return ErrNotConnected
	}
	return nil
}
