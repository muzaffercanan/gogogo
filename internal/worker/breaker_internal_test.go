package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sony/gobreaker/v2"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

func testBreaker(t *testing.T, openFor time.Duration) (*breaker, *Metrics) {
	t.Helper()
	m := NewMetrics(prometheus.NewRegistry())
	b, err := newBreaker(domain.ChannelSMS, BreakerSettings{Window: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenFor: openFor},
		m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return b, m
}

// record runs n deliveries with outcome o through b, which must let them through.
func record(t *testing.T, b *breaker, o Outcome, n int) {
	t.Helper()
	for range n {
		done, err := b.cb.Allow()
		if err != nil {
			t.Fatalf("the breaker refused a delivery: %v", err)
		}
		done(verdict(o))
	}
}

func TestBreakerOpensWhenMoreThanHalfOfEnoughDeliveriesFailRetryably(t *testing.T) {
	tests := []struct {
		name     string
		outcomes map[Outcome]int
		wantOpen bool
	}{
		{name: "19 retryable failures are too few", outcomes: map[Outcome]int{OutcomeRetry: 19}},
		{name: "20 retryable failures", outcomes: map[Outcome]int{OutcomeRetry: 20}, wantOpen: true},
		{name: "exhausted attempts count as retryable failures", outcomes: map[Outcome]int{OutcomeExhausted: 20}, wantOpen: true},
		{name: "half failing is not more than half", outcomes: map[Outcome]int{OutcomeSent: 10, OutcomeRetry: 10}},
		{name: "11 of 21 failing", outcomes: map[Outcome]int{OutcomeSent: 10, OutcomeRetry: 11}, wantOpen: true},
		{name: "permanent refusals show the provider is up", outcomes: map[Outcome]int{OutcomeFailed: 30, OutcomeRetry: 19}},
		{name: "deliveries that never reached the provider do not count",
			outcomes: map[Outcome]int{OutcomeSkipped: 10, OutcomeRejected: 10, OutcomeAbandoned: 10, OutcomeRetry: 19}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, _ := testBreaker(t, time.Minute)
			// Failures last, so that the breaker sees the whole mix before it decides.
			for _, o := range []Outcome{OutcomeSent, OutcomeFailed, OutcomeSkipped, OutcomeRejected, OutcomeAbandoned, OutcomeRetry, OutcomeExhausted} {
				record(t, b, o, tt.outcomes[o])
			}
			if open := b.cb.State() == gobreaker.StateOpen; open != tt.wantOpen {
				t.Errorf("breaker open = %t (%v), want %t", open, b.cb.State(), tt.wantOpen)
			}
		})
	}
}

func TestBreakerLetsOneProbeThroughAfterItsOpenTime(t *testing.T) {
	for _, tt := range []struct {
		probe     Outcome
		wantState gobreaker.State
	}{
		{probe: OutcomeSent, wantState: gobreaker.StateClosed},
		{probe: OutcomeFailed, wantState: gobreaker.StateClosed},
		{probe: OutcomeRetry, wantState: gobreaker.StateOpen},
		{probe: OutcomeSkipped, wantState: gobreaker.StateHalfOpen},
	} {
		t.Run(string(tt.probe), func(t *testing.T) {
			b, m := testBreaker(t, 50*time.Millisecond)
			record(t, b, OutcomeRetry, 20)
			if _, err := b.cb.Allow(); !errors.Is(err, gobreaker.ErrOpenState) {
				t.Fatalf("Allow right after opening = %v, want ErrOpenState", err)
			}
			if got := testutil.ToFloat64(m.breaker.WithLabelValues("sms")); got != 2 {
				t.Errorf("circuit_breaker_state while open = %v, want 2", got)
			}

			time.Sleep(60 * time.Millisecond)
			probe, err := b.cb.Allow()
			if err != nil {
				t.Fatalf("Allow after the open time = %v, want the probe let through", err)
			}
			if _, err := b.cb.Allow(); !errors.Is(err, gobreaker.ErrTooManyRequests) {
				t.Errorf("a second Allow while the probe is in flight = %v, want ErrTooManyRequests", err)
			}
			if got := testutil.ToFloat64(m.breaker.WithLabelValues("sms")); got != 1 {
				t.Errorf("circuit_breaker_state while half-open = %v, want 1", got)
			}
			probe(verdict(tt.probe))
			if got := b.cb.State(); got != tt.wantState {
				t.Errorf("after a %s probe the breaker is %v, want %v", tt.probe, got, tt.wantState)
			}
			if got := testutil.ToFloat64(m.breaker.WithLabelValues("sms")); got != float64(tt.wantState) {
				t.Errorf("circuit_breaker_state after the probe = %v, want %v", got, float64(tt.wantState))
			}
		})
	}
}

func TestBreakerAllowWaitsWhileOpenAndStopsWithItsContext(t *testing.T) {
	b, _ := testBreaker(t, time.Minute)
	record(t, b, OutcomeRetry, 20)

	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, ok := b.allow(ctx); ok {
		t.Fatal("allow let a delivery through an open breaker")
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Errorf("allow returned after %v, before its context ended", elapsed)
	}
}

func TestBreakerSettingsMustBeUsable(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, s := range []BreakerSettings{
		{Window: 0, MinRequests: 20, FailureRatio: 0.5, OpenFor: time.Second},
		{Window: time.Second, MinRequests: 0, FailureRatio: 0.5, OpenFor: time.Second},
		{Window: time.Second, MinRequests: 20, FailureRatio: 0, OpenFor: time.Second},
		{Window: time.Second, MinRequests: 20, FailureRatio: 1.5, OpenFor: time.Second},
		{Window: time.Second, MinRequests: 20, FailureRatio: 0.5, OpenFor: 0},
	} {
		if _, err := newBreaker(domain.ChannelSMS, s, m, log); err == nil {
			t.Errorf("newBreaker(%+v) succeeded, want an error", s)
		}
	}
	if _, err := newBreaker(domain.ChannelSMS, DefaultBreakerSettings(), m, log); err != nil {
		t.Errorf("newBreaker with the defaults: %v", err)
	}
}
