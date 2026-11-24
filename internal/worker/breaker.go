package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

// BreakerSettings tune a channel's circuit breaker (ADR 0009, K-42).
type BreakerSettings struct {
	// Window is how far back the breaker counts deliveries, in a rolling window of ten buckets.
	Window time.Duration
	// MinRequests is how many deliveries the window must hold before the breaker can open.
	MinRequests uint32
	// FailureRatio opens the breaker when retryable failures make up more than this share of them.
	FailureRatio float64
	// OpenFor is how long the breaker stays open before it lets one probe through.
	OpenFor time.Duration
}

// DefaultBreakerSettings opens the breaker when more than half of at least 20 deliveries in the last
// 10 s failed with a retryable error, and keeps it open for 30 s.
func DefaultBreakerSettings() BreakerSettings {
	return BreakerSettings{Window: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenFor: 30 * time.Second}
}

func (s BreakerSettings) validate() error {
	if s.Window <= 0 || s.OpenFor <= 0 || s.MinRequests < 1 || s.FailureRatio <= 0 || s.FailureRatio > 1 {
		return fmt.Errorf("breaker settings %+v: window and open time must be positive, min requests at least 1 and the failure ratio in (0, 1]", s)
	}
	return nil
}

// Errors a delivery's outcome turns into for the breaker.
var (
	// errRetryableFailure is the provider failing in a way worth retrying: it counts against it.
	errRetryableFailure = errors.New("retryable provider failure")
	// errNotCounted is a delivery that never reached the provider: it counts neither way.
	errNotCounted = errors.New("delivery did not reach the provider")
)

// verdict tells the breaker what an outcome says about the provider. Only retryable failures count
// against it; a permanent refusal still proves it is up (ADR 0009).
func verdict(o Outcome) error {
	switch o {
	case OutcomeSent, OutcomeFailed:
		return nil
	case OutcomeRetry, OutcomeExhausted:
		return errRetryableFailure
	default: // skipped, rejected or abandoned: the provider was not called, or the result is unknown
		return errNotCounted
	}
}

// breaker guards the sends of one channel. While it is open the dispatcher picks nothing (K-42).
type breaker struct {
	cb  *gobreaker.TwoStepCircuitBreaker[struct{}]
	log *slog.Logger
}

// breakerPoll is how often a dispatcher waiting on an open breaker asks whether it may send again.
const breakerPoll = 250 * time.Millisecond

func newBreaker(c domain.Channel, s BreakerSettings, metrics *Metrics, log *slog.Logger) (*breaker, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	b := &breaker{log: log}
	b.cb = gobreaker.NewTwoStepCircuitBreaker[struct{}](gobreaker.Settings{
		Name:         string(c),
		MaxRequests:  1, // one probe while half-open
		Interval:     s.Window,
		BucketPeriod: s.Window / 10,
		Timeout:      s.OpenFor,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Requests also counts deliveries still in flight and ones that do not count; judge only
			// the ones with a verdict.
			decided := counts.TotalSuccesses + counts.TotalFailures
			return decided >= s.MinRequests && float64(counts.TotalFailures) > s.FailureRatio*float64(decided)
		},
		IsExcluded: func(err error) bool { return errors.Is(err, errNotCounted) },
		OnStateChange: func(_ string, from, to gobreaker.State) {
			metrics.breakerState(c, to)
			level := slog.LevelInfo
			if to == gobreaker.StateOpen {
				level = slog.LevelWarn
			}
			log.Log(context.Background(), level, "circuit breaker changed state", "from", from.String(), "to", to.String())
		},
	})
	return b, nil
}

// allow waits until the breaker lets a delivery through and returns the function that reports its
// outcome; every allowed delivery must report one. It reports false if ctx ended first.
func (b *breaker) allow(ctx context.Context) (func(Outcome), bool) {
	for {
		done, err := b.cb.Allow()
		if err == nil {
			return func(o Outcome) { done(verdict(o)) }, true
		}
		// Open, or half-open with its probe in flight: hold on to what is picked and wait.
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(breakerPoll):
		}
	}
}
