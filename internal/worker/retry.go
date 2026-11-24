package worker

import (
	"time"

	"github.com/muzaffercanan/gogogo/internal/platform/backoff"
)

// RetryPolicy decides how long a delivery that failed with a retryable error waits before its next
// attempt (ADR 0009, ADR 0014). Whether there is a next attempt is the notification's own limit
// (max_attempts).
type RetryPolicy struct {
	// Base is the longest delay after the first attempt; the window doubles with each further attempt.
	Base time.Duration
	// Cap bounds the backoff window.
	Cap time.Duration
	// MaxRetryAfter bounds how much of a provider's Retry-After is honoured.
	MaxRetryAfter time.Duration

	jitter func(window time.Duration) time.Duration // rand(0, window); replaced in tests
}

// DefaultRetryPolicy returns base 1 s, cap 60 s, Retry-After honoured up to one hour.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Base: time.Second, Cap: time.Minute, MaxRetryAfter: time.Hour}
}

// Delay returns the wait before the next attempt of a delivery that has failed attempts times so far
// (1 after the first attempt): a random delay in [0, min(Cap, Base·2^(attempts-1))], raised to the
// provider's retryAfter (0 when it gave none) clamped to MaxRetryAfter.
func (p RetryPolicy) Delay(attempts int, retryAfter time.Duration) time.Duration {
	window := backoff.Window(attempts-1, p.Base, p.Cap)
	var d time.Duration
	if p.jitter != nil {
		d = p.jitter(window)
	} else {
		d = backoff.FullJitter(attempts-1, p.Base, p.Cap)
	}
	return max(d, min(retryAfter, p.MaxRetryAfter))
}
