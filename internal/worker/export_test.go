package worker

import "time"

// WithJitter returns p drawing its random delays from jitter, which receives the backoff window.
func WithJitter(p RetryPolicy, jitter func(window time.Duration) time.Duration) RetryPolicy {
	p.jitter = jitter
	return p
}
