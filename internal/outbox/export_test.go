package outbox

import "time"

// SetTiming changes the relay's batch size, poll interval and error backoff window so tests do not
// wait for production delays.
func SetTiming(r *Relay, batchSize int, pollInterval, minBackoff, maxBackoff time.Duration) {
	r.batchSize = batchSize
	r.pollInterval = pollInterval
	r.minBackoff = minBackoff
	r.maxBackoff = maxBackoff
}

// Backoff exposes the backoff calculation to tests.
func Backoff(r *Relay, failures int) time.Duration { return r.backoff(failures) }

// SetReaperTiming changes the reaper's batch size, lease and scan interval so tests do not wait for
// production delays.
func SetReaperTiming(r *Reaper, batchSize int, lease, interval time.Duration) {
	r.batchSize = batchSize
	r.lease = lease
	r.interval = interval
}
