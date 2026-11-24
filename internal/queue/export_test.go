package queue

import "time"

// SetBackoff changes the reconnect backoff window so tests do not wait for production delays.
func SetBackoff(s *Supervisor, minBackoff, maxBackoff time.Duration) {
	s.minBackoff, s.maxBackoff = minBackoff, maxBackoff
}

// Backoff exposes the backoff calculation to tests.
func Backoff(s *Supervisor, failures int) time.Duration { return s.backoff(failures) }
