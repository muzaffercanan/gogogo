// Package backoff computes exponential backoff with full jitter.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Window returns min(maxDelay, base·2^n): the longest delay after n earlier failures. A negative n
// counts as 0.
func Window(n int, base, maxDelay time.Duration) time.Duration {
	n = max(n, 0)
	// base·2^n > maxDelay exactly when base > maxDelay/2^n (rounded down); comparing that way never
	// computes a product that could overflow.
	if n >= 63 || base > maxDelay>>n {
		return maxDelay
	}
	return base << n
}

// FullJitter returns a random delay in [0, Window(n, base, maxDelay)].
func FullJitter(n int, base, maxDelay time.Duration) time.Duration {
	return rand.N(Window(n, base, maxDelay) + 1) //nolint:gosec // G404: jitter needs no cryptographic randomness
}
