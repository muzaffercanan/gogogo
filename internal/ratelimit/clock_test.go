package ratelimit_test

import (
	"math"
	"testing"
	"time"
)

// clockReading is one reading of the Redis server's clock and the host's monotonic clock just before
// and just after it.
type clockReading struct {
	before, after time.Time // host
	redis         time.Time
}

// clockRates reports how fast the Redis clock ran against the host's: at its fastest over any second
// of host time, and how far it got past its first reading per unit of host time from the first reading
// to the last.
//
// The fastest rate is an upper bound: each Redis reading happened somewhere between its before and
// after, so it divides by the shortest host time the two readings can be apart. A backward step (the
// server's clock set back) only slows the windows across it, while a forward step shows up as a fast
// window.
//
// The limiter can only hand out the slots up to the latest Redis time reached: after a backward step
// it waits until the clock is back there. So reached, not the plain average, scales the rate the
// limiter gives over the readings.
func clockRates(readings []clockReading) (fastest, reached float64, ok bool) {
	if len(readings) < 2 {
		return 0, 0, false
	}
	first, last := readings[0], readings[len(readings)-1]
	latest := first.redis
	for _, r := range readings {
		if r.redis.After(latest) {
			latest = r.redis
		}
	}
	reached = float64(latest.Sub(first.redis)) / float64(mid(last).Sub(mid(first)))
	for i := range readings {
		for j := i + 1; j < len(readings); j++ {
			if span := readings[j].before.Sub(readings[i].after); span >= time.Second {
				fastest = math.Max(fastest, float64(readings[j].redis.Sub(readings[i].redis))/float64(span))
				ok = true
				break
			}
		}
	}
	return fastest, reached, ok
}

func mid(r clockReading) time.Time { return r.before.Add(r.after.Sub(r.before) / 2) }

func TestClockRates(t *testing.T) {
	host := time.Now()
	redis := time.Unix(1_700_000_000, 0)
	// readings every 20 ms over 3 s of host time, the call taking 1 ms; the Redis clock runs at rate
	// and is set back by step at stepAt.
	readings := func(rate float64, stepAt, step time.Duration) []clockReading {
		var out []clockReading
		for at := time.Duration(0); at <= 3*time.Second; at += 20 * time.Millisecond {
			r := redis.Add(time.Duration(float64(at+500*time.Microsecond) * rate))
			if step != 0 && at >= stepAt {
				r = r.Add(-step)
			}
			out = append(out, clockReading{before: host.Add(at), after: host.Add(at + time.Millisecond), redis: r})
		}
		return out
	}
	tests := []struct {
		name                   string
		readings               []clockReading
		fastestMin, fastestMax float64
		reachedMin, reachedMax float64
	}{
		// The fastest rates divide by the host time two readings can be apart at least: 1.02 s of
		// Redis time over 1.019 s.
		{"same rate", readings(1, 0, 0), 1.0005, 1.0015, 0.999, 1.001},
		{"3.5 % fast", readings(1.035, 0, 0), 1.0355, 1.0365, 1.034, 1.036},
		// The clock gets back past where it was before the step: (3 s * 1.035 - 850 ms) / 3 s.
		{"fast, set back by 850 ms", readings(1.035, 1500*time.Millisecond, 850*time.Millisecond), 1.0355, 1.0365, 0.75, 0.753},
		// The clock never gets back to where it was before the step: 2.88 s * 1.035 / 3 s.
		{"fast, set back just before the end", readings(1.035, 2900*time.Millisecond, 850*time.Millisecond), 1.0355, 1.0365, 0.993, 0.994},
		{"slow", readings(0.98, 0, 0), 0.9805, 0.9815, 0.979, 0.981},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fastest, reached, ok := clockRates(tt.readings)
			if !ok {
				t.Fatal("clockRates found no second without a backward step")
			}
			if fastest < tt.fastestMin || fastest > tt.fastestMax {
				t.Errorf("fastest = %.4f, want %.4f to %.4f", fastest, tt.fastestMin, tt.fastestMax)
			}
			if reached < tt.reachedMin || reached > tt.reachedMax {
				t.Errorf("reached = %.4f, want %.4f to %.4f", reached, tt.reachedMin, tt.reachedMax)
			}
		})
	}

	if _, _, ok := clockRates(readings(1, 0, 0)[:30]); ok {
		t.Error("clockRates reported a rate from readings less than a second apart")
	}
}
