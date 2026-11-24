package redisclock_test

import (
	"strings"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/testutil/redisclock"
)

func TestRates(t *testing.T) {
	host := time.Now()
	redis := time.Unix(1_700_000_000, 0)
	// readings every 20 ms over 3 s of host time, the call taking 1 ms; the Redis clock runs at rate
	// and is set back by step at stepAt.
	readings := func(rate float64, stepAt, step time.Duration) []redisclock.Reading {
		var out []redisclock.Reading
		for at := time.Duration(0); at <= 3*time.Second; at += 20 * time.Millisecond {
			r := redis.Add(time.Duration(float64(at+500*time.Microsecond) * rate))
			if step != 0 && at >= stepAt {
				r = r.Add(-step)
			}
			out = append(out, redisclock.Reading{Before: host.Add(at), After: host.Add(at + time.Millisecond), Redis: r})
		}
		return out
	}
	tests := []struct {
		name                   string
		readings               []redisclock.Reading
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
			fastest, reached, ok := redisclock.Rates(tt.readings)
			if !ok {
				t.Fatal("Rates found no second without a backward step")
			}
			if fastest < tt.fastestMin || fastest > tt.fastestMax {
				t.Errorf("fastest = %.4f, want %.4f to %.4f", fastest, tt.fastestMin, tt.fastestMax)
			}
			if reached < tt.reachedMin || reached > tt.reachedMax {
				t.Errorf("reached = %.4f, want %.4f to %.4f", reached, tt.reachedMin, tt.reachedMax)
			}
		})
	}

	if _, _, ok := redisclock.Rates(readings(1, 0, 0)[:30]); ok {
		t.Error("Rates reported a rate from readings less than a second apart")
	}
}

func TestSummary(t *testing.T) {
	host := time.Now()
	redis := time.Unix(1_700_000_000, 0)
	// Readings every 20 ms over 3 s, each call taking 2 ms; the Redis clock is set back by 850 ms at
	// 1.5 s and jumps 300 ms ahead at 2.5 s, and the reading at 2 s takes 100 ms and is followed by
	// a 200 ms gap.
	var readings []redisclock.Reading
	for at := time.Duration(0); at <= 3*time.Second; at += 20 * time.Millisecond {
		r := redis.Add(at + time.Millisecond)
		if at >= 1500*time.Millisecond {
			r = r.Add(-850 * time.Millisecond)
		}
		if at >= 2500*time.Millisecond {
			r = r.Add(300 * time.Millisecond)
		}
		call := 2 * time.Millisecond
		switch {
		case at == 2*time.Second:
			call = 100 * time.Millisecond
		case at > 2*time.Second && at < 2300*time.Millisecond:
			continue
		}
		readings = append(readings, redisclock.Reading{Before: host.Add(at), After: host.Add(at + call), Redis: r})
	}

	got := redisclock.Summary(host, readings)
	for _, want := range []string{
		"steps: -850ms at +1.481s, +300ms at +2.481s;",
		"longest gap between readings 200ms at +2.1s",
		"slowest reading 100ms at +2s",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Summary = %q, want it to contain %q", got, want)
		}
	}

	// A slow reading alone is not a step, nor is a long gap.
	if got := redisclock.Summary(host, readings[:70]); !strings.Contains(got, "steps: none;") {
		t.Errorf("Summary of readings without a step = %q, want no steps", got)
	}
	if got := redisclock.Summary(host, nil); got != "no readings of the Redis clock" {
		t.Errorf("Summary(nil) = %q", got)
	}
}
