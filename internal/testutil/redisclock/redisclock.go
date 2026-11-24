// Package redisclock measures how the clock of a Redis server runs against the host's, for tests that
// compare times Redis hands out with times the host measures. On Docker Desktop Redis runs in a VM
// whose clock can run a few percent fast and is stepped back and forth now and then.
package redisclock

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reading is one reading of the Redis server's clock and the host's monotonic clock just before and
// just after it.
type Reading struct {
	Before, After time.Time // host
	Redis         time.Time
}

func mid(r Reading) time.Time { return r.Before.Add(r.After.Sub(r.Before) / 2) }

// Watch reads the clock of the Redis server at url every 20 ms until ctx ends. The returned function
// waits for that and reports how fast the clock ran against the host's (see Rates), with the readings.
func Watch(ctx context.Context, t testing.TB, url string) func() (fastest, reached float64, readings []Reading) {
	t.Helper()
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	var readings []Reading
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			before := time.Now()
			now, err := client.Time(ctx).Result()
			after := time.Now()
			if err == nil {
				readings = append(readings, Reading{Before: before, After: after, Redis: now})
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	return func() (float64, float64, []Reading) {
		<-done
		fastest, reached, ok := Rates(readings)
		if !ok {
			t.Fatalf("could not tell how fast the Redis clock ran from %d readings", len(readings))
		}
		return fastest, reached, readings
	}
}

// Rates reports how fast the Redis clock ran against the host's: at its fastest over any second of
// host time, and how far it got past its first reading per unit of host time from the first reading to
// the last.
//
// The fastest rate is an upper bound: each Redis reading happened somewhere between its Before and
// After, so it divides by the shortest host time the two readings can be apart. A backward step (the
// server's clock set back) only slows the windows across it, while a forward step shows up as a fast
// window.
//
// A rate limiter that hands out slots by the Redis clock can only hand out the slots up to the latest
// Redis time reached: after a backward step it waits until the clock is back there. So reached, not the
// plain average, scales the rate such a limiter gives over the readings.
func Rates(readings []Reading) (fastest, reached float64, ok bool) {
	if len(readings) < 2 {
		return 0, 0, false
	}
	first, last := readings[0], readings[len(readings)-1]
	latest := first.Redis
	for _, r := range readings {
		if r.Redis.After(latest) {
			latest = r.Redis
		}
	}
	reached = float64(latest.Sub(first.Redis)) / float64(mid(last).Sub(mid(first)))
	for i := range readings {
		for j := i + 1; j < len(readings); j++ {
			if span := readings[j].Before.Sub(readings[i].After); span >= time.Second {
				fastest = math.Max(fastest, float64(readings[j].Redis.Sub(readings[i].Redis))/float64(span))
				ok = true
				break
			}
		}
	}
	return fastest, reached, ok
}

// Summary describes the readings for a failure message, with host times given from origin: how far
// each clock went from the first reading to the last, every step of the Redis clock against the host's
// between two readings (one that its reading times cannot explain by more than 20 ms), the longest gap
// between readings and the slowest call.
func Summary(origin time.Time, readings []Reading) string {
	if len(readings) == 0 {
		return "no readings of the Redis clock"
	}
	at := func(t time.Time) time.Duration { return t.Sub(origin).Round(time.Millisecond) }
	first, last := readings[0], readings[len(readings)-1]
	var (
		steps            []string
		gap, slowest     time.Duration
		gapAt, slowestAt time.Time
	)
	for i, r := range readings {
		if d := r.After.Sub(r.Before); d > slowest {
			slowest, slowestAt = d, r.Before
		}
		if i == 0 {
			continue
		}
		p := readings[i-1]
		if g := r.Before.Sub(p.After); g > gap {
			gap, gapAt = g, p.After
		}
		// The two Redis readings were taken between r.Before - p.After and r.After - p.Before apart.
		moved, slack := r.Redis.Sub(p.Redis), 20*time.Millisecond
		if moved < r.Before.Sub(p.After)-slack || moved > r.After.Sub(p.Before)+slack {
			step, sign := (moved - mid(r).Sub(mid(p))).Round(time.Millisecond), ""
			if step > 0 {
				sign = "+"
			}
			steps = append(steps, fmt.Sprintf("%s%v at +%v", sign, step, at(mid(p))))
		}
	}
	if len(steps) == 0 {
		steps = []string{"none"}
	}
	return fmt.Sprintf("%d readings from +%v to +%v of host time; the Redis clock went %v in %v; steps: %s; longest gap between readings %v at +%v; slowest reading %v at +%v",
		len(readings), at(mid(first)), at(mid(last)), last.Redis.Sub(first.Redis).Round(time.Millisecond),
		mid(last).Sub(mid(first)).Round(time.Millisecond), strings.Join(steps, ", "),
		gap.Round(time.Millisecond), at(gapAt), slowest.Round(time.Millisecond), at(slowestAt))
}
