//go:build integration

package ratelimit_test

import (
	"context"
	"math"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/testutil/redisclock"
	"github.com/muzaffercanan/gogogo/internal/testutil/redistest"
)

var rdb *redistest.Server

func TestMain(m *testing.M) { os.Exit(redistest.Main(m, &rdb)) }

func TestLimiterStartsAtMostTheRatePerRollingSecondAcrossInstances(t *testing.T) {
	const (
		rate      = 100
		instances = 4
		run       = 2500 * time.Millisecond
	)
	url := rdb.NewURL(t)

	// Each instance is a worker process with its own Redis client, all limiting the same channel. It
	// reserves a slot, sleeps until it as Wait does, and records when it woke up and how late that was:
	// the time from the call that reserved the slot to waking, less the wait. The slot itself lies
	// between the call's start and end plus the wait, so a send starts at most that lateness after its
	// slot.
	var (
		mu       sync.Mutex
		starts   []time.Time
		lateness time.Duration // the most any send started after its slot
		wg       sync.WaitGroup
	)
	// Redis spaces the slots by its own clock, while the starts are timed by the host's. On Docker
	// Desktop Redis runs in a VM whose clock can run a few percent fast and is set back now and then, so
	// the test watches how fast it runs.
	ctx, cancel := context.WithTimeout(context.Background(), run)
	defer cancel()
	origin := time.Now()
	clock := redisclock.Watch(ctx, t, url)
	for range instances {
		opts, err := redis.ParseURL(url)
		if err != nil {
			t.Fatal(err)
		}
		client := redis.NewClient(opts)
		t.Cleanup(func() { _ = client.Close() })
		l := newLimiter(t, client, rate)
		wg.Go(func() {
			for {
				called := time.Now()
				wait, err := l.Reserve(ctx, domain.ChannelSMS)
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("Reserve: %v", err)
					}
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
				woke := time.Now()
				mu.Lock()
				starts = append(starts, woke)
				lateness = max(lateness, woke.Sub(called)-wait)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	fastest, reached, readings := clock()
	defer func() {
		// The rates above summarise the clock; when they do not explain the sends, the details might.
		if t.Failed() {
			t.Logf("Redis clock: %s", redisclock.Summary(origin, readings))
			t.Logf("sends per 250 ms from the start: %v", perQuarterSecond(origin, starts, run))
		}
	}()

	slices.SortFunc(starts, func(a, b time.Time) int { return a.Compare(b) })
	most := 0
	for i := range starts {
		j := i
		for j < len(starts) && starts[j].Sub(starts[i]) < time.Second {
			j++
		}
		most = max(most, j-i)
	}
	perSecond := float64(len(starts)) / run.Seconds()
	interval := time.Second / rate
	allowed := int(math.Ceil(float64(time.Second+lateness) * fastest / float64(interval)))
	t.Logf("%d instances started %d sends in %v (%.1f/s); most in any rolling second: %d; latest start %v after its slot; Redis clock at most %.4f times as fast as the host's, and %.4f times as far",
		instances, len(starts), run, perSecond, most, lateness, fastest, reached)

	// Redis hands out slots exactly one interval apart by its clock, which ran at most fastest times as
	// fast as the host's in any second (setting it back only spaces slots out), so in host time they are
	// at least interval / fastest apart. Sends that start up to the measured lateness after their slots
	// can put the slots of 1 s plus that lateness into one second of starts.
	if most > allowed {
		t.Errorf("%d sends started within one second, want at most %d (%d per second by a Redis clock running %.4f times as fast, plus %v of start lateness)",
			most, allowed, rate, fastest, lateness)
	}
	// The instances share one limit: together they get the rate by the Redis clock, not a multiple of
	// it, and the limiter does not hold them back much below it.
	if want := rate * reached; perSecond < 0.9*want || perSecond > 1.05*want {
		t.Errorf("%d instances together sent %.1f per second, want about %.1f (%d per second by a Redis clock that got %.4f times as far as the host's)",
			instances, perSecond, want, rate, reached)
	}
}

// perQuarterSecond counts the starts in each 250 ms from origin over run.
func perQuarterSecond(origin time.Time, starts []time.Time, run time.Duration) []int {
	counts := make([]int, int(run/(250*time.Millisecond))+1)
	for _, s := range starts {
		if i := int(s.Sub(origin) / (250 * time.Millisecond)); i >= 0 && i < len(counts) {
			counts[i]++
		}
	}
	return counts
}

func TestScriptRunsOnRedisWithTheSameResultsAsOnMiniredis(t *testing.T) {
	// One send per second, so no slot passes while the calls run, however slow Redis is.
	client := rdb.NewClient(t)
	l := newLimiter(t, client, 1)

	type reservation struct {
		start, end time.Time
		wait       time.Duration
	}
	var rs []reservation
	for range 3 {
		start := time.Now()
		wait := reserve(t, l, domain.ChannelEmail)
		rs = append(rs, reservation{start: start, end: time.Now(), wait: wait})
	}

	if rs[0].wait != 0 {
		t.Errorf("first reservation waits %v, want 0", rs[0].wait)
	}
	// Consecutive slots are exactly one second apart on the Redis clock, so between two calls the wait
	// grows by one second less the Redis time that passed. That time lies between the gap separating
	// the calls and the span covering both (with a millisecond for rounding to microseconds).
	for i := 1; i < len(rs); i++ {
		passed := time.Second - (rs[i].wait - rs[i-1].wait)
		lo := rs[i].start.Sub(rs[i-1].end) - time.Millisecond
		hi := rs[i].end.Sub(rs[i-1].start) + time.Millisecond
		if passed < lo || passed > hi {
			t.Errorf("reservation %d waits %v after %v: %v of Redis time passed between the calls, want within [%v, %v]",
				i+1, rs[i].wait, rs[i-1].wait, passed, lo, hi)
		}
	}

	// Redis's Lua rounds numbers to 14 digits when it turns them into strings itself (tostring), which
	// would cost the microsecond timestamps their last two digits. The script hands them to redis.call,
	// which writes every digit.
	stored, err := client.Get(t.Context(), "ratelimit:email").Result()
	if err != nil {
		t.Fatalf("reading the theoretical arrival time: %v", err)
	}
	if _, err := strconv.ParseInt(stored, 10, 64); err != nil {
		t.Errorf("ratelimit:email holds %q, want an integer number of microseconds", stored)
	}
}
