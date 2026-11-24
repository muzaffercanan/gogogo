//go:build integration

package e2e_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/testutil/redisclock"
	"github.com/muzaffercanan/gogogo/internal/worker"
)

// timedLimiter records, for each token it hands out, when the worker asked for it and how long Redis
// made it wait: the slot lies at least that wait after the asking.
type timedLimiter struct {
	inner  worker.Limiter
	mu     sync.Mutex
	grants []grant
}

type grant struct {
	asked time.Time
	wait  time.Duration
}

func (l *timedLimiter) Wait(ctx context.Context, c domain.Channel) (time.Duration, error) {
	asked := time.Now()
	wait, err := l.inner.Wait(ctx, c)
	if err == nil {
		l.mu.Lock()
		l.grants = append(l.grants, grant{asked: asked, wait: wait})
		l.mu.Unlock()
	}
	return wait, err
}

func (l *timedLimiter) taken() []grant {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.grants)
}

func TestWorkersTogetherSendAtMostTheRatePerRollingSecond(t *testing.T) {
	const (
		rate    = 100
		workers = 4
		total   = 300
	)
	s := newSystem(t, nil)

	// Each worker sends one notification at a time and to a path of its own, so its kth request to the
	// provider is the one its kth token was for.
	limiters := make([]*timedLimiter, workers)
	for i := range workers {
		s.startWorker(workerConfig{
			name:        fmt.Sprintf("worker-%d", i),
			rate:        rate,
			providerURL: fmt.Sprintf("%s/worker-%d", s.provider.srv.URL, i),
			opts:        oneAtATime(),
			wrapLimiter: func(l worker.Limiter) worker.Limiter {
				limiters[i] = &timedLimiter{inner: l}
				return limiters[i]
			},
		})
	}

	// Redis hands out the slots by its own clock and the provider times the requests by the host's; on
	// Docker Desktop the two can run at different rates, so the test watches the Redis clock.
	ctx, stopWatching := context.WithCancel(t.Context())
	origin := time.Now()
	clock := redisclock.Watch(ctx, t, s.redisURL)

	items := make([]create, total)
	for i := range items {
		items[i] = create{Channel: "sms", Priority: "normal", Recipient: fmt.Sprintf("+90555%07d", i), Content: "rate limited"}
	}
	var batch struct {
		BatchID  string `json:"batch_id"`
		Accepted int    `json:"accepted"`
	}
	if code := s.request(http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": items}, nil, &batch); code != http.StatusAccepted || batch.Accepted != total {
		t.Fatalf("POST /v1/notifications/batch answered %d accepting %d, want 202 accepting %d", code, batch.Accepted, total)
	}
	waitFor(t, 30*time.Second, "every notification of the batch is sent", func() bool {
		var b struct {
			StatusCounts map[string]int `json:"status_counts"`
		}
		s.request(http.MethodGet, "/v1/batches/"+batch.BatchID, nil, nil, &b)
		return b.StatusCounts["sent"] == total
	})
	stopWatching()
	fastest, reached, readings := clock()

	// How late after its slot a request can reach the provider: from the earliest the slot can be (the
	// wait after the asking) to the request's arrival.
	var (
		arrivals []time.Time
		late     time.Duration
	)
	for i, l := range limiters {
		reqs := s.provider.receivedOn(fmt.Sprintf("/worker-%d", i))
		grants := l.taken()
		if len(reqs) != len(grants) {
			t.Fatalf("worker-%d took %d tokens and made %d requests, want as many", i, len(grants), len(reqs))
		}
		if len(reqs) == 0 {
			t.Errorf("worker-%d sent nothing, want every worker to share the work", i)
		}
		for k, r := range reqs {
			late = max(late, r.at.Sub(grants[k].asked.Add(grants[k].wait)))
			arrivals = append(arrivals, r.at)
		}
	}
	if len(arrivals) != total {
		t.Fatalf("the provider got %d requests, want %d", len(arrivals), total)
	}
	slices.SortFunc(arrivals, func(a, b time.Time) int { return a.Compare(b) })
	most := 0
	for i := range arrivals {
		j := i
		for j < len(arrivals) && arrivals[j].Sub(arrivals[i]) < time.Second {
			j++
		}
		most = max(most, j-i)
	}
	interval := time.Second / rate
	allowed := int(math.Ceil(float64(time.Second+late) * fastest / float64(interval)))
	took := arrivals[len(arrivals)-1].Sub(arrivals[0])
	t.Logf("%d workers sent %d in %v (%.1f/s); most in any rolling second: %d; latest arrival %v after its slot; Redis clock at most %.4f times as fast as the host's, and %.4f times as far",
		workers, total, took.Round(time.Millisecond), float64(total-1)/took.Seconds(), most, late.Round(time.Microsecond), fastest, reached)

	// The slots are one interval apart by the Redis clock, at least interval / fastest apart by the
	// host's; requests reaching the provider up to late after their slots can put the slots of 1 s
	// plus late into one second.
	if most > allowed {
		t.Errorf("the provider got %d requests within one second, want at most %d (%d per second by a Redis clock running %.4f times as fast, plus %v of lateness)",
			most, allowed, rate, fastest, late)
		t.Logf("Redis clock: %s", redisclock.Summary(origin, readings))
	}
}
