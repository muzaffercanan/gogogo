//go:build integration

package worker_test

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/worker"
)

func TestWorkerHoldsItsDeliveriesWhileTheBreakerIsOpen(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	h := newHarness(t, func(int) answer {
		if failing.Load() {
			return answer{status: http.StatusServiceUnavailable}
		}
		return accepted("m-9")
	})
	// One send at a time, so the breaker sees every result before the next delivery starts, but for
	// the one it let through while the last result was pending. That one then waits for the sender and
	// a token, so it can reach the provider after the breaker has opened; the slow limiter makes it
	// always do so.
	h.opts.Concurrency = 1
	h.rate = 5
	h.opts.Breaker = worker.BreakerSettings{Window: 10 * time.Second, MinRequests: 5, FailureRatio: 0.5, OpenFor: 2 * time.Second}
	var created []domain.Notification
	for range 12 {
		created = append(created, h.create(t, nil))
	}
	h.dispatch(t)
	h.startWorker(t)

	waitFor(t, "the breaker opens", func() bool { return metric(t, h.reg, "circuit_breaker_state") == 2 })
	// While open the worker sends nothing but the delivery already let through, and picks nothing more
	// (at 5 sends/s it would have sent about 5 more in this second).
	time.Sleep(time.Second)
	tripped := len(h.provider.got())
	if tripped < 5 || tripped > 6 {
		t.Fatalf("the provider got %d requests by 1 s after the breaker opened, want 5 (plus at most the one already let through)", tripped)
	}

	// The provider recovers; after the open time one probe goes through, succeeds and closes the
	// breaker, and the held deliveries go out.
	failing.Store(false)
	waitFor(t, "the breaker closes", func() bool { return metric(t, h.reg, "circuit_breaker_state") == 0 })
	sent := 0
	for _, n := range created {
		waitFor(t, "every notification is sent or waits for a retry", func() bool {
			got, err := h.store.GetNotification(t.Context(), n.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status == domain.StatusSent {
				sent++
			}
			return got.Status == domain.StatusSent || got.Status == domain.StatusRetryScheduled
		})
	}
	if want := len(created) - tripped; sent != want {
		t.Errorf("%d notifications sent after the breaker closed, want the %d that were held", sent, want)
	}
}

func TestWorkerKeepsTheBreakerClosedWhenTheProviderRefusesForGood(t *testing.T) {
	h := newHarness(t, always(answer{status: http.StatusBadRequest, body: `{"error":"recipient rejected"}`}))
	h.opts.Breaker = worker.BreakerSettings{Window: 10 * time.Second, MinRequests: 5, FailureRatio: 0.5, OpenFor: 2 * time.Second}
	var created []domain.Notification
	for range 10 {
		created = append(created, h.create(t, nil))
	}
	h.dispatch(t)
	h.startWorker(t)

	for _, n := range created {
		h.waitStatus(t, n.ID, domain.StatusPermanentlyFailed)
	}
	if state := metric(t, h.reg, "circuit_breaker_state"); state != 0 {
		t.Errorf("circuit_breaker_state = %v after permanent refusals, want 0 (closed)", state)
	}
}
