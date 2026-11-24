package queue_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/queue"
)

func TestBackoffStaysInAGrowingCappedWindow(t *testing.T) {
	s := queue.NewSupervisor("amqp://unused", "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	queue.SetBackoff(s, 100*time.Millisecond, 2*time.Second)

	tests := []struct {
		failures int
		window   time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{5, 2 * time.Second}, // 3.2 s, capped
		{40, 2 * time.Second},
	}
	for _, tt := range tests {
		var longest time.Duration
		distinct := map[time.Duration]bool{}
		for range 2000 {
			d := queue.Backoff(s, tt.failures)
			if d < 0 || d > tt.window {
				t.Fatalf("Backoff after %d failures = %v, want within [0, %v]", tt.failures, d, tt.window)
			}
			longest = max(longest, d)
			distinct[d] = true
		}
		// Full jitter spreads the delays over the whole window.
		if longest < tt.window*9/10 || len(distinct) < 100 {
			t.Errorf("after %d failures: longest of 2000 delays %v, %d distinct; want close to %v and spread out",
				tt.failures, longest, len(distinct), tt.window)
		}
	}
}
