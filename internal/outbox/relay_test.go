package outbox_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"

	"github.com/muzaffercanan/gogogo/internal/outbox"
)

func TestBackoffStaysInAGrowingCappedWindow(t *testing.T) {
	r := outbox.NewRelay(nil, nil, discardMetrics(), noop.NewTracerProvider(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	outbox.SetTiming(r, 100, time.Second, 200*time.Millisecond, 5*time.Second)

	tests := []struct {
		failures int
		window   time.Duration
	}{
		{0, 200 * time.Millisecond},
		{1, 400 * time.Millisecond},
		{3, 1600 * time.Millisecond},
		{5, 5 * time.Second}, // 6.4 s, capped
		{40, 5 * time.Second},
	}
	for _, tt := range tests {
		var longest time.Duration
		distinct := map[time.Duration]bool{}
		for range 2000 {
			d := outbox.Backoff(r, tt.failures)
			if d < 0 || d > tt.window {
				t.Fatalf("Backoff after %d failures = %v, want within [0, %v]", tt.failures, d, tt.window)
			}
			longest = max(longest, d)
			distinct[d] = true
		}
		if longest < tt.window*9/10 || len(distinct) < 100 {
			t.Errorf("after %d failures: longest of 2000 delays %v, %d distinct; want close to %v and spread out",
				tt.failures, longest, len(distinct), tt.window)
		}
	}
}
