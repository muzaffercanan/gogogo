package worker_test

import (
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/worker"
)

// widest draws the longest delay of every window, so the policy's window can be read off Delay.
func widest(window time.Duration) time.Duration { return window }

// none draws no delay at all, so only Retry-After remains.
func none(time.Duration) time.Duration { return 0 }

func TestRetryDelayWindowDoublesFromOneSecondToTheSixtySecondCap(t *testing.T) {
	p := worker.WithJitter(worker.DefaultRetryPolicy(), widest)
	tests := []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 1, want: time.Second},
		{attempts: 2, want: 2 * time.Second},
		{attempts: 3, want: 4 * time.Second},
		{attempts: 4, want: 8 * time.Second},
		{attempts: 5, want: 16 * time.Second},
		{attempts: 7, want: time.Minute}, // 64 s, capped
		{attempts: 100, want: time.Minute},
	}
	for _, tt := range tests {
		if got := p.Delay(tt.attempts, 0); got != tt.want {
			t.Errorf("Delay(%d attempts) with the widest jitter = %v, want %v", tt.attempts, got, tt.want)
		}
	}
}

func TestRetryDelayIsFullyJittered(t *testing.T) {
	p := worker.DefaultRetryPolicy()
	var shortest, longest time.Duration = time.Hour, 0
	for range 2000 {
		d := p.Delay(3, 0) // window 4 s
		if d < 0 || d > 4*time.Second {
			t.Fatalf("Delay(3 attempts) = %v, want within [0, 4s]", d)
		}
		shortest, longest = min(shortest, d), max(longest, d)
	}
	if shortest > 400*time.Millisecond || longest < 3600*time.Millisecond {
		t.Errorf("2000 delays spanned [%v, %v], want close to the whole [0, 4s]", shortest, longest)
	}
}

func TestRetryDelayWaitsAtLeastRetryAfterUpToAnHour(t *testing.T) {
	tests := []struct {
		name       string
		jitter     func(time.Duration) time.Duration
		retryAfter time.Duration
		want       time.Duration
	}{
		{name: "retry-after above the jittered delay", jitter: none, retryAfter: 90 * time.Second, want: 90 * time.Second},
		{name: "jittered delay above retry-after", jitter: widest, retryAfter: 100 * time.Millisecond, want: 4 * time.Second},
		{name: "retry-after beyond an hour", jitter: none, retryAfter: 2 * time.Hour, want: time.Hour},
		{name: "no retry-after", jitter: none, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := worker.WithJitter(worker.DefaultRetryPolicy(), tt.jitter)
			if got := p.Delay(3, tt.retryAfter); got != tt.want {
				t.Errorf("Delay(3 attempts, retry after %v) = %v, want %v", tt.retryAfter, got, tt.want)
			}
		})
	}
}
