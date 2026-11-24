package backoff_test

import (
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/platform/backoff"
)

func TestWindowDoublesUpToTheCap(t *testing.T) {
	tests := []struct {
		n    int
		want time.Duration
	}{
		{n: -1, want: time.Second},
		{n: 0, want: time.Second},
		{n: 1, want: 2 * time.Second},
		{n: 4, want: 16 * time.Second},
		{n: 5, want: 32 * time.Second},
		{n: 6, want: time.Minute}, // 64 s, capped
		{n: 40, want: time.Minute},
		{n: 63, want: time.Minute},
		{n: 1000, want: time.Minute},
	}
	for _, tt := range tests {
		if got := backoff.Window(tt.n, time.Second, time.Minute); got != tt.want {
			t.Errorf("Window(%d, 1s, 1m) = %v, want %v", tt.n, got, tt.want)
		}
	}
}

func TestFullJitterSpreadsOverTheWholeWindow(t *testing.T) {
	for _, n := range []int{0, 3, 10} {
		window := backoff.Window(n, 100*time.Millisecond, 2*time.Second)
		var longest time.Duration
		distinct := map[time.Duration]bool{}
		for range 2000 {
			d := backoff.FullJitter(n, 100*time.Millisecond, 2*time.Second)
			if d < 0 || d > window {
				t.Fatalf("FullJitter(%d) = %v, want within [0, %v]", n, d, window)
			}
			longest = max(longest, d)
			distinct[d] = true
		}
		if longest < window*9/10 || len(distinct) < 100 {
			t.Errorf("n=%d: longest of 2000 delays %v, %d distinct; want close to %v and spread out", n, longest, len(distinct), window)
		}
	}
}
