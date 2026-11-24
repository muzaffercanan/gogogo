package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/ratelimit"
)

// t0 is a fixed Redis clock, with microseconds, for the miniredis tests.
var t0 = time.Date(2026, 11, 24, 12, 0, 0, 123456000, time.UTC)

// fakeRedis returns a miniredis server whose clock stands at t0 and a client for it. The script reads
// the clock with TIME, which miniredis answers from SetTime.
func fakeRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	m := miniredis.RunT(t)
	m.SetTime(t0)
	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return m, rdb
}

func newLimiter(t *testing.T, rdb redis.Scripter, rate int) *ratelimit.Limiter {
	t.Helper()
	l, err := ratelimit.New(rdb, rate)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func reserve(t *testing.T, l *ratelimit.Limiter, c domain.Channel) time.Duration {
	t.Helper()
	wait, err := l.Reserve(t.Context(), c)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	return wait
}

func TestReserveSpacesSlotsOneIntervalApart(t *testing.T) {
	m, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 100)

	// Five reservations at the same instant get consecutive 10 ms slots.
	for i := range 5 {
		if got, want := reserve(t, l, domain.ChannelSMS), time.Duration(i)*10*time.Millisecond; got != want {
			t.Errorf("reservation %d waits %v, want %v", i, got, want)
		}
	}
	// 25 ms later the next free slot is 50 ms after t0.
	m.SetTime(t0.Add(25 * time.Millisecond))
	if got, want := reserve(t, l, domain.ChannelSMS), 25*time.Millisecond; got != want {
		t.Errorf("after 25 ms: waits %v, want %v", got, want)
	}
}

func TestReserveBanksNoIdleTime(t *testing.T) {
	m, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 100)

	reserve(t, l, domain.ChannelSMS)
	// After five idle seconds the channel may send at once, but only once: the slot after it is one
	// interval later, as if the idle time had never been.
	m.SetTime(t0.Add(5 * time.Second))
	if got := reserve(t, l, domain.ChannelSMS); got != 0 {
		t.Errorf("first reservation after idling waits %v, want 0", got)
	}
	if got, want := reserve(t, l, domain.ChannelSMS), 10*time.Millisecond; got != want {
		t.Errorf("second reservation after idling waits %v, want %v", got, want)
	}
}

func TestReserveKeepsEachChannelsSlotsApart(t *testing.T) {
	_, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 100)

	for _, c := range []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush} {
		if got := reserve(t, l, c); got != 0 {
			t.Errorf("first %s reservation waits %v, want 0", c, got)
		}
	}
	if got, want := reserve(t, l, domain.ChannelEmail), 10*time.Millisecond; got != want {
		t.Errorf("second email reservation waits %v, want %v", got, want)
	}
}

func TestReserveSpacesSlotsByTheConfiguredRate(t *testing.T) {
	tests := []struct {
		rate     int
		interval time.Duration
	}{
		{rate: 1, interval: time.Second},
		{rate: 3, interval: 333333 * time.Microsecond},
		{rate: 100, interval: 10 * time.Millisecond},
		{rate: 1000, interval: time.Millisecond},
	}
	for _, tt := range tests {
		_, rdb := fakeRedis(t)
		l := newLimiter(t, rdb, tt.rate)
		reserve(t, l, domain.ChannelPush)
		if got := reserve(t, l, domain.ChannelPush); got != tt.interval {
			t.Errorf("rate %d: second reservation waits %v, want %v", tt.rate, got, tt.interval)
		}
	}
}

func TestReserveLetsTheKeyExpireAfterItsSlot(t *testing.T) {
	m, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 100)
	reserve(t, l, domain.ChannelSMS)
	reserve(t, l, domain.ChannelSMS) // the theoretical arrival time is now t0 + 20 ms

	// The key lives one second past the next slot.
	if got, want := m.TTL("ratelimit:sms"), 1020*time.Millisecond; got != want {
		t.Errorf("TTL of ratelimit:sms = %v, want %v", got, want)
	}
}

func TestReserveFailsWithErrUnavailableWithoutRedis(t *testing.T) {
	m, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 100)
	m.Close()

	_, err := l.Reserve(t.Context(), domain.ChannelSMS)
	if !errors.Is(err, ratelimit.ErrUnavailable) {
		t.Errorf("Reserve without Redis = %v, want ErrUnavailable", err)
	}
}

func TestReserveReturnsTheContextErrorWhenTheContextEnded(t *testing.T) {
	_, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := l.Reserve(ctx, domain.ChannelSMS)
	if !errors.Is(err, context.Canceled) || errors.Is(err, ratelimit.ErrUnavailable) {
		t.Errorf("Reserve with a cancelled context = %v, want context.Canceled and not ErrUnavailable", err)
	}
}

func TestWaitSleepsUntilTheReservedSlot(t *testing.T) {
	_, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 10) // 100 ms apart; the fake clock stands still

	if waited, err := l.Wait(t.Context(), domain.ChannelSMS); err != nil || waited != 0 {
		t.Fatalf("first Wait = %v, %v; want 0, nil", waited, err)
	}
	start := time.Now()
	waited, err := l.Wait(t.Context(), domain.ChannelSMS)
	if err != nil || waited != 100*time.Millisecond {
		t.Fatalf("second Wait = %v, %v; want 100ms, nil", waited, err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Errorf("second Wait returned after %v, before its slot", elapsed)
	}
}

func TestWaitReturnsWhenTheContextEndsBeforeTheSlot(t *testing.T) {
	_, rdb := fakeRedis(t)
	l := newLimiter(t, rdb, 1) // one second apart

	if _, err := l.Wait(t.Context(), domain.ChannelSMS); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := l.Wait(ctx, domain.ChannelSMS); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Wait returned after %v, long after its context ended", elapsed)
	}
}

func TestNewRejectsARateOutsideItsRange(t *testing.T) {
	for _, rate := range []int{0, -1, 1_000_001} {
		if _, err := ratelimit.New(redis.NewClient(&redis.Options{}), rate); err == nil {
			t.Errorf("New with rate %d succeeded, want an error", rate)
		}
	}
	if _, err := ratelimit.New(redis.NewClient(&redis.Options{}), 1_000_000); err != nil {
		t.Errorf("New with rate 1000000: %v", err)
	}
}
