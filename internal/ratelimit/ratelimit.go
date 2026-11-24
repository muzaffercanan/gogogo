// Package ratelimit spaces the sends of each delivery channel evenly across all worker instances, with
// the generic cell rate algorithm (GCRA) and a burst of one kept in Redis (ADR 0003).
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

// ErrUnavailable is wrapped by the errors of a reservation that could not reach Redis. The limiter then
// cannot tell when a send may start, and callers stop sending (ADR 0004: fail closed).
var ErrUnavailable = errors.New("rate limiter unavailable")

// keyPrefix names the Redis key that holds a channel's theoretical arrival time, e.g. ratelimit:sms.
const keyPrefix = "ratelimit:"

// reserve hands out the next send slot of a channel (K-37). The key holds the theoretical arrival time
// (TAT): the earliest time, in microseconds of the Redis clock, at which the next send may start. A slot
// is max(TAT, now); the TAT then moves one emission interval past it. Taking the maximum with now means
// idle time is never banked, so sends are never closer than the interval (burst 1). The script returns
// how many microseconds the caller waits for its slot.
//
// Numbers go to redis.call as they are: Redis writes them with full precision, while Lua's tostring
// would round the microsecond timestamps to 14 digits.
var reserve = redis.NewScript(`
local interval = tonumber(ARGV[1])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local tat = tonumber(redis.call('GET', KEYS[1]) or 0)
if tat < now then
  tat = now
end
local next_tat = tat + interval
-- The key outlives the slot by a second; once it is gone, the next slot starts at now anyway.
redis.call('SET', KEYS[1], next_tat, 'PX', math.floor((next_tat - now) / 1000) + 1000)
return tat - now
`)

// Limiter reserves send slots for the delivery channels. It is safe for concurrent use. Create it with
// New.
type Limiter struct {
	rdb      redis.Scripter
	interval time.Duration
}

// New returns a limiter that lets each channel start at most ratePerSecond sends per second, one every
// second/ratePerSecond, through the Redis behind rdb.
func New(rdb redis.Scripter, ratePerSecond int) (*Limiter, error) {
	if ratePerSecond < 1 || ratePerSecond > 1_000_000 {
		return nil, fmt.Errorf("rate %d per second is outside [1, 1000000]", ratePerSecond)
	}
	return &Limiter{rdb: rdb, interval: time.Second / time.Duration(ratePerSecond)}, nil
}

// Reserve takes the next send slot of channel c and returns how long to wait before the send starts.
// The slot is taken whether or not the caller waits for it.
//
// It returns an error wrapping ErrUnavailable if Redis cannot be reached or fails, and ctx.Err() if ctx
// ended first.
func (l *Limiter) Reserve(ctx context.Context, c domain.Channel) (time.Duration, error) {
	wait, err := reserve.Run(ctx, l.rdb, []string{keyPrefix + string(c)}, l.interval.Microseconds()).Int64()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, fmt.Errorf("reserving a send slot for %s: %w: %w", c, ErrUnavailable, err)
	}
	return time.Duration(wait) * time.Microsecond, nil
}

// Wait reserves the next send slot of channel c and sleeps until it, returning how long it waited. If
// ctx ends while waiting, the slot is lost and Wait returns ctx.Err(). Errors are those of Reserve.
func (l *Limiter) Wait(ctx context.Context, c domain.Channel) (time.Duration, error) {
	wait, err := l.Reserve(ctx, c)
	if err != nil || wait <= 0 {
		return 0, err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
		return wait, nil
	}
}
