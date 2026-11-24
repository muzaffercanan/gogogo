package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/muzaffercanan/gogogo/internal/repository"
)

const (
	// reaperLease is how long a notification may stay in status sending before the reaper recovers it.
	reaperLease = 60 * time.Second
	// reaperInterval is how often the reaper scans for expired sending leases.
	reaperInterval = 15 * time.Second
)

// Reaper recovers notifications stuck in status sending past their delivery lease.
type Reaper struct {
	store *repository.Store
	log   *slog.Logger

	lease       time.Duration
	interval    time.Duration
	batchSize   int
	stepTimeout time.Duration
}

// NewReaper returns a reaper that scans store every 15 s for deliveries in status sending for at least
// 60 s.
func NewReaper(store *repository.Store, log *slog.Logger) *Reaper {
	return &Reaper{
		store:       store,
		log:         log,
		lease:       reaperLease,
		interval:    reaperInterval,
		batchSize:   batchSize,
		stepTimeout: stepTimeout,
	}
}

// Once runs one reaper batch step bounded by the step timeout. It returns the number of expired
// deliveries moved to retry_scheduled and permanently_failed.
func (r *Reaper) Once(ctx context.Context) (retried, failed int, err error) {
	stepCtx, cancel := context.WithTimeout(ctx, r.stepTimeout)
	defer cancel()

	return r.store.ReapExpiredDeliveries(stepCtx, r.lease, r.batchSize)
}

// Run scans for expired sending leases until ctx ends. A full batch is followed at once by another step
// so a large batch of expired leases drains without waiting for the next tick.
func (r *Reaper) Run(ctx context.Context) {
	for {
		retried, failed, err := r.Once(ctx)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			r.log.Warn("reaper step failed", "error", err)
		case retried+failed > 0:
			r.log.Info("reaped expired deliveries", "retried", retried, "permanently_failed", failed)
			if retried+failed == r.batchSize {
				if ctx.Err() != nil {
					return
				}
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.interval):
		}
	}
}
