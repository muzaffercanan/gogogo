package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository/sqlc"
)

// OutboxItem is a due notification that DispatchOutbox has moved to status queued and that the
// caller must publish before the transaction commits.
type OutboxItem struct {
	NotificationID uuid.UUID
	Channel        domain.Channel
	Priority       domain.Priority
	CorrelationID  string
}

// DispatchOutbox claims up to limit due outbox rows with FOR UPDATE SKIP LOCKED, moves their
// notifications from pending or retry_scheduled to queued (recording a status event for each one),
// calls publish with the ones that moved to queued in their available_at order, and deletes every
// claimed outbox row — all in one transaction.
//
// Claimed rows whose notification was no longer pending or retry_scheduled (for example because a
// client cancelled it) are deleted without being passed to publish. publish is not called when no
// notification moved to queued. If publish returns an error, the transaction rolls back so the status
// changes, events and deletions are undone.
//
// It returns the number of claimed outbox rows and the number of notifications handed to publish.
func (s *Store) DispatchOutbox(ctx context.Context, limit int, publish func(context.Context, []OutboxItem) error) (claimed, published int, err error) {
	if limit <= 0 {
		return 0, 0, fmt.Errorf("outbox batch limit must be positive, got %d", limit)
	}
	rowLimit, err := toInt32(limit)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox batch limit: %w", err)
	}
	allowed := domain.AllowedFrom(domain.StatusQueued)
	from := make([]string, len(allowed))
	for i, st := range allowed {
		from[i] = string(st)
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		rows, err := q.ClaimDueOutbox(ctx, rowLimit)
		if err != nil {
			return fmt.Errorf("claiming due outbox rows: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}

		notificationIDs := make([]uuid.UUID, len(rows))
		outboxIDs := make([]int64, len(rows))
		for i, r := range rows {
			notificationIDs[i] = r.NotificationID
			outboxIDs[i] = r.ID
		}

		queuedIDs, err := q.QueueNotifications(ctx, sqlc.QueueNotificationsParams{
			Ids:          notificationIDs,
			FromStatuses: from,
		})
		if err != nil {
			return fmt.Errorf("moving notifications to queued: %w", err)
		}

		queued := make(map[uuid.UUID]struct{}, len(queuedIDs))
		for _, id := range queuedIDs {
			queued[id] = struct{}{}
		}

		items := make([]OutboxItem, 0, len(queuedIDs))
		for _, r := range rows {
			if _, ok := queued[r.NotificationID]; !ok {
				continue
			}
			delete(queued, r.NotificationID)
			var p outboxPayload
			if err := json.Unmarshal(r.Payload, &p); err != nil {
				return fmt.Errorf("decoding outbox payload of notification %s: %w", r.NotificationID, err)
			}
			items = append(items, OutboxItem{
				NotificationID: r.NotificationID,
				Channel:        p.Channel,
				Priority:       p.Priority,
				CorrelationID:  p.CorrelationID,
			})
		}

		if len(items) > 0 {
			if err := publish(ctx, items); err != nil {
				return fmt.Errorf("publishing queued notifications: %w", err)
			}
		}

		if err := q.DeleteOutbox(ctx, outboxIDs); err != nil {
			return fmt.Errorf("deleting claimed outbox rows: %w", err)
		}

		claimed, published = len(rows), len(items)
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("dispatching outbox: %w", err)
	}
	return claimed, published, nil
}

// ListenOutbox acquires a dedicated connection, subscribes to the PostgreSQL "outbox" channel with
// LISTEN, calls onWake once ready (so rows committed before LISTEN took effect are not missed) and
// then calls onWake whenever a notification arrives, until ctx ends or the connection fails.
//
// The underlying connection is closed before it is released back to the pool so a session that ran
// LISTEN is never reused for ordinary queries.
func (s *Store) ListenOutbox(ctx context.Context, onWake func()) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring listen connection: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Conn().Close(closeCtx)
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, "LISTEN outbox"); err != nil {
		return fmt.Errorf("listening on outbox channel: %w", err)
	}
	onWake()

	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return fmt.Errorf("waiting for outbox notification: %w", err)
		}
		onWake()
	}
}

const (
	// leaseExpiredReason is recorded on the status event when the reaper recovers a stuck delivery.
	leaseExpiredReason = "lease_expired"
	// leaseExpiredError is stored in notifications.last_error when a delivery lease expires.
	leaseExpiredError = "delivery lease expired"
)

// ReapedDelivery is an expired sending notification that ReapExpiredDeliveries moved to
// retry_scheduled or permanently_failed.
type ReapedDelivery struct {
	NotificationID uuid.UUID
	Channel        domain.Channel
	Priority       domain.Priority
	CorrelationID  string
	Status         domain.Status
}

// ReapExpiredDeliveries moves up to limit notifications that have been in status sending for at least
// lease to retry_scheduled (when attempts < max_attempts) or permanently_failed (when attempts are
// exhausted), records a status event with reason "lease_expired" and last_error "delivery lease
// expired" for each one, and inserts due outbox rows (waking the relay with NOTIFY outbox) for the ones
// that moved to retry_scheduled — all in one transaction.
func (s *Store) ReapExpiredDeliveries(ctx context.Context, lease time.Duration, limit int) ([]ReapedDelivery, error) {
	if lease <= 0 {
		return nil, fmt.Errorf("reaper lease must be positive, got %v", lease)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("reaper batch limit must be positive, got %d", limit)
	}
	rowLimit, err := toInt32(limit)
	if err != nil {
		return nil, fmt.Errorf("reaper batch limit: %w", err)
	}
	for _, to := range []domain.Status{domain.StatusRetryScheduled, domain.StatusPermanentlyFailed} {
		if !domain.CanTransition(domain.StatusSending, to) {
			return nil, fmt.Errorf("moving notification from %s to %s: %w", domain.StatusSending, to, domain.ErrInvalidTransition)
		}
	}

	var reaped []ReapedDelivery
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		rows, err := q.ReapExpiredSending(ctx, sqlc.ReapExpiredSendingParams{
			LeaseSeconds: lease.Seconds(),
			RowLimit:     rowLimit,
			RetryStatus:  string(domain.StatusRetryScheduled),
			FailedStatus: string(domain.StatusPermanentlyFailed),
			LastError:    leaseExpiredError,
			Reason:       leaseExpiredReason,
		})
		if err != nil {
			return fmt.Errorf("reaping expired sending notifications: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}

		reaped = make([]ReapedDelivery, len(rows))
		var (
			retryIDs      []uuid.UUID
			retryPayloads []string
		)
		for i, r := range rows {
			d := ReapedDelivery{
				NotificationID: r.ID,
				Channel:        domain.Channel(r.Channel),
				Priority:       domain.Priority(r.Priority),
				CorrelationID:  deref(r.CorrelationID),
				Status:         domain.Status(r.Status),
			}
			reaped[i] = d
			if d.Status != domain.StatusRetryScheduled {
				continue
			}
			payload, err := json.Marshal(outboxPayload{
				Channel:       d.Channel,
				Priority:      d.Priority,
				CorrelationID: d.CorrelationID,
			})
			if err != nil {
				return fmt.Errorf("encoding retry outbox payload of notification %s: %w", r.ID, err)
			}
			retryIDs = append(retryIDs, r.ID)
			retryPayloads = append(retryPayloads, string(payload))
		}

		if len(retryIDs) > 0 {
			if err := q.InsertDueOutboxBatch(ctx, sqlc.InsertDueOutboxBatchParams{
				Ids:      retryIDs,
				Payloads: retryPayloads,
			}); err != nil {
				return fmt.Errorf("inserting retry outbox rows: %w", err)
			}
			if err := q.NotifyOutbox(ctx); err != nil {
				return fmt.Errorf("notifying relay: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reaping expired deliveries: %w", err)
	}
	return reaped, nil
}

// OutboxBacklog is the number of outbox rows waiting for the relay.
type OutboxBacklog struct {
	// Due rows have an available_at that has passed: the relay can dispatch them now.
	Due int64
	// Scheduled rows have an available_at in the future: scheduled sends and retries in backoff.
	Scheduled int64
}

// CountOutboxBacklog counts the outbox rows that are due and the ones scheduled for later, by the
// database clock.
func (s *Store) CountOutboxBacklog(ctx context.Context) (OutboxBacklog, error) {
	row, err := sqlc.New(s.pool).CountOutboxBacklog(ctx)
	if err != nil {
		return OutboxBacklog{}, fmt.Errorf("counting outbox backlog: %w", err)
	}
	return OutboxBacklog{Due: row.Due, Scheduled: row.Scheduled}, nil
}
