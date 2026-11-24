package repository

import (
	"context"
	"encoding/json"
	"fmt"

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
