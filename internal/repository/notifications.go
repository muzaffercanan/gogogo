// Package repository stores notifications and their dispatch requests in PostgreSQL.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository/sqlc"
)

// Store reads and writes notifications.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// outboxPayload is stored with every outbox row, so the relay can route a notification to its queue
// without reading the notification itself.
type outboxPayload struct {
	Channel       domain.Channel  `json:"channel"`
	Priority      domain.Priority `json:"priority"`
	CorrelationID string          `json:"correlation_id,omitempty"`
}

// CreateNotification stores n in status pending together with its outbox row and its first status
// event, all in one transaction, and wakes up the relay once the transaction commits.
//
// If n.IdempotencyKey is already taken, nothing is written and the stored notification is returned
// with created == false. The caller tells a replay from a key reused with another request by comparing
// RequestHash. Concurrent calls with the same key are safe: exactly one of them creates the notification.
func (s *Store) CreateNotification(ctx context.Context, n domain.NewNotification) (stored domain.Notification, created bool, err error) {
	maxAttempts, err := toInt32(n.MaxAttempts)
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("max attempts: %w", err)
	}
	payload, err := json.Marshal(outboxPayload{
		Channel:       n.Channel,
		Priority:      n.Priority,
		CorrelationID: n.CorrelationID,
	})
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("encoding outbox payload: %w", err)
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		row, err := q.InsertNotification(ctx, sqlc.InsertNotificationParams{
			ID:             n.ID,
			BatchID:        n.BatchID,
			IdempotencyKey: nilIfEmpty(n.IdempotencyKey),
			RequestHash:    n.RequestHash,
			Channel:        string(n.Channel),
			Priority:       string(n.Priority),
			Recipient:      n.Recipient,
			Content:        n.Content,
			MaxAttempts:    maxAttempts,
			CorrelationID:  nilIfEmpty(n.CorrelationID),
			ScheduledAt:    n.ScheduledAt,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			// ON CONFLICT DO NOTHING: the idempotency key is taken. If another transaction inserted it
			// concurrently, the insert waited for that transaction to commit, so the row is visible now.
			existing, err := q.GetNotificationByIdempotencyKey(ctx, n.IdempotencyKey)
			if err != nil {
				return fmt.Errorf("reading notification by idempotency key: %w", err)
			}
			stored = toDomain(existing)
			return nil
		}
		if err != nil {
			return fmt.Errorf("inserting notification: %w", err)
		}

		if err := q.InsertOutbox(ctx, sqlc.InsertOutboxParams{
			NotificationID: row.ID,
			Payload:        payload,
			AvailableAt:    n.ScheduledAt,
		}); err != nil {
			return fmt.Errorf("inserting outbox row: %w", err)
		}
		if err := q.InsertNotificationEvent(ctx, sqlc.InsertNotificationEventParams{
			NotificationID: row.ID,
			ToStatus:       string(domain.StatusPending),
		}); err != nil {
			return fmt.Errorf("inserting status event: %w", err)
		}
		if err := q.NotifyOutbox(ctx); err != nil {
			return fmt.Errorf("notifying relay: %w", err)
		}

		stored, created = toDomain(row), true
		return nil
	})
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("creating notification: %w", err)
	}
	return stored, created, nil
}

func toDomain(r sqlc.Notification) domain.Notification {
	return domain.Notification{
		ID:                r.ID,
		BatchID:           r.BatchID,
		IdempotencyKey:    deref(r.IdempotencyKey),
		RequestHash:       r.RequestHash,
		Channel:           domain.Channel(r.Channel),
		Priority:          domain.Priority(r.Priority),
		Recipient:         r.Recipient,
		Content:           r.Content,
		Status:            domain.Status(r.Status),
		Attempts:          int(r.Attempts),
		MaxAttempts:       int(r.MaxAttempts),
		LastError:         deref(r.LastError),
		ProviderMessageID: deref(r.ProviderMessageID),
		CorrelationID:     deref(r.CorrelationID),
		ScheduledAt:       r.ScheduledAt,
		CreatedAt:         r.CreatedAt,
		UpdatedAt:         r.UpdatedAt,
		SentAt:            r.SentAt,
	}
}

func toInt32(v int) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("%d does not fit in int32", v)
	}
	return int32(v), nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
