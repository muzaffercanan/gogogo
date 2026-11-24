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

// Retry schedules the next delivery attempt of a notification in sending.
type Retry struct {
	ID uuid.UUID
	// Delay is how long after the commit, by the database clock, the relay may dispatch it again.
	Delay time.Duration
	// LastError replaces the notification's last error.
	LastError string
	// Reason is recorded on the status event. Optional.
	Reason string
}

// ScheduleRetry moves a notification from sending to retry_scheduled and inserts the outbox row that
// dispatches it again after r.Delay, in one transaction (ADR 0009: no dual write). The errors are those
// of TransitionNotification; on any of them nothing is written.
func (s *Store) ScheduleRetry(ctx context.Context, r Retry) (domain.Notification, error) {
	var n domain.Notification
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)
		var err error
		n, err = transition(ctx, q, Transition{
			ID:        r.ID,
			From:      []domain.Status{domain.StatusSending},
			To:        domain.StatusRetryScheduled,
			Reason:    r.Reason,
			LastError: r.LastError,
		})
		if err != nil {
			return err
		}
		payload, err := json.Marshal(outboxPayload{Channel: n.Channel, Priority: n.Priority, CorrelationID: n.CorrelationID})
		if err != nil {
			return fmt.Errorf("encoding retry outbox payload of notification %s: %w", r.ID, err)
		}
		if err := q.InsertRetryOutbox(ctx, sqlc.InsertRetryOutboxParams{
			NotificationID: r.ID,
			Payload:        payload,
			DelaySeconds:   max(r.Delay, 0).Seconds(),
		}); err != nil {
			return fmt.Errorf("inserting retry outbox row of notification %s: %w", r.ID, err)
		}
		return nil
	})
	if err != nil {
		return domain.Notification{}, err
	}
	return n, nil
}
