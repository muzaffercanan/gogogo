package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository/sqlc"
)

// storedItemResult is how a domain.BatchItemResult is kept in batches.item_results.
type storedItemResult struct {
	Index          int                `json:"index"`
	NotificationID *uuid.UUID         `json:"notification_id,omitempty"`
	Errors         []storedFieldError `json:"errors,omitempty"`
}

type storedFieldError struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// CreateBatch stores b with its accepted notifications, their outbox rows and status events in one
// transaction, and wakes up the relay once the transaction commits. Each table takes one statement
// whatever the number of notifications.
//
// If b.IdempotencyKey is already taken, nothing is written and the stored batch is returned with
// created == false, as CreateNotification does for single notifications.
func (s *Store) CreateBatch(ctx context.Context, b domain.NewBatch) (stored domain.Batch, created bool, err error) {
	params, err := batchParams(b)
	if err != nil {
		return domain.Batch{}, false, err
	}
	rows, err := notificationArrays(b.ID, b.Notifications)
	if err != nil {
		return domain.Batch{}, false, err
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		row, err := q.InsertBatch(ctx, params)
		if errors.Is(err, pgx.ErrNoRows) {
			existing, err := q.GetBatchByIdempotencyKey(ctx, b.IdempotencyKey)
			if err != nil {
				return fmt.Errorf("reading batch by idempotency key: %w", err)
			}
			stored, err = toBatch(existing)
			return err
		}
		if err != nil {
			return fmt.Errorf("inserting batch: %w", err)
		}

		if len(b.Notifications) > 0 {
			if err := q.InsertBatchNotifications(ctx, rows.notifications); err != nil {
				return fmt.Errorf("inserting batch notifications: %w", err)
			}
			if err := q.InsertBatchOutbox(ctx, rows.outbox); err != nil {
				return fmt.Errorf("inserting batch outbox rows: %w", err)
			}
			if err := q.InsertBatchEvents(ctx, rows.notifications.Ids); err != nil {
				return fmt.Errorf("inserting batch status events: %w", err)
			}
			if err := q.NotifyOutbox(ctx); err != nil {
				return fmt.Errorf("notifying relay: %w", err)
			}
		}

		stored, err = toBatch(row)
		created = err == nil
		return err
	})
	if err != nil {
		return domain.Batch{}, false, fmt.Errorf("creating batch: %w", err)
	}
	return stored, created, nil
}

// GetBatchByIdempotencyKey returns the batch created with key. It returns an error wrapping
// domain.ErrNotFound if there is none.
func (s *Store) GetBatchByIdempotencyKey(ctx context.Context, key string) (domain.Batch, error) {
	row, err := sqlc.New(s.pool).GetBatchByIdempotencyKey(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Batch{}, fmt.Errorf("batch with idempotency key %q: %w", key, domain.ErrNotFound)
	}
	if err != nil {
		return domain.Batch{}, fmt.Errorf("reading batch by idempotency key: %w", err)
	}
	return toBatch(row)
}

func batchParams(b domain.NewBatch) (sqlc.InsertBatchParams, error) {
	items := make([]storedItemResult, len(b.Items))
	rejected := 0
	for i, it := range b.Items {
		items[i] = storedItemResult{Index: it.Index, NotificationID: it.NotificationID}
		for _, e := range it.Errors {
			items[i].Errors = append(items[i].Errors, storedFieldError{Field: e.Field, Code: string(e.Code), Message: e.Message})
		}
		if it.NotificationID == nil {
			rejected++
		}
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return sqlc.InsertBatchParams{}, fmt.Errorf("encoding batch item results: %w", err)
	}
	total, err := toInt32(len(b.Items))
	if err != nil {
		return sqlc.InsertBatchParams{}, fmt.Errorf("batch size: %w", err)
	}
	return sqlc.InsertBatchParams{
		ID:             b.ID,
		IdempotencyKey: nilIfEmpty(b.IdempotencyKey),
		RequestHash:    b.RequestHash,
		TotalCount:     total,
		AcceptedCount:  total - int32(rejected),
		RejectedCount:  int32(rejected),
		ItemResults:    encoded,
	}, nil
}

// batchRows holds the column arrays of a batch's notifications and outbox rows.
type batchRows struct {
	notifications sqlc.InsertBatchNotificationsParams
	outbox        sqlc.InsertBatchOutboxParams
}

func notificationArrays(batchID uuid.UUID, ns []domain.NewNotification) (batchRows, error) {
	r := batchRows{notifications: sqlc.InsertBatchNotificationsParams{BatchID: &batchID}}
	for _, n := range ns {
		maxAttempts, err := toInt32(n.MaxAttempts)
		if err != nil {
			return batchRows{}, fmt.Errorf("max attempts: %w", err)
		}
		payload, err := json.Marshal(outboxPayload{Channel: n.Channel, Priority: n.Priority, CorrelationID: n.CorrelationID})
		if err != nil {
			return batchRows{}, fmt.Errorf("encoding outbox payload: %w", err)
		}
		var at time.Time
		if n.ScheduledAt != nil {
			at = *n.ScheduledAt
		}
		p := &r.notifications
		p.Ids = append(p.Ids, n.ID)
		p.Channels = append(p.Channels, string(n.Channel))
		p.Priorities = append(p.Priorities, string(n.Priority))
		p.Recipients = append(p.Recipients, n.Recipient)
		p.Contents = append(p.Contents, n.Content)
		p.MaxAttempts = append(p.MaxAttempts, maxAttempts)
		p.CorrelationIds = append(p.CorrelationIds, n.CorrelationID)
		p.IsScheduled = append(p.IsScheduled, n.ScheduledAt != nil)
		p.ScheduledAts = append(p.ScheduledAts, at)
		r.outbox.Payloads = append(r.outbox.Payloads, string(payload))
	}
	r.outbox.Ids = r.notifications.Ids
	r.outbox.IsScheduled = r.notifications.IsScheduled
	r.outbox.ScheduledAts = r.notifications.ScheduledAts
	return r, nil
}

func toBatch(r sqlc.Batch) (domain.Batch, error) {
	var items []storedItemResult
	if err := json.Unmarshal(r.ItemResults, &items); err != nil {
		return domain.Batch{}, fmt.Errorf("decoding item results of batch %s: %w", r.ID, err)
	}
	b := domain.Batch{
		ID:             r.ID,
		IdempotencyKey: deref(r.IdempotencyKey),
		RequestHash:    r.RequestHash,
		Total:          int(r.TotalCount),
		Accepted:       int(r.AcceptedCount),
		Rejected:       int(r.RejectedCount),
		Items:          make([]domain.BatchItemResult, len(items)),
		CreatedAt:      r.CreatedAt,
	}
	for i, it := range items {
		b.Items[i] = domain.BatchItemResult{Index: it.Index, NotificationID: it.NotificationID}
		for _, e := range it.Errors {
			b.Items[i].Errors = append(b.Items[i].Errors, domain.FieldError{Field: e.Field, Code: domain.FieldErrorCode(e.Code), Message: e.Message})
		}
	}
	return b, nil
}
