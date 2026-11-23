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

// BatchPage is a batch with the number of its notifications in each status and one page of them.
type BatchPage struct {
	Batch domain.Batch
	// Counts holds the number of the batch's notifications per status; statuses without any are absent.
	Counts map[domain.Status]int
	// Items holds the page of notifications, oldest first.
	Items []domain.Notification
	// Next is the position to continue from, or nil on the last page.
	Next *Cursor
}

// GetBatchPage returns batch id, its status counts and up to limit of its notifications after the
// position after (nil for the first page). Everything is read from one snapshot, so the counts match the
// items. It returns an error wrapping domain.ErrNotFound if the batch does not exist.
func (s *Store) GetBatchPage(ctx context.Context, id uuid.UUID, after *Cursor, limit int) (BatchPage, error) {
	if limit <= 0 {
		return BatchPage{}, fmt.Errorf("page limit must be positive, got %d", limit)
	}
	rowLimit, err := toInt32(limit + 1)
	if err != nil {
		return BatchPage{}, fmt.Errorf("page limit: %w", err)
	}
	var page BatchPage
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
	err = pgx.BeginTxFunc(ctx, s.pool, opts, func(tx pgx.Tx) error {
		q := sqlc.New(tx)
		row, err := q.GetBatch(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("batch %s: %w", id, domain.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("reading batch: %w", err)
		}
		if page.Batch, err = toBatch(row); err != nil {
			return err
		}

		counts, err := q.CountBatchNotificationsByStatus(ctx, &id)
		if err != nil {
			return fmt.Errorf("counting batch notifications: %w", err)
		}
		page.Counts = make(map[domain.Status]int, len(counts))
		for _, c := range counts {
			page.Counts[domain.Status(c.Status)] = int(c.Notifications)
		}

		params := sqlc.ListBatchNotificationsParams{BatchID: &id, RowLimit: rowLimit}
		if after != nil {
			params.AfterCreatedAt, params.AfterID = after.CreatedAt, after.ID
		}
		rows, err := q.ListBatchNotifications(ctx, params)
		if err != nil {
			return fmt.Errorf("listing batch notifications: %w", err)
		}
		for _, r := range rows {
			page.Items = append(page.Items, toDomain(r))
		}
		// One row more than the limit tells whether another page exists.
		if len(page.Items) > limit {
			page.Items = page.Items[:limit]
			last := page.Items[limit-1]
			page.Next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
		}
		return nil
	})
	if err != nil {
		return BatchPage{}, fmt.Errorf("reading batch %s: %w", id, err)
	}
	return page, nil
}
