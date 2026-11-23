//go:build integration

package repository_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// newBatch returns a batch whose items are accepted notifications, one per recipient, followed by one
// rejected item.
func newBatch(t *testing.T, key string, modify func(*domain.NewNotification), recipients ...string) domain.NewBatch {
	t.Helper()
	b := domain.NewBatch{ID: uuid.Must(uuid.NewV7()), IdempotencyKey: key, RequestHash: []byte(key)}
	for i, r := range recipients {
		n := newNotification(t, func(n *domain.NewNotification) { n.Recipient = r; n.CorrelationID = "corr-batch" })
		if modify != nil {
			modify(&n)
		}
		b.Notifications = append(b.Notifications, n)
		b.Items = append(b.Items, domain.BatchItemResult{Index: i, NotificationID: &n.ID})
	}
	b.Items = append(b.Items, domain.BatchItemResult{Index: len(recipients), Errors: []domain.FieldError{
		{Field: "recipient", Code: domain.CodeInvalidFormat, Message: "must be an E.164 phone number"},
	}})
	return b
}

func TestCreateBatch(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	at := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	b := newBatch(t, "batch-1", nil, "+905550000001", "+905550000002")
	b.Notifications[1].ScheduledAt = &at

	got, created, err := repository.New(db).CreateBatch(ctx, b)
	if err != nil || !created {
		t.Fatalf("CreateBatch: created = %v, err = %v", created, err)
	}
	if got.ID != b.ID || got.Total != 3 || got.Accepted != 2 || got.Rejected != 1 || got.IdempotencyKey != "batch-1" {
		t.Errorf("batch = %+v, want 3 items, 2 accepted, 1 rejected", got)
	}
	if fmt.Sprint(got.Items) != fmt.Sprint(b.Items) {
		t.Errorf("item results = %v, want %v", got.Items, b.Items)
	}
	assertBatchRows(t, db, b.ID, 2)

	// The unscheduled row is available from its creation time, the scheduled one from its time.
	rows, err := db.Query(ctx, `SELECT n.recipient, n.correlation_id, n.scheduled_at, o.available_at = n.created_at, o.available_at
		FROM notifications n JOIN outbox o ON o.notification_id = n.id ORDER BY n.id`)
	if err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		var (
			recipient, corr string
			scheduled       *time.Time
			atCreation      bool
			available       time.Time
		)
		if err := rows.Scan(&recipient, &corr, &scheduled, &atCreation, &available); err != nil {
			t.Fatalf("scanning: %v", err)
		}
		if recipient != b.Notifications[i].Recipient || corr != "corr-batch" {
			t.Errorf("row %d = %s %s, want %s corr-batch in request order", i, recipient, corr, b.Notifications[i].Recipient)
		}
		switch {
		case i == 0 && (scheduled != nil || !atCreation):
			t.Errorf("unscheduled row: scheduled_at %v, available at creation %v", scheduled, atCreation)
		case i == 1 && (scheduled == nil || !scheduled.Equal(at) || !available.Equal(at)):
			t.Errorf("scheduled row: scheduled_at %v, available_at %v, want %v", scheduled, available, at)
		}
	}
}

func TestCreateBatchWithOnlyRejectedItems(t *testing.T) {
	db := pg.NewDatabase(t)
	b := newBatch(t, "", nil)

	got, created, err := repository.New(db).CreateBatch(context.Background(), b)
	if err != nil || !created || got.Accepted != 0 || got.Rejected != 1 {
		t.Fatalf("CreateBatch = %+v, created %v, err %v; want a stored batch with 1 rejected item", got, created, err)
	}
	assertBatchRows(t, db, b.ID, 0)
}

func TestCreateBatchFailureWritesNothing(t *testing.T) {
	db := pg.NewDatabase(t)
	b := newBatch(t, "", nil, "+905550000001", "+905550000002")
	b.Notifications[1].Channel = "fax" // violates the channel check of the second row

	if _, _, err := repository.New(db).CreateBatch(context.Background(), b); err == nil {
		t.Fatal("CreateBatch with an invalid row succeeded")
	}
	for _, table := range []string{"batches", "notifications", "outbox", "notification_events"} {
		if n := count(t, db, table); n != 0 {
			t.Errorf("%s has %d rows after a failed batch, want 0", table, n)
		}
	}
}

func TestCreateBatchWithTakenKeyReturnsExisting(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	first := newBatch(t, "same-key", nil, "+905550000001")
	if _, _, err := store.CreateBatch(ctx, first); err != nil {
		t.Fatalf("first CreateBatch: %v", err)
	}

	got, created, err := store.CreateBatch(ctx, newBatch(t, "same-key", nil, "+905550000002", "+905550000003"))
	if err != nil || created || got.ID != first.ID || got.Total != 2 {
		t.Fatalf("second CreateBatch = %+v, created %v, err %v; want the first batch", got, created, err)
	}
	if n := count(t, db, "notifications"); n != 1 {
		t.Errorf("%d notifications, want only the first batch's", n)
	}
}

// assertBatchRows checks that batch id has accepted notifications, each with one outbox row and one
// pending event.
func assertBatchRows(t *testing.T, db *pgxpool.Pool, id uuid.UUID, accepted int) {
	t.Helper()
	var batches, notifications, outbox, events int
	err := db.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM batches WHERE id = $1),
		(SELECT count(*) FROM notifications WHERE batch_id = $1 AND status = 'pending'),
		(SELECT count(*) FROM outbox o JOIN notifications n ON n.id = o.notification_id WHERE n.batch_id = $1),
		(SELECT count(*) FROM notification_events e JOIN notifications n ON n.id = e.notification_id
			WHERE n.batch_id = $1 AND e.from_status IS NULL AND e.to_status = 'pending')`, id).
		Scan(&batches, &notifications, &outbox, &events)
	if err != nil {
		t.Fatalf("counting batch rows: %v", err)
	}
	if batches != 1 || notifications != accepted || outbox != accepted || events != accepted {
		t.Errorf("batch rows = %d batch, %d notifications, %d outbox, %d events; want 1, %d, %d, %d",
			batches, notifications, outbox, events, accepted, accepted, accepted)
	}
}

func count(t *testing.T, db *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}
