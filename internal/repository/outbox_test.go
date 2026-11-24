//go:build integration

package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestDispatchOutboxQueuesDueNotificationsBeforePublishAndDeletesRows(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	now := time.Now().UTC().Truncate(time.Microsecond)
	older := now.Add(-2 * time.Minute)
	newer := now.Add(-time.Minute)
	future := now.Add(time.Hour)

	first := createWithAvailableAt(ctx, t, store, db, domain.StatusPending, older, func(n *domain.NewNotification) {
		n.Channel = domain.ChannelEmail
		n.Priority = domain.PriorityNormal
		n.CorrelationID = "corr-first"
	})
	second := createWithAvailableAt(ctx, t, store, db, domain.StatusRetryScheduled, newer, func(n *domain.NewNotification) {
		n.Channel = domain.ChannelPush
		n.Priority = domain.PriorityLow
		n.CorrelationID = "corr-second"
	})
	scheduled := createWithAvailableAt(ctx, t, store, db, domain.StatusPending, future, nil)

	var got []repository.OutboxItem
	claimed, published, err := store.DispatchOutbox(ctx, 10, func(ctx context.Context, items []repository.OutboxItem) error {
		got = append(got, items...)
		// Status changes happen before publish inside the open transaction, so a concurrent transaction
		// trying NOWAIT on the notification row sees the row lock held during publish.
		var lockedID uuid.UUID
		err := db.QueryRow(ctx, `SELECT id FROM notifications WHERE id = $1 FOR UPDATE NOWAIT`, first).Scan(&lockedID)
		if err == nil {
			t.Error("notification row was not locked in the transaction while publish was running")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DispatchOutbox: %v", err)
	}
	if claimed != 2 || published != 2 {
		t.Fatalf("claimed = %d, published = %d; want 2, 2", claimed, published)
	}

	want := []repository.OutboxItem{
		{NotificationID: first, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, CorrelationID: "corr-first"},
		{NotificationID: second, Channel: domain.ChannelPush, Priority: domain.PriorityLow, CorrelationID: "corr-second"},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("published items = %+v, want %+v", got, want)
	}

	if s := statusOf(ctx, t, db, first); s != domain.StatusQueued {
		t.Errorf("first status = %s, want queued", s)
	}
	if from, to, _ := lastEvent(ctx, t, db, first); from != string(domain.StatusPending) || to != string(domain.StatusQueued) {
		t.Errorf("first last event = %q -> %q, want pending -> queued", from, to)
	}
	if s := statusOf(ctx, t, db, second); s != domain.StatusQueued {
		t.Errorf("second status = %s, want queued", s)
	}
	if from, to, _ := lastEvent(ctx, t, db, second); from != string(domain.StatusRetryScheduled) || to != string(domain.StatusQueued) {
		t.Errorf("second last event = %q -> %q, want retry_scheduled -> queued", from, to)
	}
	if s := statusOf(ctx, t, db, scheduled); s != domain.StatusPending {
		t.Errorf("scheduled status = %s, want pending", s)
	}

	// Only the future scheduled row remains in the outbox.
	if n := outboxCount(ctx, t, db); n != 1 {
		t.Errorf("outbox has %d rows after dispatch, want 1 (the future row)", n)
	}
}

func TestDispatchOutboxDeletesCancelledRowsWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	cancelled := createInStatus(ctx, t, store, db, domain.StatusCancelled)
	pending := createInStatus(ctx, t, store, db, domain.StatusPending)

	var got []repository.OutboxItem
	claimed, published, err := store.DispatchOutbox(ctx, 10, func(_ context.Context, items []repository.OutboxItem) error {
		got = append(got, items...)
		return nil
	})
	if err != nil {
		t.Fatalf("DispatchOutbox: %v", err)
	}
	if claimed != 2 || published != 1 {
		t.Fatalf("claimed = %d, published = %d; want 2, 1", claimed, published)
	}
	if len(got) != 1 || got[0].NotificationID != pending {
		t.Errorf("published items = %+v, want only %s", got, pending)
	}
	if s := statusOf(ctx, t, db, cancelled); s != domain.StatusCancelled {
		t.Errorf("cancelled status = %s, want cancelled", s)
	}
	if n := eventCount(ctx, t, db, cancelled); n != 1 {
		t.Errorf("cancelled notification has %d events, want 1", n)
	}
	if n := outboxCount(ctx, t, db); n != 0 {
		t.Errorf("outbox has %d rows, want 0 (cancelled row deleted too)", n)
	}

	// When every claimed row is already cancelled, publish is not called at all.
	_ = createInStatus(ctx, t, store, db, domain.StatusCancelled)
	called := false
	claimed, published, err = store.DispatchOutbox(ctx, 10, func(context.Context, []repository.OutboxItem) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("DispatchOutbox with only cancelled rows: %v", err)
	}
	if claimed != 1 || published != 0 || called {
		t.Errorf("claimed = %d, published = %d, publish called = %v; want 1, 0, false", claimed, published, called)
	}
	if n := outboxCount(ctx, t, db); n != 0 {
		t.Errorf("outbox has %d rows, want 0", n)
	}
}

func TestDispatchOutboxRollsBackOnPublishError(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	id := createInStatus(ctx, t, store, db, domain.StatusPending)
	pubErr := errors.New("broker disconnected")

	claimed, published, err := store.DispatchOutbox(ctx, 10, func(context.Context, []repository.OutboxItem) error {
		return pubErr
	})
	if !errors.Is(err, pubErr) {
		t.Fatalf("DispatchOutbox error = %v, want %v", err, pubErr)
	}
	if claimed != 0 || published != 0 {
		t.Errorf("claimed = %d, published = %d on failure; want 0, 0", claimed, published)
	}
	if s := statusOf(ctx, t, db, id); s != domain.StatusPending {
		t.Errorf("status after rollback = %s, want pending", s)
	}
	if n := eventCount(ctx, t, db, id); n != 1 {
		t.Errorf("%d events after rollback, want 1", n)
	}
	if n := outboxCount(ctx, t, db); n != 1 {
		t.Errorf("%d outbox rows after rollback, want 1", n)
	}
}

func TestDispatchOutboxSkipsLockedRowsAndHonorsLimit(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	first := createWithAvailableAt(ctx, t, store, db, domain.StatusPending, base, nil)
	second := createWithAvailableAt(ctx, t, store, db, domain.StatusPending, base.Add(time.Second), nil)
	third := createWithAvailableAt(ctx, t, store, db, domain.StatusPending, base.Add(2*time.Second), nil)

	// Another relay holds the lock on the first outbox row.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var lockedID int64
	if err := tx.QueryRow(ctx, `SELECT id FROM outbox WHERE notification_id = $1 FOR UPDATE`, first).Scan(&lockedID); err != nil {
		t.Fatalf("locking first outbox row: %v", err)
	}

	var got []repository.OutboxItem
	claimed, published, err := store.DispatchOutbox(ctx, 1, func(_ context.Context, items []repository.OutboxItem) error {
		got = append(got, items...)
		return nil
	})
	if err != nil {
		t.Fatalf("DispatchOutbox: %v", err)
	}
	if claimed != 1 || published != 1 || len(got) != 1 || got[0].NotificationID != second {
		t.Errorf("claimed = %d, published = %d, got = %+v; want only second (%s)", claimed, published, got, second)
	}
	if s := statusOf(ctx, t, db, first); s != domain.StatusPending {
		t.Errorf("first status = %s, want pending", s)
	}
	if s := statusOf(ctx, t, db, third); s != domain.StatusPending {
		t.Errorf("third status = %s, want pending", s)
	}

	if _, _, err := store.DispatchOutbox(ctx, 0, func(context.Context, []repository.OutboxItem) error { return nil }); err == nil {
		t.Error("DispatchOutbox with limit 0 succeeded, want an error")
	}
}

func createWithAvailableAt(
	ctx context.Context,
	t *testing.T,
	store *repository.Store,
	db *pgxpool.Pool,
	status domain.Status,
	availableAt time.Time,
	modify func(*domain.NewNotification),
) uuid.UUID {
	t.Helper()
	nn := newNotification(t, modify)
	n, _, err := store.CreateNotification(ctx, nn)
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE notifications SET status = $2 WHERE id = $1`, n.ID, string(status)); err != nil {
		t.Fatalf("setting status: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE outbox SET available_at = $2 WHERE notification_id = $1`, n.ID, availableAt); err != nil {
		t.Fatalf("setting available_at: %v", err)
	}
	return n.ID
}

func outboxCount(ctx context.Context, t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("counting outbox: %v", err)
	}
	return n
}
