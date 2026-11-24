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

func TestListenOutboxSignalsOnReadyAndNotifyAndDestroysSession(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)

	cfg := db.Config().Copy()
	cfg.MaxConns = 1
	singlePool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(singlePool.Close)

	listenCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wakes := make(chan struct{}, 4)
	done := make(chan error, 1)
	go func() {
		done <- repository.New(singlePool).ListenOutbox(listenCtx, func() {
			wakes <- struct{}{}
		})
	}()

	// Initial wakeup right after LISTEN outbox succeeds.
	select {
	case <-wakes:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the initial ListenOutbox wakeup")
	}

	if _, _, err := repository.New(db).CreateNotification(ctx, newNotification(t, nil)); err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	select {
	case <-wakes:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ListenOutbox wakeup after CreateNotification")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ListenOutbox after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenOutbox did not return after its context was cancelled")
	}

	var channels int
	if err := singlePool.QueryRow(ctx, `SELECT count(*) FROM pg_listening_channels()`).Scan(&channels); err != nil {
		t.Fatalf("checking listening channels: %v", err)
	}
	if channels != 0 {
		t.Errorf("pooled connection still listens on %d channel(s), want 0", channels)
	}
}

func TestReapExpiredDeliveriesRetriesUnderMaxAndFailsExhausted(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	if _, err := db.Exec(ctx, `DELETE FROM outbox`); err != nil {
		t.Fatalf("clearing outbox: %v", err)
	}

	retryID := createStuckSending(ctx, t, store, db, 2, 5, 65*time.Second, func(n *domain.NewNotification) {
		n.Channel = domain.ChannelEmail
		n.Priority = domain.PriorityHigh
		n.CorrelationID = "corr-reap"
	})
	exhaustedID := createStuckSending(ctx, t, store, db, 5, 5, 90*time.Second, nil)
	freshID := createStuckSending(ctx, t, store, db, 1, 5, 10*time.Second, nil)

	retried, failed, err := store.ReapExpiredDeliveries(ctx, 60*time.Second, 10)
	if err != nil {
		t.Fatalf("ReapExpiredDeliveries: %v", err)
	}
	if retried != 1 || failed != 1 {
		t.Fatalf("retried = %d, failed = %d; want 1, 1", retried, failed)
	}

	gotRetry, err := store.GetNotification(ctx, retryID)
	if err != nil {
		t.Fatalf("GetNotification(retry): %v", err)
	}
	if gotRetry.Status != domain.StatusRetryScheduled || gotRetry.Attempts != 2 || gotRetry.LastError != "delivery lease expired" {
		t.Errorf("retried notification = %+v; want retry_scheduled, attempts 2, last_error \"delivery lease expired\"", gotRetry)
	}
	if from, to, reason := lastEvent(ctx, t, db, retryID); from != string(domain.StatusSending) ||
		to != string(domain.StatusRetryScheduled) || reason != "lease_expired" {
		t.Errorf("retried last event = %q -> %q (%q), want sending -> retry_scheduled (lease_expired)", from, to, reason)
	}
	if active := leaseActive(ctx, t, db, retryID); active {
		t.Error("retried notification still has sending_started_at set")
	}

	gotExhausted, err := store.GetNotification(ctx, exhaustedID)
	if err != nil {
		t.Fatalf("GetNotification(exhausted): %v", err)
	}
	if gotExhausted.Status != domain.StatusPermanentlyFailed || gotExhausted.Attempts != 5 || gotExhausted.LastError != "delivery lease expired" {
		t.Errorf("exhausted notification = %+v; want permanently_failed, attempts 5, last_error \"delivery lease expired\"", gotExhausted)
	}
	if from, to, reason := lastEvent(ctx, t, db, exhaustedID); from != string(domain.StatusSending) ||
		to != string(domain.StatusPermanentlyFailed) || reason != "lease_expired" {
		t.Errorf("exhausted last event = %q -> %q (%q), want sending -> permanently_failed (lease_expired)", from, to, reason)
	}
	if active := leaseActive(ctx, t, db, exhaustedID); active {
		t.Error("exhausted notification still has sending_started_at set")
	}

	if s := statusOf(ctx, t, db, freshID); s != domain.StatusSending {
		t.Errorf("fresh sending status = %s, want sending", s)
	}
	if active := leaseActive(ctx, t, db, freshID); !active {
		t.Error("fresh sending notification lost its sending_started_at")
	}

	// Only the retryable notification gets a new outbox row, immediately due (available_at = updated_at).
	if n := outboxCount(ctx, t, db); n != 1 {
		t.Fatalf("outbox has %d rows, want 1", n)
	}
	var (
		outboxNotifID uuid.UUID
		dueNow        bool
	)
	if err := db.QueryRow(ctx,
		`SELECT o.notification_id, o.available_at = n.updated_at
		FROM outbox AS o JOIN notifications AS n ON n.id = o.notification_id`,
	).Scan(&outboxNotifID, &dueNow); err != nil {
		t.Fatalf("reading retry outbox row: %v", err)
	}
	if outboxNotifID != retryID || !dueNow {
		t.Errorf("retry outbox row = (%s, dueNow=%v), want (%s, true)", outboxNotifID, dueNow, retryID)
	}

	if _, _, err := store.ReapExpiredDeliveries(ctx, 0, 10); err == nil {
		t.Error("ReapExpiredDeliveries with lease 0 succeeded, want an error")
	}
	if _, _, err := store.ReapExpiredDeliveries(ctx, time.Minute, 0); err == nil {
		t.Error("ReapExpiredDeliveries with limit 0 succeeded, want an error")
	}
}

func createStuckSending(
	ctx context.Context,
	t *testing.T,
	store *repository.Store,
	db *pgxpool.Pool,
	attempts, maxAttempts int,
	age time.Duration,
	modify func(*domain.NewNotification),
) uuid.UUID {
	t.Helper()
	nn := newNotification(t, func(n *domain.NewNotification) {
		n.MaxAttempts = maxAttempts
		if modify != nil {
			modify(n)
		}
	})
	n, _, err := store.CreateNotification(ctx, nn)
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM outbox WHERE notification_id = $1`, n.ID); err != nil {
		t.Fatalf("deleting initial outbox row: %v", err)
	}
	if _, err := db.Exec(ctx,
		`UPDATE notifications
		SET status = 'sending', attempts = $2,
		    sending_started_at = now() - make_interval(secs => $3::double precision)
		WHERE id = $1`,
		n.ID, attempts, age.Seconds(),
	); err != nil {
		t.Fatalf("forcing sending state: %v", err)
	}
	return n.ID
}

func leaseActive(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()
	var active bool
	if err := db.QueryRow(ctx, `SELECT sending_started_at IS NOT NULL FROM notifications WHERE id = $1`, id).Scan(&active); err != nil {
		t.Fatalf("reading lease: %v", err)
	}
	return active
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
