//go:build integration

package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// sendingWithoutOutbox returns a notification in sending whose creation outbox row is gone, as after the
// relay dispatched it.
func sendingWithoutOutbox(ctx context.Context, t *testing.T, store *repository.Store, db *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := createInStatus(ctx, t, store, db, domain.StatusSending)
	if _, err := db.Exec(ctx, `DELETE FROM outbox WHERE notification_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	return id
}

// retryRow returns how far after the database clock the notification's outbox row becomes due, and its
// payload.
func retryRow(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID) (time.Duration, map[string]string) {
	t.Helper()
	var (
		seconds float64
		payload []byte
	)
	err := db.QueryRow(ctx, `SELECT extract(epoch FROM available_at - now()), payload FROM outbox WHERE notification_id = $1`, id).
		Scan(&seconds, &payload)
	if err != nil {
		t.Fatalf("reading the retry outbox row: %v", err)
	}
	var p map[string]string
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("decoding the retry payload %s: %v", payload, err)
	}
	return time.Duration(seconds * float64(time.Second)), p
}

func TestScheduleRetryMovesSendingToRetryScheduledWithAnOutboxRowAfterTheDelay(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := sendingWithoutOutbox(ctx, t, store, db)

	n, err := store.ScheduleRetry(ctx, repository.Retry{ID: id, Delay: 30 * time.Second, LastError: "provider answered 503", Reason: "retryable failure"})
	if err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}
	if n.Status != domain.StatusRetryScheduled || n.LastError != "provider answered 503" {
		t.Errorf("ScheduleRetry returned status %s, last error %q; want retry_scheduled with the error", n.Status, n.LastError)
	}
	if leaseActive(ctx, t, db, id) {
		t.Error("the sending lease is still set")
	}
	if from, to, reason := lastEvent(ctx, t, db, id); from != "sending" || to != "retry_scheduled" || reason != "retryable failure" {
		t.Errorf("last event %s -> %s (%q), want sending -> retry_scheduled (retryable failure)", from, to, reason)
	}
	// The delay is measured on the database clock, the clock the relay compares available_at against.
	due, payload := retryRow(ctx, t, db, id)
	if due < 29*time.Second || due > 30*time.Second {
		t.Errorf("the retry row is due %v after the database clock, want just under 30s", due)
	}
	if payload["channel"] != string(n.Channel) || payload["priority"] != string(n.Priority) {
		t.Errorf("retry payload %v, want the notification's channel %s and priority %s", payload, n.Channel, n.Priority)
	}
}

func TestScheduleRetryWithoutDelayIsDueAtOnce(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := sendingWithoutOutbox(ctx, t, store, db)

	if _, err := store.ScheduleRetry(ctx, repository.Retry{ID: id, LastError: "timeout"}); err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}
	if due, _ := retryRow(ctx, t, db, id); due > 0 {
		t.Errorf("the retry row is due %v after the database clock, want now", due)
	}
}

func TestScheduleRetryOfANotificationNotSendingWritesNothing(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := createInStatus(ctx, t, store, db, domain.StatusCancelled)
	if _, err := db.Exec(ctx, `DELETE FROM outbox WHERE notification_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	events := eventCount(ctx, t, db, id)

	_, err := store.ScheduleRetry(ctx, repository.Retry{ID: id, Delay: time.Second, LastError: "provider answered 500"})
	var conflict *domain.StatusConflictError
	if !errors.As(err, &conflict) || conflict.Current != domain.StatusCancelled {
		t.Fatalf("ScheduleRetry of a cancelled notification = %v, want a status conflict with cancelled", err)
	}
	if n := outboxCount(ctx, t, db); n != 0 {
		t.Errorf("outbox holds %d rows, want none", n)
	}
	if got := eventCount(ctx, t, db, id); got != events || statusOf(ctx, t, db, id) != domain.StatusCancelled {
		t.Errorf("the refused retry changed the notification")
	}
}
