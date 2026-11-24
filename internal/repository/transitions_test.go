//go:build integration

package repository_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestTransitionNotification(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	tests := []struct {
		name           string
		start          domain.Status
		transition     repository.Transition
		wantAttempts   int
		wantLease      bool
		wantSentAt     bool
		wantLastError  string
		wantProviderID string
	}{
		{
			name:       "pending to queued",
			start:      domain.StatusPending,
			transition: repository.Transition{From: domain.AllowedFrom(domain.StatusQueued), To: domain.StatusQueued},
		},
		{
			name:       "retry_scheduled to queued records the actual source status",
			start:      domain.StatusRetryScheduled,
			transition: repository.Transition{From: domain.AllowedFrom(domain.StatusQueued), To: domain.StatusQueued},
		},
		{
			name:         "queued to sending counts an attempt and starts the lease",
			start:        domain.StatusQueued,
			transition:   repository.Transition{From: domain.AllowedFrom(domain.StatusSending), To: domain.StatusSending},
			wantAttempts: 1,
			wantLease:    true,
		},
		{
			name:           "sending to sent stores the provider id and ends the lease",
			start:          domain.StatusSending,
			transition:     repository.Transition{From: domain.AllowedFrom(domain.StatusSent), To: domain.StatusSent, ProviderMessageID: "pm-1"},
			wantSentAt:     true,
			wantProviderID: "pm-1",
		},
		{
			name:  "sending to retry_scheduled keeps the error and ends the lease",
			start: domain.StatusSending,
			transition: repository.Transition{
				From: domain.AllowedFrom(domain.StatusRetryScheduled), To: domain.StatusRetryScheduled,
				LastError: "provider returned 503", Reason: "retryable provider error",
			},
			wantLastError: "provider returned 503",
		},
		{
			name:       "queued to cancelled",
			start:      domain.StatusQueued,
			transition: repository.Transition{From: domain.AllowedFrom(domain.StatusCancelled), To: domain.StatusCancelled, Reason: "cancelled by client"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := createInStatus(ctx, t, store, db, tt.start)
			tr := tt.transition
			tr.ID = id

			got, err := store.TransitionNotification(ctx, tr)
			if err != nil {
				t.Fatalf("TransitionNotification: %v", err)
			}
			if got.ID != id || got.Status != tr.To || got.Attempts != tt.wantAttempts ||
				(got.SentAt != nil) != tt.wantSentAt || got.LastError != tt.wantLastError ||
				got.ProviderMessageID != tt.wantProviderID {
				t.Errorf("got %+v; want status %s, attempts %d, sent_at set %v, last error %q, provider id %q",
					got, tr.To, tt.wantAttempts, tt.wantSentAt, tt.wantLastError, tt.wantProviderID)
			}

			var leaseActive bool
			if err := db.QueryRow(ctx, `SELECT sending_started_at IS NOT NULL FROM notifications WHERE id = $1`, id).Scan(&leaseActive); err != nil {
				t.Fatalf("reading lease: %v", err)
			}
			if leaseActive != tt.wantLease {
				t.Errorf("lease active = %v, want %v", leaseActive, tt.wantLease)
			}

			from, to, reason := lastEvent(ctx, t, db, id)
			if from != string(tt.start) || to != string(tr.To) || reason != tr.Reason {
				t.Errorf("last event = %q -> %q (%q), want %q -> %q (%q)", from, to, reason, tt.start, tr.To, tr.Reason)
			}
		})
	}
}

func TestTransitionNotificationFromOtherStatusIsRefused(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := createInStatus(ctx, t, store, db, domain.StatusSent)

	_, err := store.TransitionNotification(ctx, repository.Transition{ID: id, From: domain.AllowedFrom(domain.StatusCancelled), To: domain.StatusCancelled})

	var conflict *domain.StatusConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error = %v, want a *domain.StatusConflictError", err)
	}
	if conflict.ID != id || conflict.Current != domain.StatusSent || conflict.Wanted != domain.StatusCancelled {
		t.Errorf("conflict = %+v, want current sent, wanted cancelled", conflict)
	}
	if status := statusOf(ctx, t, db, id); status != domain.StatusSent {
		t.Errorf("status = %s after a refused transition, want sent", status)
	}
	if n := eventCount(ctx, t, db, id); n != 1 {
		t.Errorf("%d status events, want only the creation event", n)
	}
}

func TestTransitionNotificationUnknownID(t *testing.T) {
	store := repository.New(pg.NewDatabase(t))

	_, err := store.TransitionNotification(context.Background(), repository.Transition{
		ID: uuid.Must(uuid.NewV7()), From: domain.AllowedFrom(domain.StatusCancelled), To: domain.StatusCancelled,
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("error = %v, want domain.ErrNotFound", err)
	}
}

// TestTransitionNotificationRejectsInvalidTransition stores each notification in a status listed in
// From, so the SQL guard alone would apply the change: only the state machine check refuses it.
func TestTransitionNotificationRejectsInvalidTransition(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	tests := []struct {
		name  string
		start domain.Status
		from  []domain.Status
		to    domain.Status
	}{
		{"cancel a sent notification", domain.StatusSent, []domain.Status{domain.StatusSent}, domain.StatusCancelled},
		{"skip sending", domain.StatusQueued, []domain.Status{domain.StatusQueued}, domain.StatusSent},
		{"requeue a permanent failure", domain.StatusPermanentlyFailed, []domain.Status{domain.StatusPermanentlyFailed}, domain.StatusQueued},
		{"one invalid status among valid ones", domain.StatusSent, []domain.Status{domain.StatusQueued, domain.StatusSent}, domain.StatusCancelled},
		{"back to pending", domain.StatusQueued, []domain.Status{domain.StatusQueued}, domain.StatusPending},
		{"no source status", domain.StatusPending, nil, domain.StatusCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := createInStatus(ctx, t, store, db, tt.start)

			_, err := store.TransitionNotification(ctx, repository.Transition{ID: id, From: tt.from, To: tt.to})
			if !errors.Is(err, domain.ErrInvalidTransition) {
				t.Fatalf("error = %v, want domain.ErrInvalidTransition", err)
			}
			if status := statusOf(ctx, t, db, id); status != tt.start {
				t.Errorf("status = %s after a refused transition, want %s", status, tt.start)
			}
			if n := eventCount(ctx, t, db, id); n != 1 {
				t.Errorf("%d status events, want only the creation event", n)
			}
		})
	}
}

// TestTransitionNotificationRaceHasOneWinner races workers starting delivery against clients cancelling
// the same queued notification: exactly one of them may succeed.
func TestTransitionNotificationRaceHasOneWinner(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := createInStatus(ctx, t, store, db, domain.StatusQueued)
	const callers = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners []domain.Status
	)
	for i := range callers {
		tr := repository.Transition{ID: id, From: domain.AllowedFrom(domain.StatusSending), To: domain.StatusSending}
		if i%2 == 1 {
			tr = repository.Transition{ID: id, From: domain.AllowedFrom(domain.StatusCancelled), To: domain.StatusCancelled}
		}
		wg.Go(func() {
			got, err := store.TransitionNotification(ctx, tr)
			var conflict *domain.StatusConflictError
			switch {
			case err == nil:
				mu.Lock()
				winners = append(winners, got.Status)
				mu.Unlock()
			case !errors.As(err, &conflict):
				t.Errorf("TransitionNotification to %s: %v", tr.To, err)
			}
		})
	}
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("%d transitions succeeded (%v), want exactly 1", len(winners), winners)
	}
	if status := statusOf(ctx, t, db, id); status != winners[0] {
		t.Errorf("stored status = %s, want the winner's %s", status, winners[0])
	}
	if n := eventCount(ctx, t, db, id); n != 2 {
		t.Errorf("%d status events, want 2 (creation and the winning transition)", n)
	}
}

// moveUncommitted starts a transaction that moves notification id to status and returns it uncommitted,
// as the relay holds the queued status of the notifications it is publishing until its commit.
func moveUncommitted(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID, status domain.Status) pgx.Tx {
	t.Helper()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) }) // a no-op once the test has committed
	if _, err := tx.Exec(ctx, `UPDATE notifications SET status = $2 WHERE id = $1`, id, string(status)); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestTransitionNotificationWaitsForATransactionMovingItIntoAnAllowedStatus(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := createInStatus(ctx, t, store, db, domain.StatusRetryScheduled)
	relay := moveUncommitted(ctx, t, db, id, domain.StatusQueued)

	type result struct {
		n   domain.Notification
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := store.TransitionNotification(ctx, repository.Transition{ID: id, From: []domain.Status{domain.StatusQueued}, To: domain.StatusSending})
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("TransitionNotification returned before the concurrent transaction committed: %+v, %v", r.n.Status, r.err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := relay.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil || r.n.Status != domain.StatusSending {
			t.Errorf("TransitionNotification after the commit = %s, %v; want sending", r.n.Status, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("TransitionNotification did not return after the commit")
	}
	if from, to, _ := lastEvent(ctx, t, db, id); from != "queued" || to != "sending" {
		t.Errorf("last event %s -> %s, want queued -> sending", from, to)
	}
}

func TestTransitionNotificationRefusesWhatAConcurrentTransactionMovedAway(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	id := createInStatus(ctx, t, store, db, domain.StatusQueued)
	cancel := moveUncommitted(ctx, t, db, id, domain.StatusCancelled)

	done := make(chan error, 1)
	go func() {
		_, err := store.TransitionNotification(ctx, repository.Transition{ID: id, From: []domain.Status{domain.StatusQueued}, To: domain.StatusSending})
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	if err := cancel.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var conflict *domain.StatusConflictError
	if err := <-done; !errors.As(err, &conflict) || conflict.Current != domain.StatusCancelled {
		t.Errorf("TransitionNotification after a concurrent cancel = %v, want a conflict with cancelled", err)
	}
}

// createInStatus stores a notification and forces it into status, as if earlier transitions had happened.
// Its outbox row is moved a minute into the past: a row written with available_at = now() is due at
// once, but a test that claims it right after cannot rely on that, because on Docker Desktop the
// database clock is set back by up to a second every half minute or so.
func createInStatus(ctx context.Context, t *testing.T, store *repository.Store, db *pgxpool.Pool, status domain.Status) uuid.UUID {
	t.Helper()
	n, _, err := store.CreateNotification(ctx, newNotification(t, nil))
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	_, err = db.Exec(ctx, `UPDATE notifications
		SET status = $2, sending_started_at = CASE WHEN $2 = 'sending' THEN now() END
		WHERE id = $1`, n.ID, string(status))
	if err != nil {
		t.Fatalf("forcing status %s: %v", status, err)
	}
	if _, err := db.Exec(ctx, `UPDATE outbox SET available_at = now() - interval '1 minute' WHERE notification_id = $1`, n.ID); err != nil {
		t.Fatalf("making the outbox row due: %v", err)
	}
	return n.ID
}

func lastEvent(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID) (from, to, reason string) {
	t.Helper()
	err := db.QueryRow(ctx, `SELECT coalesce(from_status, ''), to_status, coalesce(reason, '')
		FROM notification_events WHERE notification_id = $1 ORDER BY id DESC LIMIT 1`, id).Scan(&from, &to, &reason)
	if err != nil {
		t.Fatalf("reading last event: %v", err)
	}
	return from, to, reason
}

func eventCount(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM notification_events WHERE notification_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("counting events: %v", err)
	}
	return n
}

func statusOf(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID) domain.Status {
	t.Helper()
	var s string
	if err := db.QueryRow(ctx, `SELECT status FROM notifications WHERE id = $1`, id).Scan(&s); err != nil {
		t.Fatalf("reading status: %v", err)
	}
	return domain.Status(s)
}
