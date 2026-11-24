//go:build integration

package outbox_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestReaperAndRelayRecoverStuckDeliveriesAndFailExhausted(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, vhost := connectedPublisher(t)
	log := slog.New(slog.NewTextHandler(t.Output(), nil))

	relay := outbox.NewRelay(store, pub, log)
	// A 30 s poll interval requires the reaper's NOTIFY outbox to wake the relay.
	outbox.SetTiming(relay, 100, 30*time.Second, 10*time.Millisecond, 50*time.Millisecond)

	reaper := outbox.NewReaper(store, log)
	// Batch size 2 with 3 expired rows and a 30 s scan interval requires the reaper to chain full
	// batches immediately.
	outbox.SetReaperTiming(reaper, 2, 60*time.Second, 30*time.Second)

	retrySMS := createStuckSending(ctx, t, store, db, domain.ChannelSMS, domain.PriorityHigh, 1, 5, 2*time.Minute)
	exhaustedEmail := createStuckSending(ctx, t, store, db, domain.ChannelEmail, domain.PriorityNormal, 5, 5, 2*time.Minute)
	retryPush := createStuckSending(ctx, t, store, db, domain.ChannelPush, domain.PriorityLow, 4, 5, 2*time.Minute)

	runCtx, cancel := context.WithCancel(ctx)
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case <-relayDone:
		case <-time.After(5 * time.Second):
			t.Error("relay Run did not return after its context was cancelled")
		}
	}()

	waitFor(t, "the relay listener is active", func() bool {
		var count int
		_ = db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN outbox'`).Scan(&count)
		return count > 0
	})

	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		reaper.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case <-reaperDone:
		case <-time.After(5 * time.Second):
			t.Error("reaper Run did not return after its context was cancelled")
		}
	}()

	ch := openChannel(t, dial(t, vhost))
	if d := receive(t, ch, queue.QueueName(domain.ChannelSMS, domain.PriorityHigh)); d.MessageId != retrySMS.String() {
		t.Errorf("sms.high message id = %q, want %q", d.MessageId, retrySMS)
	}
	if d := receive(t, ch, queue.QueueName(domain.ChannelPush, domain.PriorityLow)); d.MessageId != retryPush.String() {
		t.Errorf("push.low message id = %q, want %q", d.MessageId, retryPush)
	}
	assertEmpty(t, ch, queue.QueueName(domain.ChannelEmail, domain.PriorityNormal))

	for _, id := range []uuid.UUID{retrySMS, retryPush} {
		got, err := store.GetNotification(ctx, id)
		if err != nil {
			t.Fatalf("GetNotification(%s): %v", id, err)
		}
		if got.Status != domain.StatusQueued || got.LastError != "delivery lease expired" {
			t.Errorf("recovered notification %s = %+v; want status queued, last_error \"delivery lease expired\"", id, got)
		}
	}

	gotExhausted, err := store.GetNotification(ctx, exhaustedEmail)
	if err != nil {
		t.Fatalf("GetNotification(exhausted): %v", err)
	}
	if gotExhausted.Status != domain.StatusPermanentlyFailed || gotExhausted.LastError != "delivery lease expired" {
		t.Errorf("exhausted notification = %+v; want status permanently_failed, last_error \"delivery lease expired\"", gotExhausted)
	}
}

func createStuckSending(
	ctx context.Context,
	t *testing.T,
	store *repository.Store,
	db *pgxpool.Pool,
	channel domain.Channel,
	priority domain.Priority,
	attempts, maxAttempts int,
	age time.Duration,
) uuid.UUID {
	t.Helper()
	id := createNotification(ctx, t, store, channel, priority, nil)
	if _, err := db.Exec(ctx, `DELETE FROM outbox WHERE notification_id = $1`, id); err != nil {
		t.Fatalf("deleting initial outbox row: %v", err)
	}
	if _, err := db.Exec(ctx,
		`UPDATE notifications
		SET status = 'sending', attempts = $2, max_attempts = $3,
		    sending_started_at = now() - make_interval(secs => $4::double precision)
		WHERE id = $1`,
		id, attempts, maxAttempts, age.Seconds(),
	); err != nil {
		t.Fatalf("forcing sending state: %v", err)
	}
	return id
}
