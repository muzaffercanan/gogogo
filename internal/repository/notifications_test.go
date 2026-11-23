//go:build integration

package repository_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository"
	"github.com/muzaffercanan/gogogo/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &pg)) }

func TestCreateNotificationWritesNotificationOutboxRowAndEvent(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	n := newNotification(t, func(n *domain.NewNotification) {
		n.IdempotencyKey = "key-1"
		n.RequestHash = []byte{1, 2, 3}
		n.CorrelationID = "corr-1"
	})

	got, created, err := store.CreateNotification(ctx, n)
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if got.ID != n.ID || got.Status != domain.StatusPending || got.Attempts != 0 || got.MaxAttempts != n.MaxAttempts ||
		got.Channel != n.Channel || got.Priority != n.Priority || got.Recipient != n.Recipient || got.Content != n.Content ||
		got.IdempotencyKey != n.IdempotencyKey || !bytes.Equal(got.RequestHash, n.RequestHash) ||
		got.CorrelationID != n.CorrelationID || got.BatchID != nil || got.ScheduledAt != nil || got.CreatedAt.IsZero() {
		t.Errorf("stored notification = %+v, does not match input %+v", got, n)
	}

	var (
		payload  []byte
		availNow bool
	)
	err = db.QueryRow(ctx,
		`SELECT payload, available_at <= now() FROM outbox WHERE notification_id = $1`, n.ID,
	).Scan(&payload, &availNow)
	if err != nil {
		t.Fatalf("reading outbox row: %v", err)
	}
	var p map[string]string
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("decoding payload %s: %v", payload, err)
	}
	want := map[string]string{"channel": "sms", "priority": "high", "correlation_id": "corr-1"}
	if len(p) != len(want) || p["channel"] != want["channel"] || p["priority"] != want["priority"] ||
		p["correlation_id"] != want["correlation_id"] {
		t.Errorf("outbox payload = %v, want %v", p, want)
	}
	if !availNow {
		t.Error("outbox row of an unscheduled notification is not available yet")
	}

	var (
		from *string
		to   string
	)
	err = db.QueryRow(ctx,
		`SELECT from_status, to_status FROM notification_events WHERE notification_id = $1`, n.ID,
	).Scan(&from, &to)
	if err != nil {
		t.Fatalf("reading status event: %v", err)
	}
	if from != nil || to != string(domain.StatusPending) {
		t.Errorf("status event = %v -> %q, want <nil> -> %q", from, to, domain.StatusPending)
	}
	assertCounts(t, db, 1)
}

func TestCreateNotificationScheduledAtDelaysOutboxRow(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	// PostgreSQL stores microseconds; truncate so the round trip compares equal.
	at := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	n := newNotification(t, func(n *domain.NewNotification) { n.ScheduledAt = &at })

	got, _, err := repository.New(db).CreateNotification(ctx, n)
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if got.ScheduledAt == nil || !got.ScheduledAt.Equal(at) {
		t.Errorf("ScheduledAt = %v, want %v", got.ScheduledAt, at)
	}

	var availableAt time.Time
	if err := db.QueryRow(ctx, `SELECT available_at FROM outbox WHERE notification_id = $1`, n.ID).Scan(&availableAt); err != nil {
		t.Fatalf("reading outbox row: %v", err)
	}
	if !availableAt.Equal(at) {
		t.Errorf("outbox available_at = %v, want %v", availableAt, at)
	}
}

func TestCreateNotificationWithTakenIdempotencyKeyReturnsExisting(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	first := newNotification(t, func(n *domain.NewNotification) {
		n.IdempotencyKey = "same-key"
		n.RequestHash = []byte("first body")
	})
	second := newNotification(t, func(n *domain.NewNotification) {
		n.IdempotencyKey = "same-key"
		n.RequestHash = []byte("second body")
		n.Content = "a different message"
	})

	if _, _, err := store.CreateNotification(ctx, first); err != nil {
		t.Fatalf("first CreateNotification: %v", err)
	}
	got, created, err := store.CreateNotification(ctx, second)
	if err != nil {
		t.Fatalf("second CreateNotification: %v", err)
	}
	if created {
		t.Error("created = true for a taken idempotency key, want false")
	}
	if got.ID != first.ID || got.Content != first.Content || !bytes.Equal(got.RequestHash, first.RequestHash) {
		t.Errorf("returned %+v, want the first notification %+v", got, first)
	}
	assertCounts(t, db, 1)
}

func TestCreateNotificationWithoutIdempotencyKeyNeverConflicts(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	for range 2 {
		if _, created, err := store.CreateNotification(ctx, newNotification(t, nil)); err != nil || !created {
			t.Fatalf("CreateNotification: created = %v, err = %v; want true, nil", created, err)
		}
	}
	assertCounts(t, db, 2)
}

func TestCreateNotificationFailureWritesNothing(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	missingBatch := uuid.Must(uuid.NewV7())
	n := newNotification(t, func(n *domain.NewNotification) { n.BatchID = &missingBatch })

	if _, _, err := repository.New(db).CreateNotification(ctx, n); err == nil {
		t.Fatal("CreateNotification with a missing batch succeeded, want a foreign key error")
	}
	assertCounts(t, db, 0)
}

func TestCreateNotificationConcurrentSameKeyCreatesOnce(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	const callers = 10

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		ids     = map[uuid.UUID]bool{}
	)
	for range callers {
		n := newNotification(t, func(n *domain.NewNotification) { n.IdempotencyKey = "contended" })
		wg.Go(func() {
			got, ok, err := store.CreateNotification(ctx, n)
			if err != nil {
				t.Errorf("CreateNotification: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if ok {
				created++
			}
			ids[got.ID] = true
		})
	}
	wg.Wait()

	if created != 1 {
		t.Errorf("%d callers created the notification, want exactly 1", created)
	}
	if len(ids) != 1 {
		t.Errorf("callers got %d different notifications, want 1", len(ids))
	}
	assertCounts(t, db, 1)
}

func TestCreateNotificationWakesListeningRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := pg.NewDatabase(t)

	conn, err := db.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring connection: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN outbox"); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}

	if _, _, err := repository.New(db).CreateNotification(ctx, newNotification(t, nil)); err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
		t.Fatalf("waiting for the outbox notification: %v", err)
	}
}

// newNotification returns a valid notification with a fresh ID; modify adjusts it before it is returned.
func newNotification(t *testing.T, modify func(*domain.NewNotification)) domain.NewNotification {
	t.Helper()
	n := domain.NewNotification{
		ID:          uuid.Must(uuid.NewV7()),
		Channel:     domain.ChannelSMS,
		Priority:    domain.PriorityHigh,
		Recipient:   "+905551234567",
		Content:     "Doğrulama kodunuz: 482913",
		MaxAttempts: 5,
	}
	if modify != nil {
		modify(&n)
	}
	return n
}

// assertCounts checks that notifications, outbox rows and status events each have want rows.
func assertCounts(t *testing.T, db *pgxpool.Pool, want int) {
	t.Helper()
	for _, table := range []string{"notifications", "outbox", "notification_events"} {
		var got int
		if err := db.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&got); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if got != want {
			t.Errorf("%s has %d rows, want %d", table, got, want)
		}
	}
}
