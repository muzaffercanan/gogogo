//go:build integration

package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestRelayOncePublishesDueNotificationsToTheirWorkQueues(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, vhost := connectedPublisher(t)
	reg := prometheus.NewRegistry()
	relay := outbox.NewRelay(store, pub, outbox.NewMetrics(reg), noop.NewTracerProvider(), slog.New(slog.NewTextHandler(t.Output(), nil)))

	pendingID := createNotification(ctx, t, store, domain.ChannelSMS, domain.PriorityHigh, nil)
	retryID := createNotification(ctx, t, store, domain.ChannelEmail, domain.PriorityLow, nil)
	if _, err := db.Exec(ctx, `UPDATE notifications SET status = $2 WHERE id = $1`, retryID, string(domain.StatusRetryScheduled)); err != nil {
		t.Fatalf("forcing retry_scheduled: %v", err)
	}
	cancelledID := createNotification(ctx, t, store, domain.ChannelPush, domain.PriorityNormal, nil)
	if _, err := store.TransitionNotification(ctx, repository.Transition{
		ID: cancelledID, From: domain.AllowedFrom(domain.StatusCancelled), To: domain.StatusCancelled,
	}); err != nil {
		t.Fatalf("cancelling notification: %v", err)
	}
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	scheduledID := createNotification(ctx, t, store, domain.ChannelSMS, domain.PriorityLow, &future)

	claimed, published, err := relay.Once(ctx)
	if err != nil {
		t.Fatalf("Once: %v", err)
	}
	if claimed != 3 || published != 2 {
		t.Fatalf("claimed = %d, published = %d; want 3, 2", claimed, published)
	}
	for _, want := range []struct {
		name   string
		labels []string
		value  float64
	}{
		{"outbox_published_total", []string{"channel", "sms", "priority", "high"}, 1},
		{"outbox_published_total", []string{"channel", "email", "priority", "low"}, 1},
		{"outbox_published_total", []string{"channel", "push", "priority", "normal"}, 0}, // cancelled
		{"outbox_published_total", []string{"channel", "sms", "priority", "low"}, 0},     // scheduled
		{"outbox_skipped_total", nil, 1},
		{"outbox_dispatch_duration_seconds", nil, 1},
		{"outbox_dispatch_errors_total", nil, 0},
	} {
		if v, ok := metricValue(t, reg, want.name, want.labels...); !ok || v != want.value {
			t.Errorf("%s%v = %v (present %v), want %v", want.name, want.labels, v, ok, want.value)
		}
	}
	// An idle step (nothing due) is not a dispatch and must not be timed.
	if claimed, _, err := relay.Once(ctx); err != nil || claimed != 0 {
		t.Fatalf("idle Once = (%d, %v), want (0, nil)", claimed, err)
	}
	if v, _ := metricValue(t, reg, "outbox_dispatch_duration_seconds"); v != 1 {
		t.Errorf("outbox_dispatch_duration_seconds count after an idle step = %v, want 1", v)
	}

	ch := openChannel(t, dial(t, vhost))
	for _, want := range []struct {
		id         uuid.UUID
		channel    domain.Channel
		priority   domain.Priority
		fromStatus domain.Status
	}{
		{pendingID, domain.ChannelSMS, domain.PriorityHigh, domain.StatusPending},
		{retryID, domain.ChannelEmail, domain.PriorityLow, domain.StatusRetryScheduled},
	} {
		qName := queue.QueueName(want.channel, want.priority)
		d := receive(t, ch, qName)
		if d.MessageId != want.id.String() {
			t.Errorf("%s message id = %q, want %q", qName, d.MessageId, want.id)
		}
		if d.DeliveryMode != amqp.Persistent || d.ContentType != "application/json" {
			t.Errorf("%s delivery mode %d content type %q, want persistent JSON", qName, d.DeliveryMode, d.ContentType)
		}
		var body outbox.MessageBody
		if err := json.Unmarshal(d.Body, &body); err != nil {
			t.Fatalf("decoding body %s: %v", d.Body, err)
		}
		if body.NotificationID != want.id {
			t.Errorf("%s body notification_id = %s, want %s", qName, body.NotificationID, want.id)
		}
		assertEmpty(t, ch, qName)

		got, err := store.GetNotification(ctx, want.id)
		if err != nil {
			t.Fatalf("GetNotification(%s): %v", want.id, err)
		}
		if got.Status != domain.StatusQueued {
			t.Errorf("notification %s status = %s, want queued", want.id, got.Status)
		}
		from, to := lastEvent(ctx, t, db, want.id)
		if from != string(want.fromStatus) || to != string(domain.StatusQueued) {
			t.Errorf("notification %s last event = %q -> %q, want %q -> queued", want.id, from, to, want.fromStatus)
		}
	}

	assertEmpty(t, ch, queue.QueueName(domain.ChannelPush, domain.PriorityNormal))
	assertEmpty(t, ch, queue.QueueName(domain.ChannelSMS, domain.PriorityLow))

	if got, err := store.GetNotification(ctx, cancelledID); err != nil || got.Status != domain.StatusCancelled {
		t.Errorf("cancelled notification = %+v, err = %v; want status cancelled", got, err)
	}
	if got, err := store.GetNotification(ctx, scheduledID); err != nil || got.Status != domain.StatusPending {
		t.Errorf("scheduled notification = %+v, err = %v; want status pending", got, err)
	}
	if n := outboxCount(ctx, t, db); n != 1 {
		t.Errorf("outbox has %d rows, want 1 (only the future scheduled row)", n)
	}
}

func TestRelayOnceRollsBackWhenBrokerRejectsPublish(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, vhost := connectedPublisher(t)
	reg := prometheus.NewRegistry()
	relay := outbox.NewRelay(store, pub, outbox.NewMetrics(reg), noop.NewTracerProvider(), slog.New(slog.NewTextHandler(t.Output(), nil)))

	id := createNotification(ctx, t, store, domain.ChannelEmail, domain.PriorityNormal, nil)

	// Delete the target work queue so the mandatory publish is returned as unroutable.
	qName := queue.QueueName(domain.ChannelEmail, domain.PriorityNormal)
	admin := openChannel(t, dial(t, vhost))
	if _, err := admin.QueueDelete(qName, false, false, false); err != nil {
		t.Fatalf("deleting %s: %v", qName, err)
	}

	if _, _, err := relay.Once(ctx); err == nil {
		t.Fatalf("Once with %s deleted succeeded, want an error", qName)
	}
	assertRelayCounts := func(when string, errors, published, steps float64) {
		t.Helper()
		if v, _ := metricValue(t, reg, "outbox_dispatch_errors_total"); v != errors {
			t.Errorf("%s: outbox_dispatch_errors_total = %v, want %v", when, v, errors)
		}
		if v, _ := metricValue(t, reg, "outbox_published_total", "channel", "email", "priority", "normal"); v != published {
			t.Errorf("%s: outbox_published_total{email,normal} = %v, want %v", when, v, published)
		}
		if v, _ := metricValue(t, reg, "outbox_dispatch_duration_seconds"); v != steps {
			t.Errorf("%s: outbox_dispatch_duration_seconds count = %v, want %v", when, v, steps)
		}
	}
	assertRelayCounts("after rollback", 1, 0, 0)

	got, err := store.GetNotification(ctx, id)
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if got.Status != domain.StatusPending {
		t.Errorf("status after failed publish = %s, want pending", got.Status)
	}
	if from, to := lastEvent(ctx, t, db, id); from != "" || to != string(domain.StatusPending) {
		t.Errorf("last event after rollback = %q -> %q, want \"\" -> pending", from, to)
	}
	if n := outboxCount(ctx, t, db); n != 1 {
		t.Errorf("outbox has %d rows after rollback, want 1", n)
	}

	// Redeclare the topology and verify the next step dispatches the notification.
	if err := queue.Declare(ctx, admin); err != nil {
		t.Fatalf("redeclaring topology: %v", err)
	}
	claimed, published, err := relay.Once(ctx)
	if err != nil || claimed != 1 || published != 1 {
		t.Fatalf("Once after recovery = (%d, %d, %v), want (1, 1, nil)", claimed, published, err)
	}
	assertRelayCounts("after recovery", 1, 1, 1)
	d := receive(t, admin, qName)
	if d.MessageId != id.String() {
		t.Errorf("received message id = %q, want %q", d.MessageId, id)
	}
}

func TestRelayRunDrainsMultiBatchBacklogAndRecoversFromOutage(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	vhost := mq.NewVHost(t)
	p, url := newProxy(t, vhost, false)
	sup := supervise(t, url)
	pub := queue.NewPublisher(sup)

	relay := outbox.NewRelay(store, pub, discardMetrics(), noop.NewTracerProvider(), slog.New(slog.NewTextHandler(t.Output(), nil)))
	// Poll interval is 30 s so draining 7 rows with batch size 3 must chain full batches immediately
	// rather than waiting for the next poll tick.
	outbox.SetTiming(relay, 3, 30*time.Second, 10*time.Millisecond, 50*time.Millisecond)

	const total = 7
	ids := make([]uuid.UUID, total)
	for i := range total {
		ids[i] = createNotification(ctx, t, store, domain.ChannelSMS, domain.PriorityNormal, nil)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("relay Run did not return after its context was cancelled")
		}
	}()

	// Let the relay fail a few steps while the broker is unreachable.
	time.Sleep(150 * time.Millisecond)
	if n := outboxCount(ctx, t, db); n != total {
		t.Fatalf("outbox count while broker is down = %d, want %d", n, total)
	}

	// Bring the broker up; all 3 batches (3 + 3 + 1) must drain without waiting for the 30 s poll.
	p.set(true)
	waitFor(t, "the outbox drains across three batches", func() bool {
		return outboxCount(ctx, t, db) == 0
	})

	ch := openChannel(t, dial(t, vhost))
	qName := queue.QueueName(domain.ChannelSMS, domain.PriorityNormal)
	for _, wantID := range ids {
		d := receive(t, ch, qName)
		if d.MessageId != wantID.String() {
			t.Errorf("received message id = %q, want %q (FIFO order)", d.MessageId, wantID)
		}
		got, err := store.GetNotification(ctx, wantID)
		if err != nil || got.Status != domain.StatusQueued {
			t.Errorf("notification %s = %+v, err = %v; want status queued", wantID, got, err)
		}
	}
	assertEmpty(t, ch, qName)
}

func TestRelayRunWakesOnNotifyWithoutWaitingForPoll(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, vhost := connectedPublisher(t)
	relay := outbox.NewRelay(store, pub, discardMetrics(), noop.NewTracerProvider(), slog.New(slog.NewTextHandler(t.Output(), nil)))
	// A 30 s poll interval guarantees only LISTEN/NOTIFY can wake the relay within receive's 5 s timeout.
	outbox.SetTiming(relay, 100, 30*time.Second, 10*time.Millisecond, 50*time.Millisecond)

	firstID := createNotification(ctx, t, store, domain.ChannelSMS, domain.PriorityHigh, nil)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("relay Run did not return after its context was cancelled")
		}
	}()

	ch := openChannel(t, dial(t, vhost))
	qName := queue.QueueName(domain.ChannelSMS, domain.PriorityHigh)
	if d := receive(t, ch, qName); d.MessageId != firstID.String() {
		t.Fatalf("first message id = %q, want %q", d.MessageId, firstID)
	}

	// Ensure the listener session is active in PostgreSQL before committing the second notification.
	waitFor(t, "the relay listener is active", func() bool {
		var count int
		_ = db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE query = 'LISTEN outbox'`).Scan(&count)
		return count > 0
	})

	secondID := createNotification(ctx, t, store, domain.ChannelSMS, domain.PriorityHigh, nil)
	if d := receive(t, ch, qName); d.MessageId != secondID.String() {
		t.Errorf("second message id = %q, want %q", d.MessageId, secondID)
	}
}

func TestRelayRunPollingFallbackDispatchesScheduledRowWhenDue(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, vhost := connectedPublisher(t)
	relay := outbox.NewRelay(store, pub, discardMetrics(), noop.NewTracerProvider(), slog.New(slog.NewTextHandler(t.Output(), nil)))
	outbox.SetTiming(relay, 100, 50*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)

	// NOTIFY fires at creation while available_at is still in the future; no new NOTIFY fires when the
	// scheduled time arrives, so only the polling fallback can pick the row up once it becomes due.
	dueAt := time.Now().UTC().Add(250 * time.Millisecond).Truncate(time.Microsecond)
	id := createNotification(ctx, t, store, domain.ChannelPush, domain.PriorityHigh, &dueAt)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("relay Run did not return after its context was cancelled")
		}
	}()

	ch := openChannel(t, dial(t, vhost))
	qName := queue.QueueName(domain.ChannelPush, domain.PriorityHigh)
	if d := receive(t, ch, qName); d.MessageId != id.String() {
		t.Errorf("received message id = %q, want %q", d.MessageId, id)
	}
}

func TestBacklogCollectorReportsDueAndScheduledOutboxRows(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, _ := connectedPublisher(t)
	log := slog.New(slog.NewTextHandler(t.Output(), nil))
	relay := outbox.NewRelay(store, pub, discardMetrics(), noop.NewTracerProvider(), log)

	reg := prometheus.NewRegistry()
	reg.MustRegister(outbox.NewBacklogCollector(store, log))

	future := time.Now().UTC().Add(time.Hour)
	createNotification(ctx, t, store, domain.ChannelSMS, domain.PriorityHigh, nil)
	createNotification(ctx, t, store, domain.ChannelEmail, domain.PriorityLow, nil)
	createNotification(ctx, t, store, domain.ChannelPush, domain.PriorityNormal, &future)

	assertBacklog := func(when string, due, scheduled float64) {
		t.Helper()
		for _, want := range []struct {
			state string
			value float64
		}{{"due", due}, {"scheduled", scheduled}} {
			if v, ok := metricValue(t, reg, "outbox_backlog", "state", want.state); !ok || v != want.value {
				t.Errorf("%s: outbox_backlog{state=%q} = %v (present %v), want %v", when, want.state, v, ok, want.value)
			}
		}
		if v, ok := metricValue(t, reg, "outbox_backlog_query_errors_total"); !ok || v != 0 {
			t.Errorf("%s: outbox_backlog_query_errors_total = %v (present %v), want 0", when, v, ok)
		}
	}
	assertBacklog("before dispatch", 2, 1)

	if claimed, published, err := relay.Once(ctx); err != nil || claimed != 2 || published != 2 {
		t.Fatalf("relay.Once = (%d, %d, %v), want (2, 2, nil)", claimed, published, err)
	}
	assertBacklog("after dispatch", 0, 1)
}

func TestCorrelationIDPropagatesThroughOutboxHeadersAndLogs(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	pub, vhost := connectedPublisher(t)

	var logBuf bytes.Buffer
	log := logging.New(&logBuf, "info")
	relay := outbox.NewRelay(store, pub, discardMetrics(), noop.NewTracerProvider(), log)
	reaper := outbox.NewReaper(store, discardMetrics(), log)

	const corrID = "corr-e2e-99"
	id := uuid.Must(uuid.NewV7())
	if _, _, err := store.CreateNotification(ctx, domain.NewNotification{
		ID:            id,
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityHigh,
		Recipient:     "+905551234567",
		Content:       "hello",
		MaxAttempts:   5,
		CorrelationID: corrID,
	}); err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}

	if _, _, err := relay.Once(ctx); err != nil {
		t.Fatalf("first relay.Once: %v", err)
	}

	ch := openChannel(t, dial(t, vhost))
	qName := queue.QueueName(domain.ChannelSMS, domain.PriorityHigh)
	firstDelivery := receive(t, ch, qName)
	if got := queue.CorrelationID(firstDelivery.Headers); got != corrID {
		t.Errorf("first delivery correlation_id header = %q, want %q", got, corrID)
	}
	assertLogEntry(t, logBuf.String(), "notification queued", id.String(), corrID)

	// Simulate the worker starting delivery and getting stuck past the 60 s lease.
	if _, err := db.Exec(ctx,
		`UPDATE notifications
		SET status = 'sending', attempts = 1, sending_started_at = now() - interval '2 minutes'
		WHERE id = $1`, id,
	); err != nil {
		t.Fatalf("forcing stuck sending: %v", err)
	}

	logBuf.Reset()
	if retried, failed, err := reaper.Once(ctx); err != nil || retried != 1 || failed != 0 {
		t.Fatalf("reaper.Once = (%d, %d, %v), want (1, 0, nil)", retried, failed, err)
	}
	assertLogEntry(t, logBuf.String(), "reaped expired delivery", id.String(), corrID)

	logBuf.Reset()
	if _, _, err := relay.Once(ctx); err != nil {
		t.Fatalf("second relay.Once: %v", err)
	}
	assertLogEntry(t, logBuf.String(), "notification queued", id.String(), corrID)

	retryDelivery := receive(t, ch, qName)
	if got := queue.CorrelationID(retryDelivery.Headers); got != corrID {
		t.Errorf("retry delivery correlation_id header = %q, want %q", got, corrID)
	}

	var workerBuf bytes.Buffer
	workerLog := logging.New(&workerBuf, "info")
	workerCtx := queue.WithCorrelationID(ctx, retryDelivery.Headers)
	workerLog.InfoContext(workerCtx, "worker received delivery", "notification_id", retryDelivery.MessageId)
	assertLogEntry(t, workerBuf.String(), "worker received delivery", id.String(), corrID)
}

func assertLogEntry(t *testing.T, output, wantMsg, wantNotificationID, wantCorrelationID string) {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decoding log line %q: %v", line, err)
		}
		if entry["msg"] == wantMsg && entry["notification_id"] == wantNotificationID && entry["correlation_id"] == wantCorrelationID {
			return
		}
	}
	t.Errorf("log output %s did not contain msg=%q notification_id=%q correlation_id=%q",
		output, wantMsg, wantNotificationID, wantCorrelationID)
}

func createNotification(
	ctx context.Context,
	t *testing.T,
	store *repository.Store,
	channel domain.Channel,
	priority domain.Priority,
	scheduledAt *time.Time,
) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	_, _, err := store.CreateNotification(ctx, domain.NewNotification{
		ID:          id,
		Channel:     channel,
		Priority:    priority,
		Recipient:   "+905551234567",
		Content:     "hello",
		MaxAttempts: 5,
		ScheduledAt: scheduledAt,
	})
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	return id
}

func lastEvent(ctx context.Context, t *testing.T, db *pgxpool.Pool, id uuid.UUID) (from, to string) {
	t.Helper()
	err := db.QueryRow(ctx, `SELECT coalesce(from_status, ''), to_status
		FROM notification_events WHERE notification_id = $1 ORDER BY id DESC LIMIT 1`, id).Scan(&from, &to)
	if err != nil {
		t.Fatalf("reading last event: %v", err)
	}
	return from, to
}

func outboxCount(ctx context.Context, t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatalf("counting outbox: %v", err)
	}
	return n
}
