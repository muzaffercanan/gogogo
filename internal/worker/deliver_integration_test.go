//go:build integration

package worker_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestWorkerDeliversAQueuedNotificationAndRecordsItAsSent(t *testing.T) {
	h := newHarness(t, always(accepted("m-1")))
	stop := h.startWorker(t)
	n := h.create(t, nil)
	h.dispatch(t)

	got := h.waitStatus(t, n.ID, domain.StatusSent)
	if got.ProviderMessageID != "m-1" || got.Attempts != 1 || got.SentAt == nil {
		t.Errorf("sent notification has provider id %q, %d attempts, sent_at %v; want m-1, 1 and a time", got.ProviderMessageID, got.Attempts, got.SentAt)
	}
	if want := []string{"pending", "queued", "sending", "sent"}; !slices.Equal(h.statuses(t, n.ID), want) {
		t.Errorf("status events %v, want %v", h.statuses(t, n.ID), want)
	}
	reqs := h.provider.got()
	if len(reqs) != 1 || reqs[0].key != n.ID.String() {
		t.Fatalf("provider got %v, want one request with Idempotency-Key %s", reqs, n.ID)
	}
	var body map[string]string
	if err := json.Unmarshal([]byte(reqs[0].body), &body); err != nil || body["to"] != n.Recipient || body["content"] != n.Content || body["channel"] != "sms" {
		t.Errorf("provider got body %s, want the notification's recipient, content and channel", reqs[0].body)
	}
	ch := h.channel(t)
	assertAcknowledged(t, ch, stop)
	for _, m := range []struct {
		name   string
		labels []string
		want   float64
	}{
		{name: "notifications_sent_total", want: 1},
		{name: "notifications_failed_total", labels: []string{"final", "false"}, want: 0},
		{name: "provider_request_duration_seconds", want: 1},
		{name: "notification_e2e_latency_seconds", labels: []string{"priority", "high"}, want: 1},
		{name: "ratelimit_wait_seconds", want: 1},
	} {
		if got := metricWith(t, h.reg, m.name, m.labels...); got != m.want {
			t.Errorf("%s%v = %v, want %v", m.name, m.labels, got, m.want)
		}
	}
}

func TestWorkerSchedulesARetryAfterARetryableFailure(t *testing.T) {
	h := newHarness(t, always(answer{status: http.StatusServiceUnavailable, header: map[string]string{"Retry-After": "30"}, body: `{"error":"maintenance"}`}))
	h.startWorker(t)
	n := h.create(t, nil)
	h.dispatch(t)

	got := h.waitStatus(t, n.ID, domain.StatusRetryScheduled)
	if got.Attempts != 1 || got.LastError != "provider answered 503: maintenance" {
		t.Errorf("retry_scheduled with %d attempts and last error %q, want 1 and the provider's answer", got.Attempts, got.LastError)
	}
	if reason := h.lastReason(t, n.ID); reason != "retryable failure" {
		t.Errorf("last event reason %q, want retryable failure", reason)
	}
	// The policy's jitter is pinned to the top of the window (1 s after the first attempt), so the
	// provider's Retry-After of 30 s decides the delay.
	due, ok := h.outboxDue(t, n.ID)
	if !ok || due < 29*time.Second || due > 30*time.Second {
		t.Errorf("retry outbox row due in %v (present %t), want just under 30s", due, ok)
	}
	if depth := queueDepth(t, h.channel(t), "sms.dlq"); depth != 0 {
		t.Errorf("sms.dlq holds %d messages, want none", depth)
	}
	if got := metricWith(t, h.reg, "notifications_failed_total", "final", "false"); got != 1 {
		t.Errorf("notifications_failed_total{final=false} = %v, want 1", got)
	}
	if got := metricWith(t, h.reg, "retries_total", "reason", "5xx"); got != 1 {
		t.Errorf("retries_total{reason=5xx} = %v, want 1", got)
	}
	if got := metricWith(t, h.reg, "notifications_sent_total"); got != 0 {
		t.Errorf("notifications_sent_total = %v, want 0", got)
	}
}

func TestWorkerFailsARejectedNotificationAndDeadLettersIt(t *testing.T) {
	h := newHarness(t, always(answer{status: http.StatusBadRequest, body: `{"error":"recipient rejected"}`}))
	stop := h.startWorker(t)
	n := h.create(t, nil)
	h.dispatch(t)

	got := h.waitStatus(t, n.ID, domain.StatusPermanentlyFailed)
	if got.LastError != "provider answered 400: recipient rejected" || h.lastReason(t, n.ID) != "permanent failure" {
		t.Errorf("permanently_failed with last error %q and reason %q, want the provider's answer and permanent failure", got.LastError, h.lastReason(t, n.ID))
	}
	ch := h.channel(t)
	d := deadLetter(t, ch)
	if d.MessageId != n.ID.String() || d.Headers["failure_reason"] != got.LastError || d.Headers["attempts"] != "1" || queue.CorrelationID(d.Headers) != n.CorrelationID {
		t.Errorf("dead letter id %q headers %v, want the notification with its failure reason, attempts and correlation id", d.MessageId, d.Headers)
	}
	if _, ok := h.outboxDue(t, n.ID); ok {
		t.Error("a permanently failed notification has an outbox row")
	}
	if got := metricWith(t, h.reg, "notifications_failed_total", "final", "true"); got != 1 {
		t.Errorf("notifications_failed_total{final=true} = %v, want 1", got)
	}
	assertAcknowledged(t, ch, stop)
}

func TestWorkerFailsANotificationWhoseLastAttemptFails(t *testing.T) {
	h := newHarness(t, always(answer{status: http.StatusInternalServerError}))
	h.startWorker(t)
	n := h.create(t, func(nn *domain.NewNotification) { nn.MaxAttempts = 1 })
	h.dispatch(t)

	got := h.waitStatus(t, n.ID, domain.StatusPermanentlyFailed)
	if got.Attempts != 1 || h.lastReason(t, n.ID) != "attempts exhausted" {
		t.Errorf("permanently_failed after %d attempts with reason %q, want 1 and attempts exhausted", got.Attempts, h.lastReason(t, n.ID))
	}
	if d := deadLetter(t, h.channel(t)); d.MessageId != n.ID.String() {
		t.Errorf("dead letter %q, want %s", d.MessageId, n.ID)
	}
}

func TestWorkerRetriesUntilTheProviderAccepts(t *testing.T) {
	h := newHarness(t, func(n int) answer {
		if n == 0 {
			return answer{status: http.StatusBadGateway}
		}
		return accepted("m-2")
	})
	h.startWorker(t)
	n := h.create(t, nil)
	h.dispatch(t)
	h.waitStatus(t, n.ID, domain.StatusRetryScheduled)

	// The retry row is due after the backoff (1 s); then the relay queues the notification again.
	waitFor(t, "the retry is due", func() bool {
		due, ok := h.outboxDue(t, n.ID)
		return ok && due <= 0
	})
	h.dispatch(t)
	got := h.waitStatus(t, n.ID, domain.StatusSent)
	if got.Attempts != 2 || got.ProviderMessageID != "m-2" {
		t.Errorf("sent after %d attempts with provider id %q, want 2 and m-2", got.Attempts, got.ProviderMessageID)
	}
	if reqs := h.provider.got(); len(reqs) != 2 || reqs[0].key != reqs[1].key {
		t.Errorf("provider got %v, want two requests with the same Idempotency-Key", reqs)
	}
	want := []string{"pending", "queued", "sending", "retry_scheduled", "queued", "sending", "sent"}
	if got := h.statuses(t, n.ID); !slices.Equal(got, want) {
		t.Errorf("status events %v, want %v", got, want)
	}
}

func TestWorkerSkipsANotificationCancelledAfterItWasQueued(t *testing.T) {
	h := newHarness(t, always(accepted("m-3")))
	n := h.create(t, nil)
	h.dispatch(t)
	if _, err := h.store.TransitionNotification(t.Context(), repository.Transition{
		ID: n.ID, From: domain.AllowedFrom(domain.StatusCancelled), To: domain.StatusCancelled,
	}); err != nil {
		t.Fatalf("cancelling: %v", err)
	}

	stop := h.startWorker(t)
	ch := h.channel(t)
	// Skipping changes nothing in the database; the worker logs it once it has acknowledged the message.
	h.waitLogged(t, "skipped a notification that is not queued", n.ID.String())
	assertAcknowledged(t, ch, stop)
	if reqs := h.provider.got(); len(reqs) != 0 {
		t.Errorf("provider got %d requests for a cancelled notification, want none", len(reqs))
	}
	if got, _ := h.store.GetNotification(t.Context(), n.ID); got.Status != domain.StatusCancelled {
		t.Errorf("status %s, want cancelled", got.Status)
	}
	if got := metricWith(t, h.reg, "deliveries_skipped_total", "reason", "not_queued"); got != 1 {
		t.Errorf("deliveries_skipped_total{reason=not_queued} = %v, want 1", got)
	}
	if got := metricWith(t, h.reg, "provider_request_duration_seconds"); got != 0 {
		t.Errorf("provider_request_duration_seconds has %v samples for a skipped delivery, want 0", got)
	}
}

func TestWorkerSendsADuplicateMessageOnce(t *testing.T) {
	h := newHarness(t, always(accepted("m-4")))
	n := h.create(t, nil)
	h.dispatch(t)
	// A second copy, as after a relay that crashed between publishing and committing.
	if err := h.pub.Publish(t.Context(), []queue.Message{{
		Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, ID: n.ID.String(), Body: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}

	stop := h.startWorker(t)
	h.waitStatus(t, n.ID, domain.StatusSent)
	h.waitLogged(t, "skipped a notification that is not queued", n.ID.String())
	assertAcknowledged(t, h.channel(t), stop)
	if reqs := h.provider.got(); len(reqs) != 1 {
		t.Errorf("provider got %d requests, want one", len(reqs))
	}
}

func TestWorkerCountsAnAcceptanceWithoutAMessageIDAsSent(t *testing.T) {
	h := newHarness(t, always(answer{status: http.StatusOK, body: "ok"}))
	h.startWorker(t)
	n := h.create(t, nil)
	h.dispatch(t)

	if got := h.waitStatus(t, n.ID, domain.StatusSent); got.ProviderMessageID != "" {
		t.Errorf("provider message id %q, want none", got.ProviderMessageID)
	}
	if got := metricWith(t, h.reg, "provider_malformed_acceptances_total"); got != 1 {
		t.Errorf("provider_malformed_acceptances_total = %v, want 1", got)
	}
	if got := metricWith(t, h.reg, "notifications_sent_total"); got != 1 {
		t.Errorf("notifications_sent_total = %v, want 1", got)
	}
}

func TestWorkerDeadLettersAMessageThatNamesNoNotification(t *testing.T) {
	h := newHarness(t, always(accepted("m-5")))
	if err := h.pub.Publish(t.Context(), []queue.Message{{
		Channel: domain.ChannelSMS, Priority: domain.PriorityLow, ID: "not-a-uuid", Body: []byte(`{}`),
	}}); err != nil {
		t.Fatal(err)
	}
	h.startWorker(t)

	if d := deadLetter(t, h.channel(t)); d.MessageId != "not-a-uuid" {
		t.Errorf("dead letter %q, want not-a-uuid", d.MessageId)
	}
	if got := metricWith(t, h.reg, "deliveries_skipped_total", "reason", "invalid_message"); got != 1 {
		t.Errorf("deliveries_skipped_total{reason=invalid_message} = %v, want 1", got)
	}
	if reqs := h.provider.got(); len(reqs) != 0 {
		t.Errorf("provider got %d requests, want none", len(reqs))
	}
}

func TestWorkerSpacesTheSendsOfAChannelByTheRateLimit(t *testing.T) {
	var (
		mu    sync.Mutex
		times []time.Time
	)
	h := newHarness(t, func(int) answer {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		return accepted("m-7")
	})
	h.rate = 5 // one send every 200 ms
	var ids []domain.Notification
	for range 4 {
		ids = append(ids, h.create(t, nil))
	}
	h.dispatch(t)
	h.startWorker(t)
	for _, n := range ids {
		h.waitStatus(t, n.ID, domain.StatusSent)
	}

	mu.Lock()
	defer mu.Unlock()
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
	// Slots are exactly 200 ms apart; a request reaches the provider a little after its slot, by a
	// varying amount, so consecutive arrivals may come somewhat closer.
	for i := 1; i < len(times); i++ {
		if gap := times[i].Sub(times[i-1]); gap < 150*time.Millisecond {
			t.Errorf("sends %d and %d reached the provider %v apart, want about 200ms", i, i+1, gap)
		}
	}
}

func TestWorkerFinishesTheSendsInFlightWhenItStops(t *testing.T) {
	h := newHarness(t, func(int) answer {
		a := accepted("m-6")
		a.delay = 500 * time.Millisecond
		return a
	})
	stop := h.startWorker(t)
	n := h.create(t, nil)
	h.dispatch(t)
	waitFor(t, "the provider got the request", func() bool { return len(h.provider.got()) == 1 })

	stop() // returns once the send in flight has finished
	if got, _ := h.store.GetNotification(t.Context(), n.ID); got.Status != domain.StatusSent {
		t.Errorf("after the worker stopped the notification is %s, want sent", got.Status)
	}
	if !strings.Contains(strings.Join(h.statuses(t, n.ID), ","), "sending,sent") {
		t.Errorf("status events %v, want sending then sent", h.statuses(t, n.ID))
	}
}

func TestWorkerMeasuresLatencyFromWhenANotificationWasDue(t *testing.T) {
	h := newHarness(t, always(accepted("m-10")))
	// Scheduled 1.5 s ahead by the database clock, the clock the relay and the latency use.
	var dbNow time.Time
	if err := h.db.QueryRow(t.Context(), `SELECT now()`).Scan(&dbNow); err != nil {
		t.Fatal(err)
	}
	due := dbNow.Add(1500 * time.Millisecond)
	n := h.create(t, func(nn *domain.NewNotification) { nn.ScheduledAt = &due })
	h.startWorker(t)
	waitFor(t, "the scheduled notification is dispatched", func() bool {
		claimed, _, err := h.relay.Once(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return claimed == 1
	})
	got := h.waitStatus(t, n.ID, domain.StatusSent)

	sinceCreated := got.SentAt.Sub(got.CreatedAt)
	latency := histogramSum(t, h.reg, "notification_e2e_latency_seconds", "priority", "high")
	if sinceCreated < 1500*time.Millisecond {
		t.Fatalf("sent %v after creation, before it was due", sinceCreated)
	}
	if want := got.SentAt.Sub(due).Seconds(); latency > want+0.001 || latency < want-0.001 {
		t.Errorf("notification_e2e_latency_seconds recorded %.3fs, want %.3fs from the scheduled time (not %.3fs from creation)",
			latency, want, sinceCreated.Seconds())
	}
}
