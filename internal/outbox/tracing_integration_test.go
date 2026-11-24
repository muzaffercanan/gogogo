//go:build integration

package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// apiTraceParent is the span of the API request that created a notification.
const apiTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func createTraced(ctx context.Context, t *testing.T, store *repository.Store, c domain.Channel, p domain.Priority, traceparent string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if _, _, err := store.CreateNotification(ctx, domain.NewNotification{
		ID: id, Channel: c, Priority: p, Recipient: "+905551234567", Content: "hello", MaxAttempts: 5, TraceParent: traceparent,
	}); err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	return id
}

// publishSpans returns the relay's ended publish spans by notification ID.
func publishSpans(spans *tracetest.SpanRecorder) map[string]sdktrace.ReadOnlySpan {
	out := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans.Ended() {
		for _, kv := range s.Attributes() {
			if kv.Key == tracing.NotificationIDKey {
				out[kv.Value.AsString()] = s
			}
		}
	}
	return out
}

func traceParentOf(sc trace.SpanContext) string {
	return fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
}

func TestRelayPublishesEachNotificationInItsTrace(t *testing.T) {
	ctx := context.Background()
	store := repository.New(pg.NewDatabase(t))
	pub, vhost := connectedPublisher(t)
	spans := tracetest.NewSpanRecorder()
	var logs bytes.Buffer
	relay := outbox.NewRelay(store, pub, discardMetrics(), sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)), logging.New(io.MultiWriter(t.Output(), &logs), "info"))

	traced := createTraced(ctx, t, store, domain.ChannelSMS, domain.PriorityHigh, apiTraceParent)
	untraced := createTraced(ctx, t, store, domain.ChannelEmail, domain.PriorityLow, "")
	if _, _, err := relay.Once(ctx); err != nil {
		t.Fatalf("Once: %v", err)
	}

	api := trace.SpanContextFromContext(tracing.WithTraceParent(ctx, apiTraceParent))
	got := publishSpans(spans)
	ch := openChannel(t, dial(t, vhost))
	for _, tt := range []struct {
		id      uuid.UUID
		c       domain.Channel
		p       domain.Priority
		inTrace bool
	}{
		{traced, domain.ChannelSMS, domain.PriorityHigh, true},
		{untraced, domain.ChannelEmail, domain.PriorityLow, false},
	} {
		key := queue.QueueName(tt.c, tt.p)
		s, ok := got[tt.id.String()]
		if !ok {
			t.Fatalf("no publish span for %s", tt.id)
		}
		if s.Name() != "publish "+key || s.SpanKind() != trace.SpanKindProducer || s.Status().Code != codes.Unset {
			t.Errorf("span of %s: %q, %v, status %v; want %q, a producer, unset", tt.id, s.Name(), s.SpanKind(), s.Status().Code, "publish "+key)
		}
		if tt.inTrace && (s.SpanContext().TraceID() != api.TraceID() || s.Parent().SpanID() != api.SpanID()) {
			t.Errorf("span of %s is in trace %s under %s, want under the API's span %s of trace %s",
				tt.id, s.SpanContext().TraceID(), s.Parent().SpanID(), api.SpanID(), api.TraceID())
		}
		if !tt.inTrace && s.Parent().IsValid() {
			t.Errorf("span of %s, whose outbox row has no trace, has parent %s; want a new trace", tt.id, s.Parent().SpanID())
		}
		d := receive(t, ch, key)
		if h, want := queue.TraceParent(d.Headers), traceParentOf(s.SpanContext()); h != want {
			t.Errorf("message of %s carries traceparent %q, want its publish span's %q", tt.id, h, want)
		}
		if rec := queuedRecord(t, logs.String(), tt.id); rec.TraceID != s.SpanContext().TraceID().String() || rec.SpanID != s.SpanContext().SpanID().String() {
			t.Errorf("notification queued record of %s names trace %q and span %q, want its publish span %s of trace %s",
				tt.id, rec.TraceID, rec.SpanID, s.SpanContext().SpanID(), s.SpanContext().TraceID())
		}
	}
}

type logRecord struct {
	Msg            string `json:"msg"`
	NotificationID string `json:"notification_id"`
	TraceID        string `json:"trace_id"`
	SpanID         string `json:"span_id"`
}

// queuedRecord returns the "notification queued" log record of notification id.
func queuedRecord(t *testing.T, logs string, id uuid.UUID) logRecord {
	t.Helper()
	for line := range strings.Lines(logs) {
		var rec logRecord // fresh per line: Unmarshal keeps fields a line does not have
		if err := json.Unmarshal([]byte(line), &rec); err == nil && rec.Msg == "notification queued" && rec.NotificationID == id.String() {
			return rec
		}
	}
	t.Fatalf("no notification queued record for %s in %s", id, logs)
	return logRecord{}
}

func TestRelayFailsThePublishSpansOfAStepThatRolledBack(t *testing.T) {
	ctx := context.Background()
	store := repository.New(pg.NewDatabase(t))
	pub, vhost := connectedPublisher(t)
	spans := tracetest.NewSpanRecorder()
	relay := outbox.NewRelay(store, pub, discardMetrics(), sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)), slog.New(slog.NewTextHandler(t.Output(), nil)))

	id := createTraced(ctx, t, store, domain.ChannelEmail, domain.PriorityNormal, apiTraceParent)
	key := queue.QueueName(domain.ChannelEmail, domain.PriorityNormal)
	if _, err := openChannel(t, dial(t, vhost)).QueueDelete(key, false, false, false); err != nil {
		t.Fatalf("deleting %s: %v", key, err)
	}
	if _, _, err := relay.Once(ctx); err == nil {
		t.Fatalf("Once with %s deleted succeeded, want an error", key)
	}

	s, ok := publishSpans(spans)[id.String()]
	if !ok {
		t.Fatalf("no publish span for %s", id)
	}
	if s.Status().Code != codes.Error || len(s.Events()) == 0 {
		t.Errorf("span of the rolled back step has status %v and %d events, want an error and the recorded error", s.Status().Code, len(s.Events()))
	}
}
