//go:build integration

package api_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
)

// clientTraceParent is the trace context a calling service sends.
const clientTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

const plainSMS = `{"channel":"sms","recipient":"+905551234567","content":"Kodunuz: 1234"}`

// outboxTraceParent returns the traceparent in the outbox row of notification id.
func (a *testAPI) outboxTraceParent(t *testing.T, id string) string {
	t.Helper()
	var tp string
	if err := a.db.QueryRow(t.Context(), "SELECT coalesce(payload->>'traceparent', '') FROM outbox WHERE notification_id = $1", id).Scan(&tp); err != nil {
		t.Fatalf("reading the outbox row of %s: %v", id, err)
	}
	return tp
}

// endedSpans returns the spans the API ended with the given name.
func (a *testAPI) endedSpans(name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range a.spans.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

func spanContextOf(t *testing.T, traceparent string) trace.SpanContext {
	t.Helper()
	sc := trace.SpanContextFromContext(tracing.WithTraceParent(context.Background(), traceparent))
	if !sc.IsValid() {
		t.Fatalf("%q is not a traceparent", traceparent)
	}
	return sc
}

func traceParentOf(sc trace.SpanContext) string {
	return fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
}

func attr(s sdktrace.ReadOnlySpan, key attribute.Key) string {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value.String()
		}
	}
	return ""
}

func TestCreateContinuesTheTraceOfTheRequest(t *testing.T) {
	a := newAPI(t)
	client := spanContextOf(t, clientTraceParent)

	resp := a.do(t, http.MethodPost, "/v1/notifications", plainSMS, "traceparent", clientTraceParent)
	if resp.status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", resp.status, resp.raw)
	}
	id := fmt.Sprint(resp.body["id"])

	spans := a.endedSpans("POST /v1/notifications")
	if len(spans) != 1 {
		t.Fatalf("%d spans named after the route, want 1 (spans: %v)", len(spans), a.spans.Ended())
	}
	server := spans[0]
	if server.SpanKind() != trace.SpanKindServer || server.SpanContext().TraceID() != client.TraceID() ||
		server.Parent().SpanID() != client.SpanID() || !server.Parent().IsRemote() {
		t.Errorf("server span is a %v span of trace %s under %s, want a server span under the client's %s of trace %s",
			server.SpanKind(), server.SpanContext().TraceID(), server.Parent().SpanID(), client.SpanID(), client.TraceID())
	}
	if got := attr(server, tracing.NotificationIDKey); got != id {
		t.Errorf("notification.id = %q, want %q", got, id)
	}
	if got, want := a.outboxTraceParent(t, id), traceParentOf(server.SpanContext()); got != want {
		t.Errorf("outbox traceparent = %q, want the server span's %q", got, want)
	}

	// The span is named after the route, not the path.
	a.do(t, http.MethodGet, "/v1/notifications/"+id, "")
	if n := len(a.endedSpans("GET /v1/notifications/{id}")); n != 1 {
		t.Errorf("%d spans named GET /v1/notifications/{id}, want 1", n)
	}
}

func TestCreateStartsATraceWhenTheRequestHasNone(t *testing.T) {
	a := newAPI(t)

	resp := a.do(t, http.MethodPost, "/v1/notifications", plainSMS)
	if resp.status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", resp.status, resp.raw)
	}
	spans := a.endedSpans("POST /v1/notifications")
	if len(spans) != 1 {
		t.Fatalf("%d spans named after the route, want 1", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Errorf("server span has parent %s, want a root span", spans[0].Parent().SpanID())
	}
	if got, want := a.outboxTraceParent(t, fmt.Sprint(resp.body["id"])), traceParentOf(spans[0].SpanContext()); got != want {
		t.Errorf("outbox traceparent = %q, want the server span's %q", got, want)
	}
}

func TestBatchGivesEachNotificationATraceOfItsOwn(t *testing.T) {
	a := newAPI(t)
	client := spanContextOf(t, clientTraceParent)
	body := batchBody(smsItem("+905551234567"), `{"channel":"email","recipient":"not-an-email","content":"x"}`, smsItem("+905551234568"))

	resp := a.do(t, http.MethodPost, "/v1/notifications/batch", body, "traceparent", clientTraceParent)
	if resp.status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", resp.status, resp.raw)
	}
	batchID := fmt.Sprint(resp.body["batch_id"])
	servers := a.endedSpans("POST /v1/notifications/batch")
	if len(servers) != 1 {
		t.Fatalf("%d spans named after the batch route, want 1", len(servers))
	}
	server := servers[0]
	if server.SpanContext().TraceID() != client.TraceID() || attr(server, tracing.BatchIDKey) != batchID {
		t.Errorf("batch span is in trace %s with batch ID %q, want the client's trace %s and %q",
			server.SpanContext().TraceID(), attr(server, tracing.BatchIDKey), client.TraceID(), batchID)
	}

	created := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range a.endedSpans("create notification") {
		created[attr(s, tracing.NotificationIDKey)] = s
	}
	traces := map[trace.TraceID]bool{}
	items, _ := resp.body["items"].([]any)
	accepted := 0
	for _, raw := range items {
		item := raw.(map[string]any)
		id, ok := item["notification_id"].(string)
		if !ok {
			continue // the rejected item
		}
		accepted++
		s, ok := created[id]
		if !ok {
			t.Errorf("no create notification span for %s", id)
			continue
		}
		if s.Parent().IsValid() || s.SpanContext().TraceID() == client.TraceID() || traces[s.SpanContext().TraceID()] {
			t.Errorf("span of %s has parent %s in trace %s, want the root of a trace of its own", id, s.Parent().SpanID(), s.SpanContext().TraceID())
		}
		traces[s.SpanContext().TraceID()] = true
		if links := s.Links(); len(links) != 1 || !links[0].SpanContext.Equal(server.SpanContext()) {
			t.Errorf("span of %s links to %v, want the batch request's span", id, links)
		}
		if attr(s, tracing.BatchIDKey) != batchID {
			t.Errorf("span of %s has batch ID %q, want %q", id, attr(s, tracing.BatchIDKey), batchID)
		}
		if got, want := a.outboxTraceParent(t, id), traceParentOf(s.SpanContext()); got != want {
			t.Errorf("outbox traceparent of %s = %q, want its own span's %q", id, got, want)
		}
	}
	if accepted != 2 || len(created) != 2 {
		t.Errorf("%d accepted notifications and %d create notification spans, want 2 and 2", accepted, len(created))
	}
}

func TestUntracedAPIPassesTheTraceOn(t *testing.T) {
	a := newUntracedAPI(t)

	resp := a.do(t, http.MethodPost, "/v1/notifications", plainSMS, "traceparent", clientTraceParent)
	if resp.status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", resp.status, resp.raw)
	}
	if got := a.outboxTraceParent(t, fmt.Sprint(resp.body["id"])); got != clientTraceParent {
		t.Errorf("outbox traceparent = %q, want the client's %q", got, clientTraceParent)
	}

	resp = a.do(t, http.MethodPost, "/v1/notifications", plainSMS)
	if got := a.outboxTraceParent(t, fmt.Sprint(resp.body["id"])); got != "" {
		t.Errorf("outbox traceparent = %q without a trace, want none", got)
	}
}
