//go:build integration

package worker_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// relayTraceParent stands for the relay's publish span; the harness's relay records no spans, so it
// passes the outbox row's traceparent on unchanged.
const relayTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func withTrace(n *domain.NewNotification) { n.TraceParent = relayTraceParent }

// processSpans waits until the worker has ended want process spans and returns them in start order.
func (h *harness) processSpans(t *testing.T, want int) []sdktrace.ReadOnlySpan {
	t.Helper()
	var out []sdktrace.ReadOnlySpan
	waitFor(t, fmt.Sprintf("%d process spans end", want), func() bool {
		out = out[:0]
		for _, s := range h.spans.Ended() {
			if strings.HasPrefix(s.Name(), "process ") {
				out = append(out, s)
			}
		}
		return len(out) >= want
	})
	slices.SortFunc(out, func(a, b sdktrace.ReadOnlySpan) int { return a.StartTime().Compare(b.StartTime()) })
	return out
}

func outcome(s sdktrace.ReadOnlySpan) string {
	for _, kv := range s.Attributes() {
		if kv.Key == tracing.OutcomeKey {
			return kv.Value.AsString()
		}
	}
	return ""
}

func spanContextOf(traceparent string) trace.SpanContext {
	return trace.SpanContextFromContext(tracing.WithTraceParent(context.Background(), traceparent))
}

func traceParentOf(sc trace.SpanContext) string {
	return fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
}

func TestWorkerContinuesTheTraceOfTheMessage(t *testing.T) {
	h := newHarness(t, always(accepted("m-1")))
	h.startWorker(t)
	n := h.create(t, withTrace)
	h.dispatch(t)
	h.waitStatus(t, n.ID, domain.StatusSent)

	s := h.processSpans(t, 1)[0]
	relay := spanContextOf(relayTraceParent)
	if s.Name() != "process sms.high" || s.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("span %q of kind %v, want %q, a consumer", s.Name(), s.SpanKind(), "process sms.high")
	}
	if s.SpanContext().TraceID() != relay.TraceID() || s.Parent().SpanID() != relay.SpanID() {
		t.Errorf("process span is in trace %s under %s, want under the relay's span %s of trace %s",
			s.SpanContext().TraceID(), s.Parent().SpanID(), relay.SpanID(), relay.TraceID())
	}
	if outcome(s) != "sent" || s.Status().Code != codes.Unset {
		t.Errorf("process span has outcome %q and status %v, want sent and unset", outcome(s), s.Status().Code)
	}
	var events []string
	for _, e := range s.Events() {
		events = append(events, e.Name)
	}
	if want := []string{"circuit breaker allowed", "sender acquired", "rate limit token granted"}; !slices.Equal(events, want) {
		t.Errorf("events %v, want %v", events, want)
	}

	var client sdktrace.ReadOnlySpan
	for _, c := range h.spans.Ended() {
		if c.SpanKind() == trace.SpanKindClient {
			client = c
		}
	}
	if client == nil || client.Parent().SpanID() != s.SpanContext().SpanID() {
		t.Fatalf("provider call span %v, want a client span under the process span", client)
	}
	if got, want := h.provider.got()[0].traceparent, traceParentOf(client.SpanContext()); got != want {
		t.Errorf("the provider got traceparent %q, want the client span's %q", got, want)
	}
}

func TestWorkerRetryContinuesTheTraceOfTheFailedAttempt(t *testing.T) {
	h := newHarness(t, func(n int) answer {
		if n == 0 {
			return answer{status: http.StatusBadGateway}
		}
		return accepted("m-2")
	})
	h.startWorker(t)
	n := h.create(t, withTrace)
	h.dispatch(t)
	h.waitStatus(t, n.ID, domain.StatusRetryScheduled)
	waitFor(t, "the retry is due", func() bool {
		due, ok := h.outboxDue(t, n.ID)
		return ok && due <= 0
	})
	h.dispatch(t)
	h.waitStatus(t, n.ID, domain.StatusSent)

	spans := h.processSpans(t, 2)
	first, second := spans[0], spans[1]
	if outcome(first) != "retry_scheduled" || first.Status().Code != codes.Error {
		t.Errorf("first attempt has outcome %q and status %v, want retry_scheduled and an error", outcome(first), first.Status().Code)
	}
	if second.Parent().SpanID() != first.SpanContext().SpanID() || second.SpanContext().TraceID() != first.SpanContext().TraceID() {
		t.Errorf("second attempt is under %s in trace %s, want under the first attempt's span %s in trace %s",
			second.Parent().SpanID(), second.SpanContext().TraceID(), first.SpanContext().SpanID(), first.SpanContext().TraceID())
	}
	if outcome(second) != "sent" {
		t.Errorf("second attempt has outcome %q, want sent", outcome(second))
	}
}

func TestWorkerDeadLettersAFailedDeliveryWithItsTrace(t *testing.T) {
	h := newHarness(t, always(answer{status: http.StatusBadRequest, body: `{"error":"recipient rejected"}`}))
	h.startWorker(t)
	n := h.create(t, withTrace)
	h.dispatch(t)
	h.waitStatus(t, n.ID, domain.StatusPermanentlyFailed)

	d := deadLetter(t, h.channel(t))
	s := h.processSpans(t, 1)[0]
	if got, want := queue.TraceParent(d.Headers), traceParentOf(s.SpanContext()); got != want {
		t.Errorf("dead letter carries traceparent %q, want the delivery's %q", got, want)
	}
	if got := queue.CorrelationID(d.Headers); got != "corr-"+t.Name() {
		t.Errorf("dead letter carries correlation ID %q, want %q", got, "corr-"+t.Name())
	}
	if outcome(s) != "permanently_failed" || s.Status().Code != codes.Error {
		t.Errorf("process span has outcome %q and status %v, want permanently_failed and an error", outcome(s), s.Status().Code)
	}
}
