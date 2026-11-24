package provider_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/provider"
)

func TestSendTracesTheRequestAndSendsItsTraceParent(t *testing.T) {
	sent := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent <- r.Header.Get("traceparent")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"m-1","status":"accepted"}`)
	}))
	defer srv.Close()
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	c, err := provider.NewClient(srv.URL, time.Second, tp)
	if err != nil {
		t.Fatal(err)
	}

	ctx, delivery := tp.Tracer("test").Start(context.Background(), "process sms.high")
	if _, err := c.Send(ctx, provider.Message{NotificationID: "n-1", Channel: domain.ChannelSMS, To: "+905551234567", Content: "hi"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	delivery.End()

	var client sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			client = s
		}
	}
	if client == nil {
		t.Fatalf("no client span among %v", spans.Ended())
	}
	if client.Parent().SpanID() != delivery.SpanContext().SpanID() {
		t.Errorf("client span's parent is %s, want the delivery's span %s", client.Parent().SpanID(), delivery.SpanContext().SpanID())
	}
	sc := client.SpanContext()
	if got, want := <-sent, fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags()); got != want {
		t.Errorf("the provider got traceparent %q, want the client span's %q", got, want)
	}
}
