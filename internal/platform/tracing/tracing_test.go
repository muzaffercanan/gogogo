package tracing_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
)

// otelEnv lists the variables Setup or the SDK read; every test clears them first so the machine's
// environment cannot leak in.
var otelEnv = []string{
	"OTEL_EXPORTER_OTLP_ENDPOINT",
	"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_SERVICE_NAME",
	"OTEL_RESOURCE_ATTRIBUTES",
	"OTEL_TRACES_SAMPLER",
	"OTEL_TRACES_SAMPLER_ARG",
}

// collector is a fake OTLP/HTTP receiver that remembers what it was sent.
type collector struct {
	srv    *httptest.Server
	status int

	mu       sync.Mutex
	paths    []string
	requests []*coltracepb.ExportTraceServiceRequest
}

func newCollector(t *testing.T, status int) *collector {
	t.Helper()
	c := &collector{status: status}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading the export request: %v", err)
		}
		req := &coltracepb.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			t.Errorf("decoding the export request: %v", err)
		}
		c.mu.Lock()
		c.paths = append(c.paths, r.URL.Path)
		c.requests = append(c.requests, req)
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(c.status)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// exported is a span as the collector received it.
type exported struct {
	path, service, name string
	traceID, parentID   []byte
}

func (c *collector) spans() []exported {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []exported
	for i, req := range c.requests {
		for _, rs := range req.GetResourceSpans() {
			service := ""
			for _, kv := range rs.GetResource().GetAttributes() {
				if kv.GetKey() == "service.name" {
					service = kv.GetValue().GetStringValue()
				}
			}
			for _, ss := range rs.GetScopeSpans() {
				for _, s := range ss.GetSpans() {
					out = append(out, exported{path: c.paths[i], service: service, name: s.GetName(), traceID: s.GetTraceId(), parentID: s.GetParentSpanId()})
				}
			}
		}
	}
	return out
}

// remoteParent returns a context carrying a sampled trace context received from another process.
func remoteParent(t *testing.T) (context.Context, trace.SpanContext) {
	t.Helper()
	traceID, err := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("b7ad6b7169203331")
	if err != nil {
		t.Fatal(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true})
	return trace.ContextWithRemoteSpanContext(context.Background(), sc), sc
}

func TestSetup(t *testing.T) {
	tests := []struct {
		name string
		// env values; {collector} stands for the fake collector's URL.
		env         map[string]string
		wantPath    string // "" when nothing may be recorded or exported
		wantService string
	}{
		{
			name:        "exports to the OTLP endpoint",
			env:         map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "{collector}"},
			wantPath:    "/v1/traces",
			wantService: "test-service",
		},
		{
			name:        "exports to the traces endpoint as given",
			env:         map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "{collector}/custom"},
			wantPath:    "/custom",
			wantService: "test-service",
		},
		{
			name:        "OTEL_SERVICE_NAME overrides the service name",
			env:         map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "{collector}", "OTEL_SERVICE_NAME": "renamed"},
			wantPath:    "/v1/traces",
			wantService: "renamed",
		},
		{
			name: "records nothing without an endpoint",
			env:  map[string]string{},
		},
		{
			name: "a blank endpoint is no endpoint",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "  ", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": " "},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newCollector(t, http.StatusOK)
			for _, key := range otelEnv {
				t.Setenv(key, "")
			}
			for key, value := range tt.env {
				t.Setenv(key, strings.ReplaceAll(value, "{collector}", col.srv.URL))
			}

			tp, err := tracing.Setup(context.Background(), "test-service", slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}
			ctx, parent := remoteParent(t)
			_, span := tp.Tracer("test").Start(ctx, "work")
			if got := span.SpanContext().TraceID(); got != parent.TraceID() {
				t.Errorf("span trace ID = %s, want the parent's %s", got, parent.TraceID())
			}
			if span.IsRecording() != (tt.wantPath != "") {
				t.Errorf("span recording = %v, want %v", span.IsRecording(), tt.wantPath != "")
			}
			span.End()
			if err := tp.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown: %v", err)
			}

			got := col.spans()
			if tt.wantPath == "" {
				if len(got) != 0 {
					t.Fatalf("exported %d spans, want none", len(got))
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("exported %d spans, want 1: %+v", len(got), got)
			}
			s := got[0]
			if s.path != tt.wantPath || s.service != tt.wantService || s.name != "work" {
				t.Errorf("exported span %q of service %q to %s, want %q of %q to %s", s.name, s.service, s.path, "work", tt.wantService, tt.wantPath)
			}
			if traceID := parent.TraceID(); !bytes.Equal(s.traceID, traceID[:]) {
				t.Errorf("exported trace ID %x, want the parent's %s", s.traceID, parent.TraceID())
			}
			if spanID := parent.SpanID(); !bytes.Equal(s.parentID, spanID[:]) {
				t.Errorf("exported parent span ID %x, want %s", s.parentID, parent.SpanID())
			}
		})
	}
}

func TestTraceParentCrossesAsAString(t *testing.T) {
	ctx, parent := remoteParent(t)
	tp := tracing.TraceParent(ctx)
	if want := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"; tp != want {
		t.Fatalf("TraceParent = %q, want %q", tp, want)
	}

	got := trace.SpanContextFromContext(tracing.WithTraceParent(context.Background(), tp))
	if !got.Equal(parent) {
		t.Errorf("WithTraceParent(%q) carries %+v, want %+v", tp, got, parent)
	}
	if !got.IsRemote() {
		t.Error("WithTraceParent carries a span context that is not remote; spans started from it would not count it as a parent from another process")
	}
}

func TestTraceParentOfAContextWithoutASpanIsEmpty(t *testing.T) {
	if tp := tracing.TraceParent(context.Background()); tp != "" {
		t.Errorf("TraceParent = %q, want \"\"", tp)
	}
}

func TestWithTraceParentIgnoresWhatIsNotATraceParent(t *testing.T) {
	for _, tp := range []string{"", "not a traceparent", "00-00000000000000000000000000000000-b7ad6b7169203331-01"} {
		ctx := tracing.WithTraceParent(context.Background(), tp)
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			t.Errorf("WithTraceParent(%q) carries %+v, want no span context", tp, sc)
		}
	}
}

func TestSetupLogsSpansItCannotExport(t *testing.T) {
	col := newCollector(t, http.StatusBadRequest)
	for _, key := range otelEnv {
		t.Setenv(key, "")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", col.srv.URL)
	var logs syncBuffer
	tp, err := tracing.Setup(context.Background(), "test-service", slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	_, span := tp.Tracer("test").Start(context.Background(), "work")
	span.End()
	_ = tp.Shutdown(context.Background())

	if got := logs.String(); !strings.Contains(got, `"level":"WARN","msg":"tracing"`) || !strings.Contains(got, "400") {
		t.Errorf("logs = %q, want a warning about the rejected export", got)
	}
}

// syncBuffer is a bytes.Buffer safe for the exporter's goroutine and the test to share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
