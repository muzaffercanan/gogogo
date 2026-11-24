package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestCorrelationIDIsAddedFromContext(t *testing.T) {
	tests := []struct {
		name   string
		ctx    context.Context
		wantID string
	}{
		{name: "with correlation id", ctx: WithCorrelationID(context.Background(), "abc-123"), wantID: "abc-123"},
		{name: "without correlation id", ctx: context.Background(), wantID: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			New(&buf, "info").With("service", "test").InfoContext(tt.ctx, "hello")

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("log line is not JSON: %v (%q)", err, buf.String())
			}
			got, _ := rec["correlation_id"].(string)
			if got != tt.wantID {
				t.Errorf("correlation_id = %q, want %q", got, tt.wantID)
			}
			if rec["service"] != "test" {
				t.Errorf("service attribute lost: %v", rec["service"])
			}
		})
	}
}

func TestTraceIDsAreAddedFromContext(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	spanID, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	tests := []struct {
		name                string
		ctx                 context.Context
		wantTrace, wantSpan string
	}{
		{
			name:      "with a span",
			ctx:       trace.ContextWithSpanContext(context.Background(), span),
			wantTrace: "0af7651916cd43dd8448eb211c80319c",
			wantSpan:  "b7ad6b7169203331",
		},
		{
			name:      "with a span that is not sampled",
			ctx:       trace.ContextWithSpanContext(context.Background(), span.WithTraceFlags(0)),
			wantTrace: "0af7651916cd43dd8448eb211c80319c",
			wantSpan:  "b7ad6b7169203331",
		},
		{name: "without a span", ctx: context.Background()},
		{name: "with an invalid span context", ctx: trace.ContextWithSpanContext(context.Background(), trace.SpanContext{})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			New(&buf, "info").With("service", "test").InfoContext(tt.ctx, "hello")

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("log line is not JSON: %v (%q)", err, buf.String())
			}
			gotTrace, hasTrace := rec["trace_id"].(string)
			gotSpan, hasSpan := rec["span_id"].(string)
			if gotTrace != tt.wantTrace || gotSpan != tt.wantSpan || hasTrace != (tt.wantTrace != "") || hasSpan != (tt.wantSpan != "") {
				t.Errorf("trace_id = %q, span_id = %q, want %q and %q", gotTrace, gotSpan, tt.wantTrace, tt.wantSpan)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"verbose", slog.LevelInfo},
	}
	for _, tt := range tests {
		if got := ParseLevel(tt.in); got != tt.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestDebugIsSuppressedAtInfoLevel(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, "info").Debug("hidden")
	if buf.Len() != 0 {
		t.Errorf("debug record written at info level: %q", buf.String())
	}
}
