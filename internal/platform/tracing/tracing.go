// Package tracing sets up OpenTelemetry tracing for a process.
//
// It is configured with the standard OpenTelemetry environment variables, which the SDK reads itself:
// OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) says where spans go over OTLP/HTTP,
// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES describe the process, and OTEL_TRACES_SAMPLER with
// OTEL_TRACES_SAMPLER_ARG choose the sampler (by default every trace a parent has not decided against).
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// ShutdownTimeout bounds how long a process waits on exit for its last spans to be exported.
const ShutdownTimeout = 5 * time.Second

// Provider is the tracer provider of a process.
type Provider struct {
	trace.TracerProvider
	shutdown func(context.Context) error
}

// Setup returns the tracer provider of the process named service.
//
// It records and exports spans only when OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or
// OTEL_EXPORTER_OTLP_ENDPOINT is set. Otherwise its tracers record nothing, but a span started from a
// context that carries a trace context still carries that trace context, so a process without tracing
// passes a trace on unbroken.
//
// service becomes the service.name resource attribute unless OTEL_SERVICE_NAME sets another one.
// Spans that cannot be exported are dropped, and the errors are logged to log as warnings; that
// handler is process-wide, as OpenTelemetry has a single one.
func Setup(ctx context.Context, service string, log *slog.Logger) (*Provider, error) {
	if !exporting() {
		return &Provider{
			TracerProvider: noop.NewTracerProvider(),
			shutdown:       func(context.Context) error { return nil },
		}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(service)),
		// After the attribute above, so OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override it.
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("describing the process for tracing: %w", err)
	}
	// The exporter only connects when it first sends, so a collector that is down does not stop the
	// process from starting.
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating the trace exporter: %w", err)
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("tracing", "error", err)
	}))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	return &Provider{TracerProvider: tp, shutdown: tp.Shutdown}, nil
}

// Shutdown exports the spans not exported yet and stops the provider. It gives up when ctx ends.
func (p *Provider) Shutdown(ctx context.Context) error {
	if err := p.shutdown(ctx); err != nil {
		return fmt.Errorf("shutting down tracing: %w", err)
	}
	return nil
}

// exporting reports whether an OTLP endpoint for traces is configured.
func exporting() bool {
	for _, key := range []string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"} {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}
