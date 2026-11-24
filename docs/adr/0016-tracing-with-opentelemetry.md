# 0016. Tracing with OpenTelemetry

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

A notification passes through three processes and two asynchronous hops before the provider call:
the API writes it with its outbox row, the relay publishes it to RabbitMQ, a worker consumes and sends
it. Logs share a correlation ID, but they do not show where the time went or which hop a slow or
failed delivery spent it in.

## Decision

- The api, relay and worker trace with the OpenTelemetry Go SDK and export spans over OTLP/HTTP
  (protobuf).
- They are configured by the standard OpenTelemetry environment variables, which the SDK reads
  itself: `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`), `OTEL_SERVICE_NAME`,
  `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`. A process records
  spans only when an endpoint is set; without one its tracers record nothing but still pass on the trace
  context they are given.
- Sampling is the SDK default, parent-based always-on: the API decides for a whole trace and the
  relay and worker follow its decision, so every notification's trace is kept. A ratio is one
  environment variable away.
- The tracer provider is passed explicitly to what makes spans; there is no global tracer provider.
  The one global is OpenTelemetry's error handler, which logs export failures as warnings.
- Tracing never holds up work: spans are exported in batches in the background, the exporter connects
  only when it first sends, and spans that cannot be exported are dropped. On exit a process waits at
  most 5 s for its last spans.
- Docker Compose runs Jaeger v2 (`jaegertracing/jaeger`, in-memory storage) in the default stack: UI on
  16686, OTLP/HTTP on 4318. Its storage keeps the latest 20 000 traces, about 240 MB (a trace of
  seven spans took about 12 KB).

## Alternatives considered

- **OTLP/gRPC.** The Go OTLP/HTTP exporter links gRPC as well (the binaries differ by 85 KB), so gRPC
  saves nothing, and plain HTTP is easier to inspect and passes any proxy.
- **Configuration variables of our own** (`TRACING_ENDPOINT`, `TRACING_SAMPLE_RATIO`), validated at
  startup like the others: one configuration style, but it renames standard variables that the
  OpenTelemetry documentation and tools already know.
- **A 10 % ratio by default.** A tenth of the load, but the trace of a given notification would
  usually be missing.
- **Jaeger behind a Compose profile.** A leaner default stack, but with the profile off the processes
  would log export failures unless the endpoint were cleared as well.

## Consequences

- At full load (three channels at 100 msg/s) the processes export about 2 000 spans/s, and Jaeger
  keeps roughly the last minute of traces; at demo rates it keeps hours.
- The `OTEL_*` variables are not validated at startup: a misspelt sampler is logged as a warning and
  the default sampler is used.
- If Jaeger is unreachable for long, each process's span queue (2 048 spans) fills and later spans are
  dropped; requests and deliveries are unaffected.
