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

## Propagation

Each notification gets one trace, from the request that created it to the provider's answer, across
every attempt.

- **Spans at the boundaries.** The API server and the provider client get their spans from `otelhttp`
  (server spans named by route). The relay starts a producer span, `publish <queue>`, for each
  notification it publishes, and the worker a consumer span, `process <queue>`. SQL and Redis calls do
  not get spans of their own.
- **Carried as a `traceparent` string.** The API stores the W3C `traceparent` of its span in the
  notification's outbox payload. The relay's publish span continues it and goes into the AMQP message
  as a `traceparent` header. Only `traceparent` crosses the outbox and the broker; `tracestate` does not.
- **The relay span lasts until the commit.** Every publish span of a relay step ends when the step's
  transaction commits, and is marked as an error if it rolls back. All spans of one step therefore
  show the same duration.
- **The worker span starts when the dispatcher picks the delivery.** Waiting for the circuit breaker,
  a sender and a rate-limit token are events on that span (the token's wait is an attribute), and the
  outcome is an attribute; outcomes other than sent or skipped mark the span as an error.
- **Retries stay in the trace.** A retryable failure writes the failed attempt's `traceparent` into the
  new outbox row, so the next attempt's spans sit under it; a dead-lettered message carries it in its
  headers. Deliveries the reaper recovers start a new trace.
- **Batches.** Each item of `POST /v1/notifications/batch` gets its own root span,
  `create notification`, linked to the batch request's span, so a trace never holds more than one
  notification.
- **The provider sees the trace.** The provider client sends `traceparent`, and the mock provider
  records a server span under it.
- **An incoming `traceparent` is trusted.** A caller's trace context becomes the parent of the API's
  span, so callers inside the same system see one trace end to end.
- **Logs name the span.** A record logged with a context that carries a span gets `trace_id` and
  `span_id` next to `correlation_id`.

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
- **Spans for every SQL and Redis call** (`otelpgx`, `redisotel`): slow queries and rate-limit calls
  would show directly, but a notification would have three times the spans, filling Jaeger's storage
  three times as fast; their time is already visible inside the spans around them.
- **One span per relay step, linked to each notification's trace** (the OpenTelemetry messaging
  convention for batches): the step's duration and size in one place, but a notification's path would
  continue only through a link. The relay's metrics already show its steps.
- **Keeping the original `traceparent` on the notification row**, so that reaper recoveries stay in the
  trace too: it needs a migration for a path taken only when a worker dies mid-delivery.
- **A new trace per attempt, linked to the previous one:** shorter traces, but a notification's history
  spread over several.
- **Not sending `traceparent` to the provider:** no trace IDs leave the system (they are random and say
  nothing about the recipient), but the provider could not join its side to ours.
- **Starting the worker span when the delivery starts:** shorter spans, but the breaker, sender and
  rate-limit waits, the likeliest causes of delay, would not show.
- **Putting a whole batch in the request's trace:** the simplest, but a batch of 1 000 makes one trace
  of about 4 000 spans, which Jaeger's UI and search handle badly, and gives batch and single creates
  different shapes.
- **Treating the API as a public endpoint** (a new root linked to the caller's context): only we would
  choose trace IDs, but the API serves services of the same system, which should see one trace.

## Consequences

- At full load (three channels at 100 msg/s) the processes export about 2 000 spans/s, and Jaeger
  keeps roughly the last minute of traces; at demo rates it keeps hours.
- The `OTEL_*` variables are not validated at startup: a misspelt sampler is logged as a warning and
  the default sampler is used.
- If Jaeger is unreachable for long, each process's span queue (2 048 spans) fills and later spans are
  dropped; requests and deliveries are unaffected.
- A notification's trace shows its whole path, including every retry, except after a reaper recovery,
  which starts a new trace. A batch item's trace reaches the batch request only through a link.
- Under full load the worker spans are mostly waiting; the events on them say for what.
- Trace IDs reach the provider, and a caller can choose the trace ID of the notifications it creates.
- Each log record with a span is about 80 characters longer.
