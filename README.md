# gogogo

An event-driven notification service written in Go. It accepts notification requests over HTTP and
delivers them asynchronously over SMS, Email and Push through an external provider, with per-channel
rate limiting, priorities, retries and status tracking.

- **Durable ingest.** Accepting a notification is one PostgreSQL transaction (notification + outbox
  row), so the API keeps accepting while RabbitMQ, Redis or the provider are down.
- **Rate limit.** At most 100 sends start per second per channel across all workers, evenly spaced
  (GCRA in Redis).
- **Priorities.** Three queues per channel (high, normal, low) are served 6:3:1 by weighted
  round-robin, so a busy high priority cannot starve the others.
- **Retries.** Server errors, 429 and timeouts are retried with exponential backoff and full jitter
  (honouring `Retry-After`). Refusals and exhausted retries are dead-lettered. A circuit breaker per
  channel pauses sending while the provider is failing.
- **At-least-once delivery.** Guarded state transitions make sure exactly one actor wins every race
  (cancel against send, duplicate messages, crashed workers). Every provider call carries
  `Idempotency-Key: <notification id>`.
- **API.** Single and batch creates (up to 1000) with idempotency keys, scheduling up to 30 days ahead,
  cancellation, filtered and cursor-paginated listing, batch status. RFC 7807 errors throughout.
- **Observability.** Prometheus metrics with a provisioned Grafana dashboard, OpenTelemetry traces in
  Jaeger (one trace per notification, across retries), JSON logs with a correlation ID.

## Quick start

Needs Docker with Compose. Go 1.27 is only needed to build or test outside containers.

```sh
cp .env.example .env        # optional: defaults work as they are
docker compose up --build -d
curl -s localhost:8080/health/ready
```

This starts PostgreSQL, RabbitMQ, Redis, the migrations, the api, relay and worker processes, a mock
provider, Jaeger, Prometheus and Grafana.

| What | Where |
|---|---|
| API, with Swagger UI at `/docs` | <http://localhost:8080> ([OpenAPI spec](api/openapi.yaml)) |
| Grafana, `Notifications` dashboard | <http://localhost:3000> (no login, bound to localhost) |
| Jaeger | <http://localhost:16686> |
| Prometheus | <http://localhost:9091> |
| RabbitMQ management | <http://localhost:15672> (guest / guest) |
| Ops ports of relay and worker (`/health/*`, `/metrics`) | 8081, 8082 |
| Mock provider | <http://localhost:9090> |

Workers deliver to the bundled mock provider, which answers after 50 ± 50 ms and can be told to fail
(`MOCK_*` in [`.env.example`](.env.example)). To deliver to [webhook.site](https://webhook.site)
instead, set `PROVIDER_URL` in `.env` to your own URL there and run `docker compose up -d worker`.

`make down` stops the stack; `docker compose down -v` also deletes the database.

## Architecture

```mermaid
flowchart LR
  C[Client] -->|HTTP| API[api]
  API -->|one transaction:<br>notification + outbox row| PG[(PostgreSQL)]
  REL[relay + reaper] -->|claim due outbox rows| PG
  REL -->|publish, wait for confirms| MQ[(RabbitMQ<br>channel.priority queues)]
  MQ --> W[worker<br>per channel]
  W -->|rate-limit slot| RD[(Redis)]
  W -->|POST, Idempotency-Key| P[Provider]
  W -->|status, retry as outbox row| PG
  W -->|failed for good| DLQ[(channel.dlq)]
```

- **api** validates a request and writes the notification and an outbox row in one transaction, then
  answers `202 Accepted`. PostgreSQL is its only dependency.
- **relay** claims due outbox rows (`FOR UPDATE SKIP LOCKED`), marks their notifications `queued` and
  publishes them to `<channel>.<priority>` with publisher confirms before it commits. It wakes on
  `NOTIFY` and polls every second as a fallback. Its **reaper** gives notifications whose worker died
  while sending (a 60 s lease) back to the retry path.
- **worker** consumes the three queues of each channel. A weighted round-robin picks the next message,
  the worker takes a slot from the rate limiter, moves the notification `queued → sending`, acks the
  message and calls the provider. The outcome is `sent`, `retry_scheduled` (a new outbox row with
  `available_at` in the future; retries and scheduled sends use the same mechanism) or
  `permanently_failed` (also published to the channel's dead-letter queue).

Statuses: `pending → queued → sending → sent`, with `retry_scheduled` looping back to `queued` and
`permanently_failed` at the end of the retries. `pending`, `queued` and `retry_scheduled` can be
cancelled. Every transition is a conditional update recorded in `notification_events`, defined in
[`internal/domain`](internal/domain).

All three processes are built into one image and scale independently. The reasons behind each choice
are in the [architecture decision records](docs/adr/README.md).

## API

The contract is [`api/openapi.yaml`](api/openapi.yaml); handlers are generated from it.

Create a notification. `priority` defaults to `normal`. The `Idempotency-Key` header is optional: a
retry with the same key and body returns the original with `Idempotent-Replayed: true`, while the same
key with another body is refused with `422`.

```sh
curl -si localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: 5d3c1a7e-2b4f-4e8a-9c6d-1f0e2a3b4c5d' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Your code is 482913","priority":"high"}'
```

```http
HTTP/1.1 202 Accepted
Location: /v1/notifications/01a20f9a-4774-790a-b556-1d5b72dda574
Idempotent-Replayed: false
X-Correlation-Id: 01a20f9a-476a-7d09-8b15-cc1f7ce9ede3

{"id":"01a20f9a-4774-790a-b556-1d5b72dda574","status":"pending","channel":"sms","priority":"high", ...}
```

Get it. Under a second later it has been sent:

```sh
curl -s localhost:8080/v1/notifications/01a20f9a-4774-790a-b556-1d5b72dda574
```

```json
{
  "id": "01a20f9a-4774-790a-b556-1d5b72dda574",
  "status": "sent",
  "attempts": 1,
  "max_attempts": 5,
  "provider_message_id": "c7756c7a-435e-4a02-835b-4163a04d7129",
  "created_at": "2026-11-24T22:08:18.804863Z",
  "sent_at": "2026-11-24T22:08:19.408754Z",
  ...
}
```

Schedule one (`scheduled_at` up to 30 days ahead), then cancel it. Cancelling a notification that is
sending or already sent answers `409`.

```sh
curl -s localhost:8080/v1/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"email","recipient":"jane@example.com","content":"Reminder","scheduled_at":"2026-12-01T09:00:00Z"}'
curl -s -X POST localhost:8080/v1/notifications/<id>/cancel
```

Create up to 1000 in one batch. Each item is validated on its own; the batch fails as a whole only if it
is malformed, empty or too large.

```sh
curl -s localhost:8080/v1/notifications/batch -H 'Content-Type: application/json' -d '{"notifications":[
  {"channel":"email","recipient":"jane@example.com","content":"Flash sale starts now!"},
  {"channel":"push","recipient":"not a token","content":"Flash sale starts now!"}]}'
```

```json
{
  "batch_id": "01a20f9a-9fe7-7813-bae5-d9487fba2f38",
  "total": 2, "accepted": 1, "rejected": 1,
  "items": [
    {"index": 0, "result": "accepted", "notification_id": "01a20f9a-9fe7-7b0f-8c30-a35fb7410687"},
    {"index": 1, "result": "rejected",
     "errors": [{"field": "recipient", "code": "invalid_format", "message": "must be a device token without whitespace"}]}
  ]
}
```

Follow a batch (status counts and a page of its notifications) and list notifications with filters
(`status`, `channel`, `created_from`, `created_to`). Pages are cursor-based: pass `next_cursor` back
as `cursor`.

```sh
curl -s localhost:8080/v1/batches/01a20f9a-9fe7-7813-bae5-d9487fba2f38
curl -s 'localhost:8080/v1/notifications?status=sent&channel=sms&limit=20'
```

Errors are `application/problem+json` with one entry per invalid field:

```json
{
  "type": "/problems/validation-error",
  "title": "Validation failed",
  "status": 400,
  "detail": "The request has 2 invalid fields.",
  "instance": "/v1/notifications",
  "errors": [
    {"field": "recipient", "code": "invalid_format", "message": "must be an E.164 phone number, e.g. +905551234567"},
    {"field": "content", "code": "required", "message": "is required"}
  ]
}
```

The provider receives `POST {"to", "channel", "content"}` with `Idempotency-Key: <notification id>`
and is expected to answer `202 {"messageId", "status", "timestamp"}`. A 2xx counts as accepted; 5xx,
429 and 408 are retried; any other answer fails the notification for good
([ADR 0014](docs/adr/0014-provider-response-classification.md)).

## Configuration

Everything is set through environment variables; [`.env.example`](.env.example) lists them with their
defaults, and Compose reads `.env`. The ones you are most likely to change:

| Variable | Default | Meaning |
|---|---|---|
| `PROVIDER_URL` | the mock provider | where workers deliver |
| `RATE_LIMIT_PER_SECOND` | 100 | sends each channel starts per second, across all workers |
| `MAX_ATTEMPTS` | 5 | attempts before a notification is `permanently_failed` |
| `PRIORITY_WEIGHT_HIGH` / `_NORMAL` / `_LOW` | 6 / 3 / 1 | shares while all priorities have messages waiting |
| `WORKER_CONCURRENCY` | 50 | sends in flight per channel and worker; size it to rate × provider latency |
| `WORKER_PREFETCH` | 20 | unacknowledged messages per work queue and worker |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Jaeger | set it empty to turn tracing off |

## Observability

- **Metrics.** Every process serves `/metrics`. Among them: `notifications_sent_total`,
  `notifications_failed_total`, `retries_total`, `queue_depth`, `outbox_backlog`,
  `ratelimit_wait_seconds`, `circuit_breaker_state`, `provider_request_duration_seconds` and the HTTP
  metrics of the API. The `Notifications` dashboard in Grafana shows delivery, queues, health and the
  API on one page.
- **Traces.** One trace per notification, from the HTTP request through the relay, every attempt and
  the provider call, including retries and batch items. Open Jaeger and search by service, or by the
  `notification.id` tag.
- **Logs.** JSON on stdout (`make logs`). Every line about a request or a notification carries its
  `correlation_id` (returned as `X-Correlation-Id`) and `trace_id`.
- **Health.** `/health/live` answers while the process runs; `/health/ready` checks the dependencies
  the process needs (the worker also reports not ready while the rate limiter is unreachable, when it
  stops sending rather than exceed the limit).

## Testing

| Command | What it runs |
|---|---|
| `make test` | unit tests |
| `make test-integration` | unit and integration tests against throwaway PostgreSQL, RabbitMQ and Redis containers (Testcontainers; needs Docker) |
| `make test-race` | unit tests with the race detector, in a Linux container |
| `make lint` | golangci-lint |
| `make load` | the k6 burst below, against the running stack |

The integration tests include end-to-end scenarios in [`internal/e2e`](internal/e2e): the api, relay
and workers run in the test process against real containers and a scripted provider, driven only
through the HTTP API. They cover delivery, retries and dead-lettering, cancel racing the worker,
concurrent creates with one idempotency key, recovery from a dead worker and from a relay that dies
between publishing and committing, and several workers together staying within the rate limit in any
rolling second.

CI (GitHub Actions) runs lint, a check that generated code is up to date, govulncheck, unit and
integration tests with the race detector, and the image build.

## Load test

`make load` runs [`test/load/burst.js`](test/load/burst.js) with k6 against the running Compose stack.
Ten virtual users each send one batch of 1000 notifications at the same moment: 10 000 in all, a
third per channel, 2 in 10 high, 5 in 10 normal and 3 in 10 low priority. k6 reports how long each
batch took to be accepted. When every notification has been sent, k6 reads them back through the API
and prints the drain rate of each channel and when each priority was sent. These times come from
`created_at` and `sent_at`, both written by the database.

Results for the default settings (100 per second per channel, `WORKER_PREFETCH=20`, mock provider
50 ± 50 ms). The machine was Windows 11 with Docker Desktop (WSL 2, 12 CPUs, 11 GiB), and the whole
stack and k6 ran in the same VM. Seconds count from the first notification's `created_at`. Each
channel behaved the same within a run (within 0.1 s), so the table shows the mean of the three
channels.

| Run | Batch accepted in (median / max) | Drain rate per channel | Median send: high / normal / low | Last send |
|---|---|---|---|---|
| 1 | 0.83 s / 0.89 s | 98.1/s | 6.6 / 18.6 / 27.5 s | 34.1 s |
| 2 | 3.55 s / 3.61 s | 79.3/s | 22.1 / 31.2 / 37.3 s | 46.1 s |
| 3 | 0.84 s / 0.86 s | 100.1/s | 7.0 / 18.7 / 27.6 s | 34.2 s |
| 4 | 3.06 s / 3.15 s | 60.5/s | 39.6 / 43.7 / 46.4 s | 58.4 s |
| 5 | 1.08 s / 1.22 s | 99.6/s | 10.1 / 19.2 / 28.0 s | 34.8 s |
| 6 | 2.82 s / 2.90 s | 58.8/s | 39.2 / 45.1 / 48.4 s | 59.2 s |
| 7 | 0.83 s / 0.88 s | 100.0/s | 7.5 / 18.8 / 27.6 s | 34.3 s |

- **Ingest.** The API accepts a batch of 1000 in 0.8–1.1 s, even while nine more arrive at the same
  time: about 10 000 notifications per second into PostgreSQL. No request failed in any run.
- **Drain.** In runs 1, 3, 5 and 7 each channel sends at its limit: 3333 notifications in 33–34 s,
  which is 100 per second. In every run, every notification ended up `sent`.
- **Priority.** With all three priorities waiting, the weights 6:3:1 give high 60 sends a second, so
  its 667 per channel are done by about 12.5 s. Normal and low then share the channel 3:1. Low keeps
  its tenth throughout and is not starved.
- **Slow runs (2, 4, 6).** The same burst sometimes runs about three times slower everywhere. Batches take
  ~3 s to accept, each relay publish and commit takes 0.65–1.9 s instead of ~0.1 s, and the workers'
  sending transitions slow down with it. Nothing failed, but the backlog reached the queues late, so
  the priorities were sent closer together. The API and the relay share only PostgreSQL. All the slow
  steps are commits, and slow runs alternated with fast ones on the same data, so the likely cause is
  disk latency inside the Docker Desktop VM rather than the service. This was not measured on Linux.
- **Prefetch.** With the earlier default `WORKER_PREFETCH=5`, the same burst drained at 72/s per
  channel: 100/s while all three priorities had messages, then about 78/s and 30/s. A message stays
  unacknowledged until its `queued → sending` transition commits, so each priority sends at most
  prefetch / commit latency (ADR [0015](docs/adr/0015-ack-after-the-sending-transition.md)).

While it runs, the `Notifications` dashboard in Grafana (<http://localhost:3000>) shows the queue
depths, send rates and rate-limit waits. `docker compose run --rm -e BATCHES=20 k6` changes the
burst; `BATCH_SIZE` and `DRAIN_TIMEOUT_SECONDS` work the same way.

## Project layout

| Path | Contents |
|---|---|
| `cmd/{api,relay,worker,mockprovider}` | entrypoints, all built into one image |
| `api/openapi.yaml` | the API contract; `internal/api/oapi` is generated from it |
| `internal/api`, `internal/service` | HTTP handlers and problem+json; use cases (validation, idempotency, cancel) |
| `internal/domain` | notification model, state machine, validation, SMS encoding |
| `internal/outbox` | relay and reaper |
| `internal/queue` | RabbitMQ topology, reconnecting supervisor, publisher, consumer |
| `internal/worker` | priority picker, circuit breaker, sender pool, delivery, retry policy |
| `internal/ratelimit` | GCRA limiter (one Lua script in Redis) |
| `internal/provider` | provider client and the mock provider |
| `internal/repository` | SQL queries; `sqlc` generates the Go code |
| `internal/platform` | config, logging, health, HTTP server, backoff, tracing |
| `internal/e2e`, `internal/testutil` | end-to-end tests; container helpers for integration tests |
| `migrations/` | SQL migrations (golang-migrate) |
| `deploy/` | Prometheus scrape config, Grafana datasource and dashboard |
| `test/load/` | k6 scripts |
| `docs/adr/` | architecture decision records |

After changing the spec, SQL queries or migrations, run `make generate`; generated code is committed
and CI fails when it is stale.

## Design decisions

Each significant choice has a short record with the alternatives considered and the consequences:
the outbox and RabbitMQ, weighted priority queues, GCRA rate limiting, failing closed when Redis is
down, spec-first API, SQL-first data access, delivery semantics, idempotency keys, retries and
dead-lettering, the queue topology, provider answer classification, acknowledging after the sending
transition, tracing and the metrics stack. See [docs/adr](docs/adr/README.md).

## Not included

- **Live status over WebSocket.** The design is recorded in
  [ADR 0010](docs/adr/0010-realtime-status-via-redis-pubsub.md) but not built; clients poll
  `GET /v1/notifications/{id}` or `GET /v1/batches/{id}`.
- **Templates.** Content is sent as given.
- **Authentication and multi-tenancy.** The API is open, and idempotency keys are global.
- **Data retention.** Notifications and their events are kept until deleted by hand.
