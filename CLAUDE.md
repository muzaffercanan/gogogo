# CLAUDE.md

Guidance for AI coding assistants working in this repository.

## Project

Event-driven notification service written in Go. It accepts notification requests over HTTP,
queues them, and delivers them through SMS, Email and Push providers with rate limiting,
priorities, retries and status tracking.

Status: feature-complete for the core flow (create, batch, schedule, cancel, list, delivery with rate
limiting, priorities, retries, dead-lettering, metrics and tracing). Live status over WebSocket (ADR 0010)
and templates are not built yet. `README.md` is the user-facing overview; architecture decisions are in
`docs/adr/`.

## Ground rules

- Design and architecture decisions belong to the maintainer. Present the options with their
  trade-offs and a recommendation; never pick one silently.
- Record every accepted decision as an ADR: `docs/adr/NNNN-short-title.md`
  (Context / Decision / Consequences), short and technical.
- Small, focused commits using Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`,
  `docs:`, `chore:`, `ci:`, `build:`).
- Never commit secrets or real provider endpoints. Use `.env` (gitignored) and keep
  `.env.example` up to date with placeholders. Every variable it documents must also be passed to its
  service in `docker-compose.yml` (a unit test in `internal/platform/config` checks this).
- Do not push without explicit approval from the maintainer.

## Environment notes

- The primary dev machine runs Windows; CI and reviewers use Linux/macOS.
- All text files are LF (`.gitattributes`). Shell scripts must be POSIX `sh`.
- The race detector needs cgo, so run race tests in Docker/CI rather than natively on Windows.
- Makefile recipes use POSIX `sh` syntax; on Windows, run `make` from Git Bash.
- Integration tests need Docker. Prefer `make test-integration`: it sets what Testcontainers needs on
  Docker Desktop. Running `go test -tags integration` by hand from Git Bash on Windows needs
  `MSYS_NO_PATHCONV=1 TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
  DOCKER_HOST=npipe:////./pipe/docker_engine` (without `DOCKER_HOST`, packages tested in parallel fail
  at random to find Docker).

## Commands

| Task | Command |
|---|---|
| Start everything (Postgres, RabbitMQ, Redis, Jaeger, Prometheus, Grafana, migrations, api, relay, worker, mock provider) | `docker compose up --build -d` or `make up` |
| Stop / wipe data | `make down` / `docker compose down -v` |
| Unit tests | `make test` (`go test -count=1 ./...`) |
| Integration tests (throwaway containers, needs Docker) | `make test-integration` (`go test -tags integration ./...`) |
| Race tests (Linux container) | `make test-race` |
| Load test (k6 burst of batch creates against the running stack) | `make load` (`docker compose run --rm k6`) |
| Lint | `make lint` (`golangci-lint run ./...`, config in `.golangci.yml`) |
| Build binaries | `make build` → `./bin` |
| Regenerate code after changing `api/openapi.yaml`, SQL queries or migrations | `make generate` (oapi-codegen + sqlc; sqlc runs in Docker) |

Ports: api 8080 (Swagger UI at `/docs`, the spec at `/openapi.yaml`), relay ops 8081, worker ops 8082, mock provider 9090, RabbitMQ UI 15672 (guest/guest),
Jaeger UI 16686 (OTLP/HTTP on 4318), Prometheus 9091, Grafana 3000 (localhost only, no login; the
`Notifications` dashboard is its home page).
Every process serves `/health/live`, `/health/ready` and `/metrics`.

## Layout

- `cmd/{api,relay,worker,mockprovider}`: entrypoints, all built into one image (`Dockerfile`).
- `api/openapi.yaml`: the API contract (source of truth for request/response shapes).
- `internal/api`: handlers of the generated strict server, middleware and the problem+json mapping.
- `internal/api/oapi`: types and strict server interface generated from the spec (`oapi.gen.go`).
- `internal/service`: use cases on domain types (validation, idempotency, cancellation); no HTTP.
- `internal/domain`: notification model, state machine, validation rules, SMS encoding, error types.
- `internal/queue`: RabbitMQ topology (exchanges, quorum work and dead-letter queues), the connection
  supervisor that reconnects and redeclares it, the publisher that waits for confirms, the consumer that
  buffers each work queue per priority, and the `queue_depth` collector.
- `internal/outbox`: the relay (claims due outbox rows, queues and publishes them) and the reaper (recovers
  deliveries whose `sending` lease expired), with their metrics.
- `internal/worker`: delivery per channel: weighted priority picker, circuit breaker, rate-limit token,
  sender pool; the delivery itself (`queued → sending`, ack, provider call, outcome), retry policy and
  worker metrics.
- `internal/ratelimit`: GCRA rate limiter in Redis (one Lua script reserves each send slot).
- `internal/provider`: HTTP client for the provider (`Idempotency-Key`, answer classification).
- `internal/repository/queries`: SQL queries; `sqlc` (config `sqlc.yaml`) generates
  `internal/repository/sqlc` from them and the migrations.
- `internal/platform`: config (env), logging (slog JSON + correlation ID), health, ops mux, HTTP server,
  exponential backoff with full jitter, tracing (OpenTelemetry over OTLP/HTTP, configured by the
  standard `OTEL_*` variables).
- `internal/provider/mockserver`: fake provider with configurable latency and failure rates.
- `internal/e2e`: end-to-end tests: the api, relay and workers assembled in the test process against
  containers, driven through the HTTP API with a scripted provider.
- `internal/testutil/pgtest`: PostgreSQL container for integration tests; one migrated database per test.
- `internal/testutil/rabbittest`: RabbitMQ container for integration tests; one virtual host per test.
- `internal/testutil/redistest`: Redis container for integration tests; one logical database per test.
- `internal/testutil/redisclock`: reads a Redis server's clock while a test runs and reports how fast it
  ran against the host's (Docker Desktop's VM clock drifts and steps), for tests that compare Redis
  times with host times.
- `test/load`: k6 scripts, run by the `k6` compose service (profile `load`). `burst.js` sends batches at
  once, waits until they are sent and prints the drain rate per channel and when each priority was sent.
- `migrations/`: versioned SQL (`golang-migrate`), applied by the `migrate` compose service.
- `deploy/prometheus`, `deploy/grafana`: Prometheus scrape config, Grafana datasource and dashboard
  provisioning for the Compose stack. Update the dashboard JSON when a metric is renamed; Grafana loads
  it only at startup (`docker compose restart grafana`).
- `docs/adr/`: architecture decision records.

## Code conventions

- `gofmt`/`goimports` clean; `golangci-lint run` must pass.
- `context.Context` is the first parameter of anything that does I/O.
- Wrap errors with context (`fmt.Errorf("doing x: %w", err)`); no panics outside `main`.
- No mutable package-level state; dependencies are passed in explicitly.
- Table-driven tests; every bug fix comes with a test that reproduces it.
- Code that talks to PostgreSQL, RabbitMQ or Redis is tested against real containers, not mocks. Those
  test files start with `//go:build integration`; database tests use `pgtest.Server.NewDatabase(t)`, broker
  tests `rabbittest.Server.NewVHost(t)`, Redis tests `redistest.Server.NewClient(t)` or `NewURL(t)`. Logic
  that needs a controllable Redis clock (the rate limiter's script) is also tested on miniredis.
- Code that makes spans takes a `trace.TracerProvider` (no global provider). Tests that do not look at
  spans pass `noop.NewTracerProvider()`; tests that do record them with `tracetest.NewSpanRecorder()`
  in an `sdktrace.NewTracerProvider`.
- Never edit `*.gen.go` by hand: change the source (spec, SQL) and run `make generate`. Generated code is
  committed; CI fails if it is stale.
