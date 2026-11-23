# CLAUDE.md

Guidance for AI coding assistants working in this repository.

## Project

Event-driven notification service written in Go. It accepts notification requests over HTTP,
queues them, and delivers them through SMS, Email and Push providers with rate limiting,
priorities, retries and status tracking.

Status: early scaffolding. Architecture decisions are recorded in `docs/adr/` as they are made.

## Ground rules

- Design and architecture decisions belong to the maintainer. Present the options with their
  trade-offs and a recommendation; never pick one silently.
- Record every accepted decision as an ADR: `docs/adr/NNNN-short-title.md`
  (Context / Decision / Consequences), short and technical.
- Small, focused commits using Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`,
  `docs:`, `chore:`, `ci:`, `build:`).
- Never commit secrets or real provider endpoints. Use `.env` (gitignored) and keep
  `.env.example` up to date with placeholders.
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
| Start everything (Postgres, RabbitMQ, Redis, migrations, api, relay, worker, mock provider) | `docker compose up --build -d` or `make up` |
| Stop / wipe data | `make down` / `docker compose down -v` |
| Unit tests | `make test` (`go test -count=1 ./...`) |
| Integration tests (throwaway containers, needs Docker) | `make test-integration` (`go test -tags integration ./...`) |
| Race tests (Linux container) | `make test-race` |
| Lint | `make lint` (`golangci-lint run ./...`, config in `.golangci.yml`) |
| Build binaries | `make build` → `./bin` |
| Regenerate code after changing `api/openapi.yaml`, SQL queries or migrations | `make generate` (oapi-codegen + sqlc; sqlc runs in Docker) |

Ports: api 8080, relay ops 8081, worker ops 8082, mock provider 9090, RabbitMQ UI 15672 (guest/guest).
Every process serves `/health/live`, `/health/ready` and `/metrics`.

## Layout

- `cmd/{api,relay,worker,mockprovider}`: entrypoints, all built into one image (`Dockerfile`).
- `api/openapi.yaml`: the API contract (source of truth for request/response shapes).
- `internal/api`: handlers of the generated strict server, middleware and the problem+json mapping.
- `internal/api/oapi`: types and strict server interface generated from the spec (`oapi.gen.go`).
- `internal/service`: use cases on domain types (validation, idempotency, cancellation); no HTTP.
- `internal/domain`: notification model, state machine, validation rules, SMS encoding, error types.
- `internal/repository/queries`: SQL queries; `sqlc` (config `sqlc.yaml`) generates
  `internal/repository/sqlc` from them and the migrations.
- `internal/platform`: config (env), logging (slog JSON + correlation ID), health, ops mux, HTTP server.
- `internal/provider/mockserver`: fake provider with configurable latency and failure rates.
- `internal/testutil/pgtest`: PostgreSQL container for integration tests; one migrated database per test.
- `migrations/`: versioned SQL (`golang-migrate`), applied by the `migrate` compose service.
- `docs/adr/`: architecture decision records.

## Code conventions

- `gofmt`/`goimports` clean; `golangci-lint run` must pass.
- `context.Context` is the first parameter of anything that does I/O.
- Wrap errors with context (`fmt.Errorf("doing x: %w", err)`); no panics outside `main`.
- No mutable package-level state; dependencies are passed in explicitly.
- Table-driven tests; every bug fix comes with a test that reproduces it.
- Code that talks to PostgreSQL, RabbitMQ or Redis is tested against real containers, not mocks. Those
  test files start with `//go:build integration`; database tests use `pgtest.Server.NewDatabase(t)`.
- Never edit `*.gen.go` by hand: change the source (spec, SQL) and run `make generate`. Generated code is
  committed; CI fails if it is stale.
