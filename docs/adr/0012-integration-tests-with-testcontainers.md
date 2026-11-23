# 0012. Integration tests against real dependencies with Testcontainers

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

The correctness of the service rests on SQL and broker semantics: `ON CONFLICT` for idempotency,
conditional `UPDATE`s for state transitions, `FOR UPDATE SKIP LOCKED` for the outbox, publisher confirms
and dead-lettering. Mocks of the database or the broker would only restate our assumptions about these
behaviours instead of checking them.

## Decision

- Code that talks to PostgreSQL, RabbitMQ or Redis is tested against the real services, started as
  throwaway containers with Testcontainers for Go.
- Integration tests and their helpers carry the `integration` build tag. `make test` stays fast and needs
  no Docker; `make test-integration` runs everything, and CI runs it in a separate job with the race
  detector.
- PostgreSQL starts once per test binary. The migrations are applied to a template database, and every
  test gets its own database cloned from it (`internal/testutil/pgtest`), so tests are isolated and can run
  in parallel without truncating tables. The migrations are run as plain SQL files rather than through
  golang-migrate.
- Helpers for RabbitMQ and Redis follow the same pattern when the code that needs them is written.

## Consequences

- Repository and messaging code is tested for real, including concurrency behaviour.
- Integration tests need a Docker engine; the first run also pulls images.
- Testcontainers adds a large test-only dependency tree to `go.mod` (the Docker client and its
  dependencies). It does not end up in the service binaries.
- On Docker Desktop, Testcontainers' cleanup container needs the socket path override that the Makefile
  sets (`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE`).
