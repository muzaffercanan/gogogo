# 0006. SQL-first data access with pgx and sqlc

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Correctness depends on a handful of precise queries: `FOR UPDATE SKIP LOCKED` in the relay, conditional
status transitions (`UPDATE … WHERE status = $expected`), and `INSERT … ON CONFLICT DO NOTHING` for
idempotency. These must stay explicit and reviewable.

## Decision

- Driver: `pgx` v5 with a connection pool.
- Queries live in `.sql` files; `sqlc` generates typed Go code and checks every query against the schema
  at build time.
- The list endpoint (optional filters + cursor) is the one exception: its SQL is built by hand, because
  `($1 IS NULL OR col = $1)` patterns can defeat index usage.
- Schema changes are versioned SQL migrations run with `golang-migrate`; `sqlc` reads the same files.

## Alternatives considered

- **Hand-written pgx code everywhere.** Full control, but repetitive, and column/type mistakes only
  surface at runtime.
- **GORM.** Fast for CRUD, but it hides exactly the queries this system depends on.

## Consequences

- Critical SQL is visible in code review and type-checked against the schema.
- Adds a code-generation step (`make generate`, verified in CI).
- One hand-built query needs its own tests.
