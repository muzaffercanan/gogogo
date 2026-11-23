# 0008. Idempotency keys stored in PostgreSQL

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Clients retry on timeouts and network errors. A retried create request must not produce a second
notification or a second send. Concurrent retries with the same key must also be safe.

## Decision

- Clients send an `Idempotency-Key` header. It is stored on the created resource
  (`notifications.idempotency_key` or `batches.idempotency_key`, both `UNIQUE`) together with a hash of
  the request body.
- The insert uses `ON CONFLICT (idempotency_key) DO NOTHING`. If no row is returned, the existing
  resource is read back.
- Same key and same body: the original resource is returned with `Idempotent-Replayed: true`.
  Same key and a different body: `422 Unprocessable Entity`.
- For batches, the key covers the whole batch.
- Keys are global (the service has no tenant model) and live as long as their rows.

## Alternatives considered

- **A Redis key cache in front of the database.** It adds a second source of truth that can disagree
  with the database and is not transactional with the insert.

## Consequences

- Idempotency is enforced in the same transaction as the write; there is no window in which two requests
  both succeed.
- One extra unique index; no expiry mechanism is needed.
