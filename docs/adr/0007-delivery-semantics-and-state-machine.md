# 0007. At-least-once delivery with guarded state transitions

- **Status:** Accepted. Amended by [0015](0015-ack-after-the-sending-transition.md): the worker acknowledges a message once the notification is `sending`.
- **Date:** 2026-11-23

## Context

RabbitMQ delivers at least once, the relay may republish after a crash, the API may cancel while a worker
is sending, and a worker may crash after the provider accepted a message but before the database recorded
it. Exactly-once delivery to an external HTTP provider is not achievable, so the system must make
duplicates rare, bounded and explainable.

## Decision

- Statuses: `pending`, `queued`, `sending`, `sent`, `retry_scheduled`, `permanently_failed`, `cancelled`.
  The allowed transitions are defined in one place (`internal/domain`), and invalid ones are rejected.
- Every status write is conditional (`UPDATE … WHERE id = $1 AND status = $expected`) and records a
  `notification_events` row in the same transaction, so exactly one actor wins any race.
- The relay sets `queued` **before** publishing, inside the transaction that claims the outbox row.
- A worker acks only after the outcome is committed.
- A reaper moves notifications stuck in `sending` for longer than a 60 s lease back to `retry_scheduled`
  (or `permanently_failed` when attempts are exhausted).
- Every provider call carries `Idempotency-Key: <notification_id>`, stable across attempts.
- Cancellation is allowed from `pending`, `queued` and `retry_scheduled`; cancelling while `sending`
  returns `409 Conflict`.

## Consequences

- Duplicate deliveries from the broker or the relay are discarded by the conditional `queued → sending`
  transition.
- One duplicate window remains: a crash after the provider accepted the message but before the DB
  commit. The reaper then resends; a provider that honours the idempotency key drops the duplicate.
  The system therefore claims at-least-once delivery, not exactly-once.
- The event table provides a full audit trail of every transition.
