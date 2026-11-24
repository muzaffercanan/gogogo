# 0010. Real-time status updates over WebSocket via Redis pub/sub

- **Status:** Accepted, not implemented yet. Until it is, clients poll `GET /v1/notifications/{id}` or
  `GET /v1/batches/{id}`.
- **Date:** 2026-11-23

## Context

Clients want live status updates for a notification or a batch. Status changes happen in the relay and
the workers, while WebSocket connections terminate on any of several API instances.

## Decision

- After committing a status change, the relay, the workers and the reaper publish
  `{id, batch_id, status, at}` to a Redis pub/sub channel.
- Every API instance subscribes and forwards events to its local WebSocket connections
  (`GET /v1/ws?notification_id=…` or `?batch_id=…`), using `coder/websocket`.
- Delivery over this path is best-effort (at-most-once). After reconnecting, clients reconcile with
  `GET /v1/notifications/{id}` or `GET /v1/batches/{id}`.

## Alternatives considered

- **PostgreSQL `LISTEN/NOTIFY`.** Avoids Redis for this feature, but holds a dedicated connection per API
  instance and adds load to the database, which is already the busiest component.

## Consequences

- Redis already serves the rate limiter, so no new infrastructure is needed.
- If Redis is down, live updates stop but the API keeps working; polling remains correct.
