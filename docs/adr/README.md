# Architecture Decision Records

Short records of the significant design decisions: context, decision, alternatives, consequences.

| # | Decision | Status |
|---|---|---|
| [0001](0001-outbox-and-rabbitmq-for-dispatch.md) | PostgreSQL outbox + RabbitMQ for dispatch | Accepted |
| [0002](0002-weighted-priority-queues.md) | Weighted priority queues per channel | Accepted |
| [0003](0003-gcra-rate-limiting.md) | GCRA rate limiting in Redis (100 msg/s per channel, burst 1) | Accepted |
| [0004](0004-fail-closed-on-rate-limiter-outage.md) | Fail closed when the rate limiter is unavailable | Accepted |
| [0005](0005-spec-first-openapi.md) | Spec-first API with OpenAPI and oapi-codegen | Accepted |
| [0006](0006-sql-first-data-access.md) | SQL-first data access with pgx and sqlc | Accepted |
| [0007](0007-delivery-semantics-and-state-machine.md) | At-least-once delivery with guarded state transitions | Accepted |
| [0008](0008-idempotency-keys-in-postgres.md) | Idempotency keys stored in PostgreSQL | Accepted |
| [0009](0009-retry-and-failure-handling.md) | Retry policy, dead-lettering and circuit breaking | Accepted |
| [0010](0010-realtime-status-via-redis-pubsub.md) | Real-time status updates over WebSocket via Redis pub/sub | Accepted |
