# 0001. PostgreSQL outbox + RabbitMQ for dispatch

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Delivery is capped at 100 msg/s per channel, so at most 300 msg/s in total. Traffic bursts are therefore an
ingest problem: requests must be accepted quickly and buffered durably, then drained in priority order at
the capped rate. The API should keep accepting requests while the broker, the rate limiter or the provider
is degraded. Writing to a database and publishing to a broker in two separate steps would risk lost or
phantom messages.

## Decision

- PostgreSQL is the source of truth. The API writes the notification and an outbox row in one transaction;
  this is the only dependency on the ingest path.
- A relay claims due outbox rows (`FOR UPDATE SKIP LOCKED`), marks the notification `queued` in the same
  transaction, publishes to RabbitMQ with publisher confirms, deletes the row and commits.
- Channel workers consume from RabbitMQ. Permanently failed messages are dead-lettered (DLX → per-channel DLQ).
- Retries and scheduled sends are outbox rows with a future `available_at`. RabbitMQ is not used for delays.

## Alternatives considered

- **PostgreSQL only (`SKIP LOCKED`).** Fewest moving parts and enough for this throughput, but the database
  becomes both store and queue, and there is no native dead-lettering or broker-side buffer.
- **Redis Streams.** Redis is needed anyway, but the burst buffer would live in memory, durability depends on
  AOF settings and the DLQ has to be hand-built.
- **Kafka.** Its throughput advantage is irrelevant under a 300 msg/s cap, and it lacks per-message
  priority, acknowledgement and delays.

## Consequences

- No dual write on the publish path or the retry path; ingest survives broker outages.
- One delay mechanism for retries and scheduled delivery; no broker plugin, no per-tier TTL queues.
- Every message still passes through PostgreSQL, so RabbitMQ does not reduce database load.
- One more infrastructure component, and a reconnect supervisor is needed (`amqp091-go` does not reconnect).
- Delivery is at-least-once. Duplicate handling is covered in a separate decision.
- Revisit if the relay becomes the bottleneck (move to change data capture) or if many independent
  consumers need the event stream (move to a log-based broker).
