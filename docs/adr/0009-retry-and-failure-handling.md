# 0009. Retry policy, dead-lettering and circuit breaking

- **Status:** Accepted. Amended by [0014](0014-provider-response-classification.md): malformed 2xx answers, 408 and the Retry-After cap.
  Amended by [0015](0015-ack-after-the-sending-transition.md): the message is acknowledged before the
  provider call, so a dead letter is published to the dead-letter exchange after the outcome commits
  instead of being `nack`ed.
- **Date:** 2026-11-23

## Context

Provider calls fail in different ways: transient errors (5xx, timeouts, throttling) and permanent errors
(invalid recipient). Retrying permanent errors wastes capacity, retrying transient errors too quickly
amplifies an outage, and a provider that is down should not be hammered by every worker.

## Decision

- **Classification:** 5xx, 429, timeouts and connection errors are retryable. Other 4xx responses and
  malformed provider responses are permanent.
- **Backoff:** exponential with full jitter, `rand(0, min(60s, 1s · 2^(attempt-1)))`, at most 5 attempts
  (configurable). On 429, the delay is at least the `Retry-After` value.
- **Mechanism:** a retry is an outbox row with a future `available_at`, written in the same transaction as
  the `sending → retry_scheduled` transition. RabbitMQ is not used for delays.
- **Dead letters:** permanent failures and exhausted retries become `permanently_failed` and are
  `nack`ed without requeue. Each work queue's dead-letter exchange routes them to `<channel>.dlq`.
  PostgreSQL remains the source of truth; the DLQ is the operational view.
- **Circuit breaker:** one per channel, counting only retryable failures. While it is open, the channel's
  workers stop consuming; half-open lets a single probe through after the cooldown. As built, the
  dispatcher holds the message it has picked until the breaker allows it, and the prefetch limit keeps
  the rest in RabbitMQ (the same pause as in ADR 0004). Defaults: it opens for 30 s when more than half of at
  least 20 deliveries in a rolling 10 s window failed with a retryable error.

## Consequences

- No dual write and no broker plugin on the retry path; scheduled sends and retries share one mechanism.
- Retries survive restarts, because they are rows in the database.
- During a provider outage, messages wait in the queue instead of burning attempts.
- Breaker state, retries and DLQ depth are exposed as metrics.
