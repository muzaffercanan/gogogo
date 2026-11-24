# 0004. Fail closed when the rate limiter is unavailable

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

The per-channel rate limit lives in Redis (see 0003). If Redis is unreachable, workers cannot know how
many messages other instances have sent. Redis is not on the ingest path: the API keeps accepting
notifications either way, so the question only concerns delivery.

## Decision

Fail closed. If a token cannot be acquired because the limiter is unavailable, the worker pauses
consumption for that channel (the same mechanism used when a circuit breaker is open), reports not-ready
and exposes the condition as a metric. Messages stay in RabbitMQ and delivery resumes automatically once
Redis is reachable again.

As built, pausing means the dispatcher holds the message it has picked and retries the limiter with
backoff. The messages already buffered stay unacknowledged and the prefetch limit keeps the broker
from sending more, so the rest stay in RabbitMQ; the consumers are not cancelled.

## Alternatives considered

- **Fail open with a local fallback limiter** at 100/N per instance (N from configuration or from the
  RabbitMQ consumer count). Delivery continues at reduced speed, but a wrong N exceeds the provider limit.
- **Fail open without a limit.** Rejected: it would trigger provider throttling and a retry storm.

## Consequences

- The provider limit is never exceeded.
- A Redis outage stops delivery on all channels for its duration; nothing is lost.
- Upgrade paths: Redis high availability, or the local fallback limiter above.
