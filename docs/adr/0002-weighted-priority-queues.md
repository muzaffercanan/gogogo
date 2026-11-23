# 0002. Weighted priority queues per channel

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Each channel delivers at most 100 msg/s, so under load a backlog forms and something has to decide which
message goes next. Notifications come in three priorities (high, normal, low). Strict priority gives the
best latency to high-priority messages but starves lower classes whenever high-priority volume stays above
the rate cap (for example, a breaking-news blast).

## Decision

- Each channel has three durable queues: `<channel>.high`, `<channel>.normal`, `<channel>.low`.
- Each worker consumes all three with a small prefetch (default 5) into local buffers, and a
  **work-conserving smooth weighted round-robin** picks the next message: default weights 6:3:1,
  configurable. An empty class yields its share to the others.
- The pick happens **before** acquiring a rate-limit token, so the next token always goes to the
  highest-weighted ready message.

## Alternatives considered

- **Single queue with `x-max-priority`.** Native and simple, but strict: low priority can starve indefinitely.

## Consequences

- No starvation: under contention, low priority still receives at least 10% of channel capacity.
- When high-priority volume is small (for example OTP codes), its latency is close to strict priority,
  because at most three normal and one low message are interleaved per six high ones.
- Priority inversion is bounded by prefetch × consumers per queue.
- 9 work queues + 3 dead-letter queues to declare and monitor.
- Weights are applied per worker instance; across instances the global ratio is approximate.
