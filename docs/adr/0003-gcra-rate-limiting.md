# 0003. GCRA rate limiting in Redis (100 msg/s per channel, burst 1)

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

Each channel may send at most 100 messages per second, and the limit must hold across all worker
instances. The common algorithms differ in what "100 per second" actually guarantees:

| Algorithm | Max in any 1 s window | Traffic shape towards the provider |
|---|---|---|
| Fixed window counter | ~200 (two windows meet at the boundary) | Bursty |
| Token bucket, capacity 100 | ~200 (full bucket plus refill) | Bursty |
| GCRA, burst 1 | 100 | Evenly spaced, one every 10 ms |
| Sliding-window log | 100 | Bursty (up to 100 at once) |

## Decision

Use GCRA with a burst of 1, stored in Redis (one key per channel) and evaluated atomically in Lua using
Redis server time. A worker acquires a token after selecting the next message and before marking it
`sending`.

## Consequences

- At most 100 sends start in any 1-second window per channel, evenly spaced, across all instances.
- One Redis round-trip per message; unused capacity is not banked as burst.
- The limit applies when the send starts; network jitter means the provider may observe slightly
  different spacing.
- Redis outage behaviour is defined in a separate decision.
