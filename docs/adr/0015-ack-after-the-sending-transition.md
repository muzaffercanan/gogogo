# 0015. Acknowledge a message once its notification is sending

- **Status:** Accepted. Amends [0007](0007-delivery-semantics-and-state-machine.md).
- **Date:** 2026-11-24

## Context

ADR 0007 has the worker acknowledge a message only after the delivery's outcome is committed, so the
message stays unacknowledged for the whole provider call. With a prefetch of 5 per queue (ADR 0002)
a worker can then have at most 5 sends of one priority in flight, and by Little's law a single busy
priority tops out at 5 / provider latency: about 66 msg/s against the mock provider (75 ms on average)
and 25 msg/s against a 200 ms provider, below the 100 msg/s each channel may send.

Holding the message buys no recovery. Once the conditional `queued → sending` transition has
committed, a redelivery of the message is skipped because the notification is no longer `queued`; if
the worker dies during or after the provider call, the reaper's 60 s lease recovers the delivery either
way. The only thing the unacknowledged message provides is dead-lettering by `nack(requeue=false)`.

## Decision

- The worker acknowledges the message right after the `queued → sending` transition commits, then
  calls the provider and records the outcome.
- A per-channel pool of senders (default 50, `WORKER_CONCURRENCY`) bounds the sends in flight. The
  dispatcher takes a free sender before the rate-limit token, so a send starts as soon as its token is
  granted.
- A notification that fails for good (a permanent answer or its last attempt) is recorded as
  `permanently_failed` first, then published to its channel's dead-letter exchange with its failure
  reason and attempt count as headers. A failed publish is logged; PostgreSQL remains the source of
  truth (ADR 0009).
- If PostgreSQL cannot be reached after the provider call, the outcome write is retried with backoff for
  up to 30 s, well inside the reaper's lease; after that the reaper takes over.
- On shutdown the worker stops picking messages, lets the sends in flight finish and record their
  outcome, then closes its broker channels; messages picked but not sent go back to their queues.

## Alternatives considered

- **Acknowledge after the outcome, prefetch 50.** Keeps 0007 and dead-lettering by `nack`, but each
  worker holds up to 150 unacknowledged messages per channel, other replicas sit idle on small backlogs,
  and throughput is still prefetch / latency (about 166 msg/s at 300 ms).
- **Acknowledge after the outcome, prefetch 5.** No change, but a busy priority cannot reach the rate
  limit.

## Consequences

- Throughput no longer depends on provider latency; the prefetch only bounds what the priority picker
  sees.
- The dead-letter queue misses a permanently failed delivery if its publish fails.
- `WORKER_CONCURRENCY` is a new setting to size against provider latency: rate × latency sends are in
  flight at full speed (100 × 0.3 s = 30).
