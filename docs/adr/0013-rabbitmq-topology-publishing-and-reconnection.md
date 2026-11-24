# 0013. RabbitMQ topology, publishing and reconnection

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

ADRs 0001, 0002 and 0009 fix what RabbitMQ carries: three work queues per channel, one dead-letter queue
per channel, and a relay that publishes with confirms inside its outbox transaction. This record fixes
how: the queue type, the routing, how confirms are awaited, and what happens when the connection drops.
PostgreSQL stays the source of truth, but a message the broker loses after confirming it leaves its
notification `queued`, so broker durability still matters.

## Decision

- **Topology.** One durable direct exchange, `notifications`, whose routing key is the work queue name
  `<channel>.<priority>`. Nine work queues and three dead-letter queues `<channel>.dlq`, all durable
  **quorum** queues. Each work queue dead-letters to the direct exchange `notifications.dlx` with routing
  key `<channel>`, which is bound to `<channel>.dlq`.
- **Declaration.** The relay and the worker declare the whole topology on every connect. Declaring is
  idempotent; a queue that exists with other arguments fails the declaration (`PRECONDITION_FAILED`) and
  the process stays unready.
- **Publishing.** Messages are persistent and published with `mandatory`. A batch is published in full,
  then every confirm is awaited. A nack, a return (unroutable message) or a timeout fails the whole batch:
  the relay rolls back and publishes the rows again, and the worker's conditional update drops duplicates.
- **While disconnected,** publishing fails at once with `ErrNotConnected`. The relay holds no transaction
  open during an outage; the outbox absorbs it.
- **Reconnection.** A supervisor owns the connection. It dials at startup and again after every
  `NotifyClose`, with exponential backoff and full jitter, without giving up, and redeclares the topology
  each time. amqp091-go's built-in recovery (`Config.Recovery`, experimental since v1.12.0) is not used.

## Alternatives considered

- **Classic queues.** Lighter, and as durable on a single node. No replication, no poison-message limit,
  and switching later means recreating every queue.
- **Default exchange, or one exchange per channel.** Fewer or more objects; neither gains anything here.
- **Confirming one message at a time.** Simple, but one round trip per message during a burst.
- **Asynchronous confirm tracking.** Highest throughput, but it does not fit a relay that claims,
  publishes and deletes in one transaction.
- **Waiting for the connection inside publish.** Holds the relay's transaction and row locks for as long
  as the outage lasts.
- **amqp091-go's recovery.** Restores channels and consumers by itself, and resolves pending confirms as
  nacks. But the API is experimental, it gives up after a fixed number of attempts (five by default), and
  it does not retry the initial dial, so an outer loop is needed anyway.

## Consequences

- Quorum queues cost more memory and disk than classic queues and replicate nothing on the single
  compose node; moving to a cluster needs no code change.
- RabbitMQ 4 dead-letters a quorum queue message after 20 redeliveries by default, so a message that
  keeps crashing workers ends up in the DLQ while its notification stays `queued`. Dead-lettering is
  at-most-once by default.
- One failed message makes the relay publish its whole batch again.
- A process starts and serves `/health/live` while the broker is down; readiness follows the connection.
- The reconnect logic is ours to test and maintain.
