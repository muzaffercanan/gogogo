# gogogo

An event-driven notification service written in Go. It accepts notification requests over HTTP
and delivers them asynchronously over SMS, Email and Push channels, with per-channel rate
limiting, priorities, retries and real-time status tracking.

> **Work in progress.** Setup instructions, the architecture overview and API examples will be
> added here as the project takes shape.

## Load test

`make load` runs [`test/load/burst.js`](test/load/burst.js) with k6 against the running Compose stack.
Ten virtual users each send one batch of 1000 notifications at the same moment: 10 000 in all, a
third per channel, 2 in 10 high, 5 in 10 normal and 3 in 10 low priority. k6 reports how long each
batch took to be accepted. When every notification has been sent, k6 reads them back through the API
and prints the drain rate of each channel and when each priority was sent. These times come from
`created_at` and `sent_at`, both written by the database.

Results for the default settings (100 per second per channel, `WORKER_PREFETCH=20`, mock provider
50 ± 50 ms). The machine was Windows 11 with Docker Desktop (WSL 2, 12 CPUs, 11 GiB), and the whole
stack and k6 ran in the same VM. Seconds count from the first notification's `created_at`. Each
channel behaved the same within a run (within 0.1 s), so the table shows the mean of the three
channels.

| Run | Batch accepted in (median / max) | Drain rate per channel | Median send: high / normal / low | Last send |
|---|---|---|---|---|
| 1 | 0.83 s / 0.89 s | 98.1/s | 6.6 / 18.6 / 27.5 s | 34.1 s |
| 2 | 3.55 s / 3.61 s | 79.3/s | 22.1 / 31.2 / 37.3 s | 46.1 s |
| 3 | 0.84 s / 0.86 s | 100.1/s | 7.0 / 18.7 / 27.6 s | 34.2 s |
| 4 | 3.06 s / 3.15 s | 60.5/s | 39.6 / 43.7 / 46.4 s | 58.4 s |
| 5 | 1.08 s / 1.22 s | 99.6/s | 10.1 / 19.2 / 28.0 s | 34.8 s |
| 6 | 2.82 s / 2.90 s | 58.8/s | 39.2 / 45.1 / 48.4 s | 59.2 s |
| 7 | 0.83 s / 0.88 s | 100.0/s | 7.5 / 18.8 / 27.6 s | 34.3 s |

- **Ingest.** The API accepts a batch of 1000 in 0.8–1.1 s, even while nine more arrive at the same
  time: about 10 000 notifications per second into PostgreSQL. No request failed in any run.
- **Drain.** In runs 1, 3, 5 and 7 each channel sends at its limit: 3333 notifications in 33–34 s,
  which is 100 per second. In every run, every notification ended up `sent`.
- **Priority.** With all three priorities waiting, the weights 6:3:1 give high 60 sends a second, so
  its 667 per channel are done by about 12.5 s. Normal and low then share the channel 3:1. Low keeps
  its tenth throughout and is not starved.
- **Slow runs (2, 4, 6).** The same burst sometimes runs about three times slower everywhere. Batches take
  ~3 s to accept, each relay publish and commit takes 0.65–1.9 s instead of ~0.1 s, and the workers'
  sending transitions slow down with it. Nothing failed, but the backlog reached the queues late, so
  the priorities were sent closer together. The API and the relay share only PostgreSQL. All the slow
  steps are commits, and slow runs alternated with fast ones on the same data, so the likely cause is
  disk latency inside the Docker Desktop VM rather than the service. This was not measured on Linux.
- **Prefetch.** With the earlier default `WORKER_PREFETCH=5`, the same burst drained at 72/s per
  channel: 100/s while all three priorities had messages, then about 78/s and 30/s. A message stays
  unacknowledged until its `queued → sending` transition commits, so each priority sends at most
  prefetch / commit latency (ADR [0015](docs/adr/0015-ack-after-the-sending-transition.md)).

While it runs, the `Notifications` dashboard in Grafana (<http://localhost:3000>) shows the queue
depths, send rates and rate-limit waits. `docker compose run --rm -e BATCHES=20 k6` changes the
burst; `BATCH_SIZE` and `DRAIN_TIMEOUT_SECONDS` work the same way.
