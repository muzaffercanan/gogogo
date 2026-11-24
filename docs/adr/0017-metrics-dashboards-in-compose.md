# 0017. Prometheus and Grafana dashboards in Docker Compose

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

The api, relay, worker and mock provider serve Prometheus metrics on `/metrics`. Nothing in the
Compose stack collects them, so they can only be read one scrape at a time with curl. Load tests and
demos need them over time: send rate per channel, queue depth, latencies and breaker state.

## Decision

- The default Compose stack runs Prometheus (`prom/prometheus`) and Grafana (`grafana/grafana`),
  next to Jaeger. Prometheus is on host port 9091 (9090 is the mock provider's), Grafana on
  `localhost:3000`.
- Prometheus scrapes the four processes every 5 s (`deploy/prometheus/prometheus.yml`). It keeps its
  data on a tmpfs: metrics live as long as the container and nothing accumulates on disk.
- Grafana is provisioned from `deploy/grafana`: the Prometheus datasource and one dashboard,
  `Notifications`, which is also the home page. Its panels cover delivery (sent, failed, retries,
  end-to-end, provider and rate-limit latency), queues (work and dead-letter queues, outbox backlog,
  publish rate), health (circuit breakers, rate limiter, errors and reaper recoveries) and the API
  (requests and latency). Provisioned dashboards cannot be saved from the UI; the JSON in the repository
  is their source.
- Grafana accepts anonymous users as admins and shows no login form. Its port is bound to localhost
  only, and its usage reporting and update checks are off.
- Grafana has no Jaeger datasource: traces stay in the Jaeger UI, reached from a log record's
  `trace_id`.

## Alternatives considered

- **A Compose profile** (`--profile metrics`): a leaner default stack, but the dashboards take a flag to
  know about. Unlike Jaeger, Prometheus pulls, so turning it off would log no errors; the choice was
  made for a stack that shows everything after one command.
- **No dashboard, or Prometheus alone:** nothing to maintain, but every look starts from an empty query.
- **The default 15 s scrape interval:** standard, but a short burst becomes one or two points.
- **A named volume for Prometheus:** history across restarts, which a development stack does not need.
- **Login, or an anonymous viewer with an admin login:** safer if the port were exposed, but an extra
  step for every fresh stack; binding to localhost covers the local case.
- **A Jaeger datasource in Grafana:** without exemplars linking metrics to traces it would duplicate
  the Jaeger UI.

## Consequences

- `docker compose up` pulls and runs two more images (about 594 MB compressed, Grafana 484 MB of it).
  Measured at rest: Grafana uses about 310 MB of memory, Prometheus about 30 MB with the stack's
  580 series.
- Metric history is lost whenever the Prometheus container is recreated.
- Renaming a metric means updating `deploy/grafana/dashboards/notifications.json` by hand. Grafana
  picks up a changed file only when it restarts.
- Anyone who can reach `localhost:3000` can change Grafana's settings; nothing there is persisted.
