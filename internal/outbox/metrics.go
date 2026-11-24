package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// backlogQueryTimeout bounds the outbox count that runs on every scrape. It stays well below
// Prometheus's default 10 s scrape timeout.
const backlogQueryTimeout = 2 * time.Second

// Metrics counts what the relay and the reaper do. Create it with NewMetrics.
type Metrics struct {
	published        *prometheus.CounterVec
	skipped          prometheus.Counter
	dispatchErrors   prometheus.Counter
	dispatchDuration prometheus.Histogram
	recovered        *prometheus.CounterVec
	reaperErrors     prometheus.Counter
}

// NewMetrics registers the relay and reaper metrics with reg:
//   - outbox_published_total{channel, priority}
//   - outbox_skipped_total
//   - outbox_dispatch_errors_total
//   - outbox_dispatch_duration_seconds
//   - reaper_recovered_total{channel, status}
//   - reaper_errors_total
//
// Every channel, priority and reaper outcome series starts at zero, so rates are defined from the first
// scrape.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_published_total",
			Help: "Notifications the relay published with broker confirms and committed as queued, by channel and priority.",
		}, []string{"channel", "priority"}),
		skipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_skipped_total",
			Help: "Claimed outbox rows deleted without publishing because the notification was no longer pending or retry_scheduled (for example cancelled).",
		}),
		dispatchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_dispatch_errors_total",
			Help: "Relay steps that failed and rolled back; their rows are retried on a later step.",
		}),
		dispatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "outbox_dispatch_duration_seconds",
			Help:    "Duration of successful relay steps that claimed at least one row: claim, queue, publish with confirms, delete and commit.",
			Buckets: prometheus.DefBuckets,
		}),
		recovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reaper_recovered_total",
			Help: "Deliveries whose sending lease expired, by channel and the status the reaper moved them to.",
		}, []string{"channel", "status"}),
		reaperErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "reaper_errors_total",
			Help: "Reaper steps that failed and rolled back.",
		}),
	}
	reg.MustRegister(m.published, m.skipped, m.dispatchErrors, m.dispatchDuration, m.recovered, m.reaperErrors)

	for _, ch := range queue.Channels() {
		for _, pr := range queue.Priorities() {
			m.published.WithLabelValues(string(ch), string(pr))
		}
		for _, st := range []domain.Status{domain.StatusRetryScheduled, domain.StatusPermanentlyFailed} {
			m.recovered.WithLabelValues(string(ch), string(st))
		}
	}
	return m
}

// dispatched records a committed relay step that claimed claimed rows and published items.
func (m *Metrics) dispatched(items []repository.OutboxItem, claimed int, took time.Duration) {
	for _, it := range items {
		m.published.WithLabelValues(string(it.Channel), string(it.Priority)).Inc()
	}
	m.skipped.Add(float64(claimed - len(items)))
	if claimed > 0 {
		m.dispatchDuration.Observe(took.Seconds())
	}
}

// reaped records the deliveries a committed reaper step recovered.
func (m *Metrics) reaped(deliveries []repository.ReapedDelivery) {
	for _, d := range deliveries {
		m.recovered.WithLabelValues(string(d.Channel), string(d.Status)).Inc()
	}
}

// BacklogCollector reports outbox_backlog{state="due"|"scheduled"} by counting the outbox each time
// Prometheus scrapes. Create it with NewBacklogCollector and register it with the process registry.
type BacklogCollector struct {
	store   *repository.Store
	log     *slog.Logger
	timeout time.Duration

	backlog *prometheus.Desc
	errors  prometheus.Counter
}

// NewBacklogCollector returns a collector that counts the outbox rows in store on every scrape.
//
// If the count fails, the scrape leaves out outbox_backlog, increments
// outbox_backlog_query_errors_total and logs a warning, so the other metrics are still served (a
// collector error would otherwise turn the whole /metrics response into a 500).
func NewBacklogCollector(store *repository.Store, log *slog.Logger) *BacklogCollector {
	return &BacklogCollector{
		store:   store,
		log:     log,
		timeout: backlogQueryTimeout,
		backlog: prometheus.NewDesc("outbox_backlog",
			"Outbox rows waiting for the relay, counted at scrape time: due (available_at has passed) or scheduled (available_at in the future). Every relay reports the same table, so aggregate with max, not sum.",
			[]string{"state"}, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_backlog_query_errors_total",
			Help: "Scrapes that left out outbox_backlog because counting the outbox failed.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *BacklogCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.backlog
	c.errors.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *BacklogCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	b, err := c.store.CountOutboxBacklog(ctx)
	if err != nil {
		c.errors.Inc()
		c.log.Warn("counting outbox backlog for metrics", "error", err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.backlog, prometheus.GaugeValue, float64(b.Due), "due")
		ch <- prometheus.MustNewConstMetric(c.backlog, prometheus.GaugeValue, float64(b.Scheduled), "scheduled")
	}
	c.errors.Collect(ch)
}
