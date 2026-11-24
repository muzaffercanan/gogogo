package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker/v2"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/provider"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// Reasons a retry is scheduled, for retries_total.
const (
	retryServerError = "5xx"
	retryThrottled   = "429"
	retryTimeout408  = "408"
	retryTimeout     = "timeout"
	retryConnection  = "connection"
	retryOther       = "other"
)

// Reasons a delivery ends without a send, for deliveries_skipped_total.
const (
	skipNotQueued      = "not_queued"      // cancelled, sent already (a duplicate) or unknown
	skipInvalidMessage = "invalid_message" // the message names no notification
	skipNotStarted     = "not_started"     // PostgreSQL unreachable, or the worker stopping, before sending
)

// e2eBuckets span a send right away (tens of milliseconds) to the retry backoff cap and beyond.
var e2eBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900}

// Metrics counts what the worker does, and tracks for readiness whether each channel can reserve send
// slots. Create it with NewMetrics.
type Metrics struct {
	sent            *prometheus.CounterVec
	failed          *prometheus.CounterVec
	retries         *prometheus.CounterVec
	skipped         *prometheus.CounterVec
	malformed       *prometheus.CounterVec
	recordFailures  *prometheus.CounterVec
	providerLatency *prometheus.HistogramVec
	e2eLatency      *prometheus.HistogramVec
	ratelimitWait   *prometheus.HistogramVec
	limiterDown     *prometheus.GaugeVec
	limiterErrors   *prometheus.CounterVec
	breaker         *prometheus.GaugeVec

	stopped map[domain.Channel]*atomic.Bool // fixed at construction: one per channel
}

// NewMetrics registers the worker's metrics with reg:
//   - notifications_sent_total{channel}
//   - notifications_failed_total{channel, final}
//   - retries_total{channel, reason}
//   - deliveries_skipped_total{channel, reason}
//   - provider_malformed_acceptances_total{channel}
//   - delivery_outcome_record_failures_total{channel}
//   - provider_request_duration_seconds{channel}
//   - notification_e2e_latency_seconds{channel, priority}
//   - ratelimit_wait_seconds{channel}
//   - ratelimit_unavailable{channel}
//   - ratelimit_errors_total{channel}
//   - circuit_breaker_state{channel}
//
// Every label combination starts at zero, so rates are defined from the first scrape.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Notifications the provider accepted, recorded as sent.",
		}, []string{"channel"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total",
			Help: "Delivery attempts that failed: final=false when a retry was scheduled, final=true when the notification became permanently_failed.",
		}, []string{"channel", "final"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retries_total",
			Help: "Retries scheduled after a retryable provider failure, by its kind: 5xx, 429, 408, timeout, connection or other.",
		}, []string{"channel", "reason"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "deliveries_skipped_total",
			Help: "Deliveries that sent nothing: not_queued (cancelled or a duplicate message), invalid_message (dead-lettered) or not_started (back to the queue).",
		}, []string{"channel", "reason"}),
		malformed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "provider_malformed_acceptances_total",
			Help: "2xx provider answers without a readable message id, counted as sent (ADR 0014).",
		}, []string{"channel"}),
		recordFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "delivery_outcome_record_failures_total",
			Help: "Provider calls whose outcome could not be written; the reaper retries those deliveries.",
		}, []string{"channel"}),
		providerLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "provider_request_duration_seconds",
			Help:    "Duration of provider calls, whatever their outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel"}),
		e2eLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_e2e_latency_seconds",
			Help:    "Time from when a notification was due (created, or its scheduled time if later) to being sent, retries included, by the database clock.",
			Buckets: e2eBuckets,
		}, []string{"channel", "priority"}),
		ratelimitWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ratelimit_wait_seconds",
			Help:    "Time a send waited for its rate-limit slot.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1 ms to about 2 s
		}, []string{"channel"}),
		limiterDown: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ratelimit_unavailable",
			Help: "1 while the channel sends nothing because the rate limiter cannot be reached (ADR 0004), else 0.",
		}, []string{"channel"}),
		limiterErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_errors_total",
			Help: "Send slot reservations that failed because the rate limiter could not be reached.",
		}, []string{"channel"}),
		breaker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "State of the channel's circuit breaker: 0 closed, 1 half-open (one probe goes through), 2 open (sending paused).",
		}, []string{"channel"}),
		stopped: map[domain.Channel]*atomic.Bool{},
	}
	reg.MustRegister(m.sent, m.failed, m.retries, m.skipped, m.malformed, m.recordFailures,
		m.providerLatency, m.e2eLatency, m.ratelimitWait, m.limiterDown, m.limiterErrors, m.breaker)
	for _, c := range queue.Channels() {
		ch := string(c)
		m.sent.WithLabelValues(ch)
		for _, final := range []string{"false", "true"} {
			m.failed.WithLabelValues(ch, final)
		}
		for _, r := range []string{retryServerError, retryThrottled, retryTimeout408, retryTimeout, retryConnection, retryOther} {
			m.retries.WithLabelValues(ch, r)
		}
		for _, r := range []string{skipNotQueued, skipInvalidMessage, skipNotStarted} {
			m.skipped.WithLabelValues(ch, r)
		}
		m.malformed.WithLabelValues(ch)
		m.recordFailures.WithLabelValues(ch)
		m.providerLatency.WithLabelValues(ch)
		for _, p := range queue.Priorities() {
			m.e2eLatency.WithLabelValues(ch, string(p))
		}
		m.ratelimitWait.WithLabelValues(ch)
		m.limiterDown.WithLabelValues(ch)
		m.limiterErrors.WithLabelValues(ch)
		m.breaker.WithLabelValues(ch)
		m.stopped[c] = &atomic.Bool{}
	}
	return m
}

// delivered records a notification recorded as sent: n is the sent row, with the database's times.
func (m *Metrics) delivered(n domain.Notification, malformed bool) {
	m.sent.WithLabelValues(string(n.Channel)).Inc()
	if malformed {
		m.malformed.WithLabelValues(string(n.Channel)).Inc()
	}
	if n.SentAt != nil {
		due := n.CreatedAt
		if n.ScheduledAt != nil && n.ScheduledAt.After(due) {
			due = *n.ScheduledAt
		}
		m.e2eLatency.WithLabelValues(string(n.Channel), string(n.Priority)).Observe(max(n.SentAt.Sub(due), 0).Seconds())
	}
}

// retried records a retry scheduled after the retryable failure pe.
func (m *Metrics) retried(c domain.Channel, pe *provider.Error) {
	m.failed.WithLabelValues(string(c), "false").Inc()
	m.retries.WithLabelValues(string(c), retryReason(pe)).Inc()
}

// failedForGood records a notification that became permanently_failed.
func (m *Metrics) failedForGood(c domain.Channel) {
	m.failed.WithLabelValues(string(c), "true").Inc()
}

// skippedDelivery records a delivery that sent nothing, for reason.
func (m *Metrics) skippedDelivery(c domain.Channel, reason string) {
	m.skipped.WithLabelValues(string(c), reason).Inc()
}

// outcomeNotRecorded records a provider call whose outcome could not be written.
func (m *Metrics) outcomeNotRecorded(c domain.Channel) {
	m.recordFailures.WithLabelValues(string(c)).Inc()
}

// providerCall records how long a provider call of channel c took.
func (m *Metrics) providerCall(c domain.Channel, took time.Duration) {
	m.providerLatency.WithLabelValues(string(c)).Observe(took.Seconds())
}

// waitedForSlot records how long a send of channel c waited for its rate-limit slot.
func (m *Metrics) waitedForSlot(c domain.Channel, waited time.Duration) {
	m.ratelimitWait.WithLabelValues(string(c)).Observe(waited.Seconds())
}

// retryReason names the kind of a retryable failure for retries_total.
func retryReason(pe *provider.Error) string {
	switch {
	case pe.StatusCode >= http.StatusInternalServerError:
		return retryServerError
	case pe.StatusCode == http.StatusTooManyRequests:
		return retryThrottled
	case pe.StatusCode == http.StatusRequestTimeout:
		return retryTimeout408
	case pe.StatusCode == 0 && errors.Is(pe, context.DeadlineExceeded):
		return retryTimeout
	case pe.StatusCode == 0 && errors.Unwrap(pe) != nil:
		return retryConnection
	default:
		return retryOther
	}
}

// limiterFailed records a reservation of channel c that could not reach the limiter: the channel's
// sends stop until limiterRecovered.
func (m *Metrics) limiterFailed(c domain.Channel) {
	m.limiterErrors.WithLabelValues(string(c)).Inc()
	m.limiterDown.WithLabelValues(string(c)).Set(1)
	if s, ok := m.stopped[c]; ok {
		s.Store(true)
	}
}

// limiterRecovered records a successful reservation of channel c.
func (m *Metrics) limiterRecovered(c domain.Channel) {
	if s, ok := m.stopped[c]; ok && s.Swap(false) {
		m.limiterDown.WithLabelValues(string(c)).Set(0)
	}
}

// breakerState records the state channel c's circuit breaker moved to.
func (m *Metrics) breakerState(c domain.Channel, s gobreaker.State) {
	m.breaker.WithLabelValues(string(c)).Set(float64(s)) // gobreaker numbers them closed 0, half-open 1, open 2
}

// CheckLimiter is a readiness check: it fails while a channel sends nothing because the rate limiter
// cannot be reached, even if Redis still answers a ping.
func (m *Metrics) CheckLimiter(context.Context) error {
	var down []string
	for c, s := range m.stopped {
		if s.Load() {
			down = append(down, string(c))
		}
	}
	if len(down) > 0 {
		slices.Sort(down)
		return fmt.Errorf("rate limiter unavailable; sending stopped for %s", strings.Join(down, ", "))
	}
	return nil
}
