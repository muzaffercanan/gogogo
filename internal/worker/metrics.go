package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// Metrics counts what the worker does, and tracks for readiness whether each channel can reserve send
// slots. Create it with NewMetrics.
type Metrics struct {
	limiterDown   *prometheus.GaugeVec
	limiterErrors *prometheus.CounterVec

	stopped map[domain.Channel]*atomic.Bool // fixed at construction: one per channel
}

// NewMetrics registers the worker's metrics with reg:
//   - ratelimit_unavailable{channel}
//   - ratelimit_errors_total{channel}
//
// Every channel's series starts at zero.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		limiterDown: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ratelimit_unavailable",
			Help: "1 while the channel sends nothing because the rate limiter cannot be reached (ADR 0004), else 0.",
		}, []string{"channel"}),
		limiterErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_errors_total",
			Help: "Send slot reservations that failed because the rate limiter could not be reached.",
		}, []string{"channel"}),
		stopped: map[domain.Channel]*atomic.Bool{},
	}
	reg.MustRegister(m.limiterDown, m.limiterErrors)
	for _, c := range queue.Channels() {
		m.limiterDown.WithLabelValues(string(c))
		m.limiterErrors.WithLabelValues(string(c))
		m.stopped[c] = &atomic.Bool{}
	}
	return m
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
