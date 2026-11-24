package queue

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// depthTimeout bounds reading the queue depths on a scrape. The broker calls are not cancellable, so a
// slow broker leaves its scrape without depths rather than holding it.
const depthTimeout = 2 * time.Second

// dlqLabel names a channel's dead-letter queue in the queue label of queue_depth.
const dlqLabel = "dlq"

// DepthCollector reports queue_depth{channel,queue} by asking the broker, each time Prometheus scrapes,
// how many messages wait in every work queue (queue=high|normal|low) and dead-letter queue (queue=dlq).
// Create it with NewDepthCollector and register it with the process registry.
type DepthCollector struct {
	sup     *Supervisor
	log     *slog.Logger
	timeout time.Duration

	depth  *prometheus.Desc
	errors prometheus.Counter
}

// NewDepthCollector returns a collector that reads the queue depths over a channel of sup's connection.
//
// If they cannot be read, the scrape leaves out queue_depth, increments queue_depth_query_errors_total
// and logs a warning, so the other metrics are still served.
func NewDepthCollector(sup *Supervisor, log *slog.Logger) *DepthCollector {
	return &DepthCollector{
		sup:     sup,
		log:     log,
		timeout: depthTimeout,
		depth: prometheus.NewDesc("queue_depth",
			"Messages waiting in each work queue (high, normal, low) and dead-letter queue (dlq), read from the broker at scrape time; messages a consumer holds unacknowledged are not counted. Every worker reports the same queues, so aggregate with max, not sum.",
			[]string{"channel", "queue"}, nil),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "queue_depth_query_errors_total",
			Help: "Scrapes that left out queue_depth because the broker could not be asked.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *DepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	c.errors.Describe(ch)
}

type queueDepth struct {
	channel, queue string
	messages       int
}

// Collect implements prometheus.Collector.
func (c *DepthCollector) Collect(ch chan<- prometheus.Metric) {
	type result struct {
		depths []queueDepth
		err    error
	}
	done := make(chan result, 1)
	go func() {
		depths, err := c.read()
		done <- result{depths, err}
	}()

	var r result
	select {
	case r = <-done:
	case <-time.After(c.timeout):
		r.err = fmt.Errorf("the broker did not answer within %v", c.timeout)
	}
	if r.err != nil {
		c.errors.Inc()
		c.log.Warn("reading queue depths for metrics", "error", r.err)
	} else {
		for _, d := range r.depths {
			ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, float64(d.messages), d.channel, d.queue)
		}
	}
	c.errors.Collect(ch)
}

// read asks the broker for the depth of every work and dead-letter queue with passive declares on one
// channel.
func (c *DepthCollector) read() ([]queueDepth, error) {
	amqpCh, err := c.sup.Channel()
	if err != nil {
		return nil, err
	}
	defer func() { _ = amqpCh.Close() }()

	var depths []queueDepth
	for _, channel := range Channels() {
		names := map[string]string{dlqLabel: DeadLetterQueueName(channel)}
		labels := []string{dlqLabel}
		for _, p := range Priorities() {
			names[string(p)] = QueueName(channel, p)
			labels = append(labels, string(p))
		}
		for _, label := range labels {
			q, err := amqpCh.QueueDeclarePassive(names[label], true, false, false, false, nil)
			if err != nil {
				return nil, fmt.Errorf("inspecting %s: %w", names[label], err)
			}
			depths = append(depths, queueDepth{channel: string(channel), queue: label, messages: q.Messages})
		}
	}
	return depths, nil
}
