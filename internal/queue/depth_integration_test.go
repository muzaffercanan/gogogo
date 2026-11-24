//go:build integration

package queue_test

import (
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// depths returns the queue_depth series of reg by "channel/queue", and the error counter.
func depths(t *testing.T, reg *prometheus.Registry) (map[string]float64, float64) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	errors := -1.0
	for _, f := range families {
		switch f.GetName() {
		case "queue_depth":
			for _, m := range f.GetMetric() {
				var channel, q string
				for _, l := range m.GetLabel() {
					switch l.GetName() {
					case "channel":
						channel = l.GetValue()
					case "queue":
						q = l.GetValue()
					}
				}
				out[channel+"/"+q] = m.GetGauge().GetValue()
			}
		case "queue_depth_query_errors_total":
			errors = f.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return out, errors
}

func TestDepthCollectorReportsEveryWorkAndDeadLetterQueue(t *testing.T) {
	url := mq.NewVHost(t)
	ch := topology(t, url)
	s, _, _ := supervise(t, url)
	waitFor(t, "the supervisor is ready", ready(s))
	reg := prometheus.NewRegistry()
	reg.MustRegister(queue.NewDepthCollector(s, slog.New(slog.NewTextHandler(t.Output(), nil))))

	for _, id := range []string{"a", "b", "c"} {
		send(t, ch, domain.ChannelSMS, domain.PriorityHigh, id, "")
	}
	if err := ch.PublishWithContext(t.Context(), queue.DeadLetterExchange, "email", false, false, amqp.Publishing{Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}

	var got map[string]float64
	var errors float64
	waitFor(t, "the depths show the published messages", func() bool {
		got, errors = depths(t, reg)
		return got["sms/high"] == 3 && got["email/dlq"] == 1
	})
	if len(got) != 12 {
		t.Errorf("queue_depth has %d series (%v), want 12: four queues for each of three channels", len(got), got)
	}
	for key, v := range got {
		if key != "sms/high" && key != "email/dlq" && v != 0 {
			t.Errorf("queue_depth{%s} = %v, want 0", key, v)
		}
	}
	if errors != 0 {
		t.Errorf("queue_depth_query_errors_total = %v, want 0", errors)
	}
}

func TestDepthCollectorLeavesOutDepthsWithoutABroker(t *testing.T) {
	_, url := newProxy(t, mq.NewVHost(t), false)
	s, _, _ := supervise(t, url)
	reg := prometheus.NewRegistry()
	reg.MustRegister(queue.NewDepthCollector(s, slog.New(slog.NewTextHandler(t.Output(), nil))))

	for scrape := 1; scrape <= 2; scrape++ {
		got, errors := depths(t, reg) // Gather fails the test if the scrape as a whole failed
		if len(got) != 0 {
			t.Errorf("scrape %d: queue_depth = %v without a broker, want no series", scrape, got)
		}
		if errors != float64(scrape) {
			t.Errorf("scrape %d: queue_depth_query_errors_total = %v, want %d", scrape, errors, scrape)
		}
	}
}
