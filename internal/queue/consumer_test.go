package queue_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

func TestNewConsumerRejectsUnknownChannelsAndPrefetchOutOfRange(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sup := queue.NewSupervisor("amqp://unused", "test", log)

	tests := []struct {
		name     string
		channel  domain.Channel
		prefetch int
		wantErr  bool
	}{
		{name: "sms, prefetch 5", channel: domain.ChannelSMS, prefetch: 5},
		{name: "push, smallest prefetch", channel: domain.ChannelPush, prefetch: 1},
		{name: "email, largest prefetch", channel: domain.ChannelEmail, prefetch: 65535},
		{name: "unknown channel", channel: "fax", prefetch: 5, wantErr: true},
		{name: "zero prefetch means unlimited to the broker", channel: domain.ChannelSMS, prefetch: 0, wantErr: true},
		{name: "negative prefetch", channel: domain.ChannelSMS, prefetch: -1, wantErr: true},
		{name: "prefetch above the AMQP limit", channel: domain.ChannelSMS, prefetch: 65536, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := queue.NewConsumer(sup, tt.channel, tt.prefetch, log)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewConsumer succeeded, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			for _, p := range queue.Priorities() {
				if got := cap(c.Deliveries(p)); got != tt.prefetch {
					t.Errorf("buffer of %s holds %d, want the prefetch %d", p, got, tt.prefetch)
				}
			}
		})
	}
}

func TestDeliveryNotReceivedFromABrokerFailsToSettle(t *testing.T) {
	d := queue.Delivery{NotificationID: "n1", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}
	if err := d.Ack(); err == nil {
		t.Error("Ack succeeded, want an error")
	}
	if err := d.Reject(); err == nil {
		t.Error("Reject succeeded, want an error")
	}
}
