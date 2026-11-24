package queue_test

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

// unconnected returns a publisher whose supervisor never runs, so any broker call would fail with
// ErrNotConnected.
func unconnected() *queue.Publisher {
	return queue.NewPublisher(queue.NewSupervisor("amqp://unused", "test", slog.New(slog.NewTextHandler(io.Discard, nil))))
}

func TestPublishAnEmptyBatchNeedsNoConnection(t *testing.T) {
	if err := unconnected().Publish(t.Context(), nil); err != nil {
		t.Errorf("Publish(nil) = %v, want nil", err)
	}
}

func TestPublishRejectsAMessageWithoutAQueue(t *testing.T) {
	tests := []struct {
		name string
		msg  queue.Message
	}{
		{"unknown channel", queue.Message{ID: "m1", Channel: "fax", Priority: domain.PriorityHigh}},
		{"unknown priority", queue.Message{ID: "m1", Channel: domain.ChannelSMS, Priority: "urgent"}},
		{"no priority", queue.Message{ID: "m1", Channel: domain.ChannelSMS}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok := queue.Message{ID: "m0", Channel: domain.ChannelEmail, Priority: domain.PriorityLow}

			err := unconnected().Publish(t.Context(), []queue.Message{ok, tt.msg})

			// Checked before any broker call, so the error is about the message, not the connection.
			if err == nil || errors.Is(err, queue.ErrNotConnected) || !strings.Contains(err.Error(), "message m1") {
				t.Errorf("Publish = %v, want an error about message m1 before connecting", err)
			}
		})
	}
}
