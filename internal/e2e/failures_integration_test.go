//go:build integration

package e2e_test

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

func TestFailedSendsAreRetriedOrDeadLettered(t *testing.T) {
	s := newSystem(t, nil)
	s.startWorker(workerConfig{})

	const (
		flaky    = "+905550000010" // two server errors, then accepted
		refusing = "+905550000011" // refused for good
		broken   = "+905550000012" // a server error on every attempt
	)
	s.provider.script(flaky, answer{status: http.StatusInternalServerError}, answer{status: http.StatusServiceUnavailable})
	s.provider.script(refusing, answer{status: http.StatusBadRequest})
	fails := make([]answer, maxAttempts)
	for i := range fails {
		fails[i] = answer{status: http.StatusInternalServerError}
	}
	s.provider.script(broken, fails...)

	retried := s.create(create{Channel: "sms", Priority: "normal", Recipient: flaky, Content: "retried"}, nil)
	refused := s.create(create{Channel: "sms", Priority: "normal", Recipient: refusing, Content: "refused"}, nil)
	exhausted := s.create(create{Channel: "sms", Priority: "normal", Recipient: broken, Content: "exhausted"}, nil)

	t.Run("server errors are retried until the provider accepts", func(t *testing.T) {
		n := s.waitForStatus(retried.ID, "sent", 20*time.Second)
		if n.Attempts != 3 {
			t.Errorf("sent after %d attempts, want 3", n.Attempts)
		}
		if want := "msg-" + n.ID + "-3"; n.ProviderMessageID == nil || *n.ProviderMessageID != want {
			t.Errorf("provider message id %v, want %q (the accepting attempt's)", n.ProviderMessageID, want)
		}
		if got := len(s.provider.received(n.ID)); got != 3 {
			t.Errorf("the provider got %d requests, want 3 with the same Idempotency-Key", got)
		}
	})
	t.Run("a refusal fails the notification at once and dead-letters it", func(t *testing.T) {
		n := s.waitForStatus(refused.ID, "permanently_failed", 20*time.Second)
		if n.Attempts != 1 || !strings.Contains(deref(n.LastError), "400") {
			t.Errorf("failed after %d attempts with last error %q, want 1 attempt and the provider's 400", n.Attempts, deref(n.LastError))
		}
		if got := len(s.provider.received(n.ID)); got != 1 {
			t.Errorf("the provider got %d requests, want 1", got)
		}
	})
	t.Run("the last failed attempt fails the notification and dead-letters it", func(t *testing.T) {
		n := s.waitForStatus(exhausted.ID, "permanently_failed", 30*time.Second)
		if n.Attempts != maxAttempts || !strings.Contains(deref(n.LastError), "500") {
			t.Errorf("failed after %d attempts with last error %q, want %d attempts and the provider's 500", n.Attempts, deref(n.LastError), maxAttempts)
		}
		if got := len(s.provider.received(n.ID)); got != maxAttempts {
			t.Errorf("the provider got %d requests, want %d", got, maxAttempts)
		}
	})

	// Only the two failed notifications reach the dead-letter queue, each once.
	dead := s.deadLettered(domain.ChannelSMS, 2)
	want := []string{refused.ID, exhausted.ID}
	slices.Sort(dead)
	slices.Sort(want)
	if !slices.Equal(dead, want) {
		t.Errorf("%s holds %v, want %v", queue.DeadLetterQueueName(domain.ChannelSMS), dead, want)
	}
}
