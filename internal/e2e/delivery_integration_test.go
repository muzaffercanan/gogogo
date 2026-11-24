//go:build integration

package e2e_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNotificationsGoFromTheAPIToTheProvider(t *testing.T) {
	s := newSystem(t, nil)
	s.startWorker(workerConfig{})

	single := s.create(create{Channel: "sms", Priority: "high", Recipient: "+905550000001", Content: "your code is 1234"},
		map[string]string{"Idempotency-Key": uuid.NewString()})
	if single.Status != "pending" || single.Attempts != 0 {
		t.Errorf("created notification is %s after %d attempts, want pending after 0", single.Status, single.Attempts)
	}

	var batch struct {
		BatchID  string `json:"batch_id"`
		Accepted int    `json:"accepted"`
		Items    []struct {
			NotificationID string `json:"notification_id"`
		} `json:"items"`
	}
	items := []create{
		{Channel: "email", Priority: "normal", Recipient: "someone@example.com", Content: "welcome"},
		{Channel: "push", Priority: "low", Recipient: "device-token-1", Content: "you have a message"},
	}
	if code := s.request(http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": items}, nil, &batch); code != http.StatusAccepted || batch.Accepted != len(items) {
		t.Fatalf("POST /v1/notifications/batch answered %d accepting %d, want 202 accepting %d", code, batch.Accepted, len(items))
	}

	want := map[string]create{single.ID: {Channel: "sms", Recipient: "+905550000001", Content: "your code is 1234"}}
	for i, it := range batch.Items {
		want[it.NotificationID] = items[i]
	}
	for id, c := range want {
		n := s.waitForStatus(id, "sent", 15*time.Second)
		if n.Attempts != 1 || n.LastError != nil || n.SentAt == nil {
			t.Errorf("sent notification %s: attempts %d, last error %v, sent at %v; want 1 attempt, no error and a send time", id, n.Attempts, n.LastError, n.SentAt)
		}
		if want := "msg-" + id + "-1"; n.ProviderMessageID == nil || *n.ProviderMessageID != want {
			t.Errorf("notification %s has provider message id %v, want %q", id, n.ProviderMessageID, want)
		}
		reqs := s.provider.received(id)
		if len(reqs) != 1 {
			t.Errorf("the provider got %d requests for %s, want 1", len(reqs), id)
			continue
		}
		if r := reqs[0]; r.channel != c.Channel || r.to != c.Recipient || r.content != c.Content {
			t.Errorf("the provider got %s to %s: %q, want %s to %s: %q", r.channel, r.to, r.content, c.Channel, c.Recipient, c.Content)
		}
	}

	var got struct {
		StatusCounts map[string]int `json:"status_counts"`
	}
	if code := s.request(http.MethodGet, "/v1/batches/"+batch.BatchID, nil, nil, &got); code != http.StatusOK || got.StatusCounts["sent"] != len(items) {
		t.Errorf("GET /v1/batches/%s answered %d with status counts %v, want 200 with %d sent", batch.BatchID, code, got.StatusCounts, len(items))
	}
}
