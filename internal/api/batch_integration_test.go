//go:build integration

package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// batchBody returns a batch request body holding items, each a JSON object.
func batchBody(items ...string) string {
	return `{"notifications":[` + strings.Join(items, ",") + `]}`
}

func smsItem(recipient string) string {
	return `{"channel":"sms","recipient":"` + recipient + `","content":"Flash sale starts now!","priority":"high"}`
}

func repeatItems(item string, n int) []string {
	items := make([]string, n)
	for i := range items {
		items[i] = item
	}
	return items
}

func TestCreateBatch(t *testing.T) {
	a := newAPI(t)
	body := batchBody(
		smsItem("+905551234567"),
		`{"channel":"email","recipient":"not-an-email","content":"Flash sale starts now!"}`,
		smsItem("+905551234568"),
	)

	resp := a.do(t, http.MethodPost, "/v1/notifications/batch", body, "X-Correlation-ID", "corr-batch")

	if resp.status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", resp.status, resp.raw)
	}
	batchID := fmt.Sprint(resp.body["batch_id"])
	if resp.header.Get("Location") != "/v1/batches/"+batchID || resp.header.Get("Idempotent-Replayed") != "false" {
		t.Errorf("headers = Location %q, Idempotent-Replayed %q", resp.header.Get("Location"), resp.header.Get("Idempotent-Replayed"))
	}
	if resp.body["total"] != float64(3) || resp.body["accepted"] != float64(2) || resp.body["rejected"] != float64(1) {
		t.Errorf("counts = %v/%v/%v, want 3 total, 2 accepted, 1 rejected", resp.body["total"], resp.body["accepted"], resp.body["rejected"])
	}

	items, _ := resp.body["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("%d item results, want 3", len(items))
	}
	wantRecipients := map[int]string{0: "+905551234567", 2: "+905551234568"}
	for i, raw := range items {
		item := raw.(map[string]any)
		if item["index"] != float64(i) {
			t.Errorf("item %d has index %v", i, item["index"])
		}
		if i == 1 {
			if item["result"] != "rejected" || fmt.Sprint(fieldErrors(item)) != "[recipient invalid_format]" || item["notification_id"] != nil {
				t.Errorf("item 1 = %v, want rejected with a recipient error", item)
			}
			continue
		}
		id, err := uuid.Parse(fmt.Sprint(item["notification_id"]))
		if item["result"] != "accepted" || err != nil || item["errors"] != nil {
			t.Fatalf("item %d = %v, want accepted with a notification ID", i, item)
		}
		var recipient, batch, corr string
		if err := a.db.QueryRow(t.Context(), `SELECT recipient, batch_id::text, correlation_id FROM notifications WHERE id = $1`, id).
			Scan(&recipient, &batch, &corr); err != nil {
			t.Fatalf("reading notification of item %d: %v", i, err)
		}
		if recipient != wantRecipients[i] || batch != batchID || corr != "corr-batch" {
			t.Errorf("notification of item %d = %s, batch %s, correlation %s; want %s in batch %s with corr-batch",
				i, recipient, batch, corr, wantRecipients[i], batchID)
		}
	}
	if n, o := a.count(t, "notifications"), a.count(t, "outbox"); n != 2 || o != 2 {
		t.Errorf("%d notifications and %d outbox rows, want 2 each", n, o)
	}
}

func TestCreateBatchSizes(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantErrors   []string
		wantAccepted float64
	}{
		{"every item rejected", batchBody(smsItem("123")), http.StatusAccepted, nil, 0},
		{"1000 items", batchBody(repeatItems(smsItem("+905551234567"), 1000)...), http.StatusAccepted, nil, 1000},
		{"1001 items", batchBody(repeatItems(smsItem("+905551234567"), 1001)...), http.StatusBadRequest, []string{"notifications too_many_items"}, 0},
		{"no items", batchBody(), http.StatusBadRequest, []string{"notifications too_short"}, 0},
		{"no notifications field", `{}`, http.StatusBadRequest, []string{"notifications too_short"}, 0},
		{
			// 1000 push items of 4 KB: over the 1 MiB limit of single requests, under the batch limit.
			"4 MB batch",
			batchBody(repeatItems(`{"channel":"push","recipient":"tok","content":"`+strings.Repeat("a", 4096)+`"}`, 1000)...),
			http.StatusAccepted, nil, 1000,
		},
		{"body over 10 MiB", `{"notifications":[{"content":"` + strings.Repeat("a", 10<<20) + `"}]}`, http.StatusRequestEntityTooLarge, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t)
			resp := a.do(t, http.MethodPost, "/v1/notifications/batch", tt.body)

			if resp.status != tt.wantStatus || fmt.Sprint(fieldErrors(resp.body)) != fmt.Sprint(tt.wantErrors) {
				t.Fatalf("response = %d %v, want %d %v", resp.status, fieldErrors(resp.body), tt.wantStatus, tt.wantErrors)
			}
			if tt.wantStatus == http.StatusAccepted && resp.body["accepted"] != tt.wantAccepted {
				t.Errorf("accepted = %v, want %v", resp.body["accepted"], tt.wantAccepted)
			}
			if n := a.count(t, "notifications"); n != int(tt.wantAccepted) {
				t.Errorf("%d notifications stored, want %v", n, tt.wantAccepted)
			}
		})
	}
}

func TestCreateBatchIdempotency(t *testing.T) {
	scheduled := `{"channel":"sms","recipient":"+905551234567","content":"Later","scheduled_at":"2026-11-25T09:00:00Z"}`
	body := batchBody(scheduled, smsItem("123"))

	t.Run("replay returns the original result", func(t *testing.T) {
		a := newAPI(t)
		a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
		first := a.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "batch-key")

		// By now the scheduled item would be rejected; the replay still reports it as accepted.
		a.setNow(time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC))
		again := a.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "batch-key")

		if again.status != http.StatusAccepted || again.header.Get("Idempotent-Replayed") != "true" || string(again.raw) != string(first.raw) {
			t.Errorf("replay = %d, replayed %q, body %s; want 202, true and the first body %s",
				again.status, again.header.Get("Idempotent-Replayed"), again.raw, first.raw)
		}
		if n := a.count(t, "batches"); n != 1 {
			t.Errorf("%d batches, want 1", n)
		}
	})

	t.Run("different batch is refused", func(t *testing.T) {
		a := newAPI(t)
		a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
		a.do(t, http.MethodPost, "/v1/notifications/batch", body, "Idempotency-Key", "batch-key")

		again := a.do(t, http.MethodPost, "/v1/notifications/batch", batchBody(smsItem("123"), scheduled), "Idempotency-Key", "batch-key")

		if again.status != http.StatusUnprocessableEntity || again.body["type"] != "/problems/idempotency-key-reused" {
			t.Errorf("reordered batch with the same key = %d %s, want 422", again.status, again.raw)
		}
	})
}
