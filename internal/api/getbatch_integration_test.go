//go:build integration

package api_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
)

// createBatch posts a batch of the given recipients (one invalid, "bad", is rejected) and returns its ID
// and the IDs of the accepted notifications in request order.
func createBatch(t *testing.T, a *testAPI, recipients ...string) (string, []string) {
	t.Helper()
	var items []string
	for _, r := range recipients {
		items = append(items, smsItem(r))
	}
	resp := a.do(t, http.MethodPost, "/v1/notifications/batch", batchBody(items...))
	if resp.status != http.StatusAccepted {
		t.Fatalf("creating batch: %d %s", resp.status, resp.raw)
	}
	var ids []string
	for _, it := range resp.body["items"].([]any) {
		if id := it.(map[string]any)["notification_id"]; id != nil {
			ids = append(ids, fmt.Sprint(id))
		}
	}
	return fmt.Sprint(resp.body["batch_id"]), ids
}

func TestGetBatch(t *testing.T) {
	a := newAPI(t)
	batchID, ids := createBatch(t, a, "+905550000001", "bad", "+905550000002", "+905550000003")
	if _, err := a.db.Exec(t.Context(), `UPDATE notifications SET status = 'sent' WHERE id = $1`, ids[1]); err != nil {
		t.Fatal(err)
	}

	resp := a.do(t, http.MethodGet, "/v1/batches/"+batchID, "")

	if resp.status != http.StatusOK {
		t.Fatalf("status = %d %s", resp.status, resp.raw)
	}
	if resp.body["id"] != batchID || resp.body["total"] != float64(4) || resp.body["accepted"] != float64(3) ||
		resp.body["rejected"] != float64(1) || resp.body["created_at"] == nil {
		t.Errorf("summary = %s", resp.raw)
	}
	wantCounts := map[string]any{
		"pending": float64(2), "queued": float64(0), "sending": float64(0), "sent": float64(1),
		"retry_scheduled": float64(0), "permanently_failed": float64(0), "cancelled": float64(0),
	}
	if fmt.Sprint(resp.body["status_counts"]) != fmt.Sprint(wantCounts) {
		t.Errorf("status_counts = %v, want %v", resp.body["status_counts"], wantCounts)
	}
	var got []string
	for _, it := range resp.body["items"].([]any) {
		got = append(got, fmt.Sprint(it.(map[string]any)["id"]))
	}
	if fmt.Sprint(got) != fmt.Sprint(ids) || resp.body["next_cursor"] != nil {
		t.Errorf("items = %v, next_cursor %v; want %v in request order and null", got, resp.body["next_cursor"], ids)
	}
}

func TestGetBatchPages(t *testing.T) {
	a := newAPI(t)
	batchID, ids := createBatch(t, a, "+905550000001", "+905550000002", "+905550000003", "+905550000004", "+905550000005")

	var got []string
	query := "?limit=2"
	for page := 1; page <= 4; page++ {
		resp := a.do(t, http.MethodGet, "/v1/batches/"+batchID+query, "")
		if resp.status != http.StatusOK {
			t.Fatalf("page %d: %d %s", page, resp.status, resp.raw)
		}
		for _, it := range resp.body["items"].([]any) {
			got = append(got, fmt.Sprint(it.(map[string]any)["id"]))
		}
		if resp.body["next_cursor"] == nil {
			break
		}
		query = "?limit=2&cursor=" + url.QueryEscape(fmt.Sprint(resp.body["next_cursor"]))
	}
	if fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Errorf("pages hold %v, want every notification once, oldest first: %v", got, ids)
	}
}

func TestGetBatchErrors(t *testing.T) {
	a := newAPI(t)
	batchID, _ := createBatch(t, a, "+905550000001")
	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantErrors []string
	}{
		{"unknown batch", "/v1/batches/" + uuid.Must(uuid.NewV7()).String(), http.StatusNotFound, nil},
		{"not a UUID", "/v1/batches/abc", http.StatusBadRequest, []string{"id invalid_format"}},
		{"bad limit and cursor", "/v1/batches/" + batchID + "?limit=0&cursor=x", http.StatusBadRequest, []string{"limit invalid_value", "cursor invalid_value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := a.do(t, http.MethodGet, tt.path, "")
			if resp.status != tt.wantStatus || fmt.Sprint(fieldErrors(resp.body)) != fmt.Sprint(tt.wantErrors) {
				t.Errorf("response = %d %v, want %d %v", resp.status, fieldErrors(resp.body), tt.wantStatus, tt.wantErrors)
			}
		})
	}
}
