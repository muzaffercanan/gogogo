//go:build integration

package api_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// createInStatus creates a notification and sets its status directly, as if it had been processed.
func createInStatus(t *testing.T, a *testAPI, status string) string {
	t.Helper()
	resp := a.do(t, http.MethodPost, "/v1/notifications", `{"channel":"sms","recipient":"+905551234567","content":"Hi"}`)
	if resp.status != http.StatusAccepted {
		t.Fatalf("creating notification: %d %s", resp.status, resp.raw)
	}
	id := fmt.Sprint(resp.body["id"])
	if _, err := a.db.Exec(t.Context(), `UPDATE notifications SET status = $2 WHERE id = $1`, id, status); err != nil {
		t.Fatalf("setting status %s: %v", status, err)
	}
	return id
}

func TestCancelNotification(t *testing.T) {
	for _, status := range []string{"pending", "queued", "retry_scheduled"} {
		t.Run(status, func(t *testing.T) {
			a := newAPI(t)
			id := createInStatus(t, a, status)

			resp := a.do(t, http.MethodPost, "/v1/notifications/"+id+"/cancel", "")

			if resp.status != http.StatusOK || resp.body["status"] != "cancelled" || resp.body["id"] != id {
				t.Fatalf("cancel = %d %s, want 200 with status cancelled", resp.status, resp.raw)
			}
			var from, reason string
			err := a.db.QueryRow(t.Context(), `SELECT from_status, reason FROM notification_events
				WHERE notification_id = $1 AND to_status = 'cancelled'`, id).Scan(&from, &reason)
			if err != nil || from != status || reason != "cancelled by client" {
				t.Errorf("cancel event = %q, %q (err %v), want from %s with the client reason", from, reason, err, status)
			}
		})
	}
}

func TestCancelNotificationTwiceIsUnchanged(t *testing.T) {
	a := newAPI(t)
	id := createInStatus(t, a, "pending")
	first := a.do(t, http.MethodPost, "/v1/notifications/"+id+"/cancel", "")

	again := a.do(t, http.MethodPost, "/v1/notifications/"+id+"/cancel", "")

	if again.status != http.StatusOK || string(again.raw) != string(first.raw) {
		t.Errorf("second cancel = %d %s, want 200 and the unchanged notification %s", again.status, again.raw, first.raw)
	}
	var events int
	if err := a.db.QueryRow(t.Context(), `SELECT count(*) FROM notification_events WHERE notification_id = $1 AND to_status = 'cancelled'`, id).
		Scan(&events); err != nil || events != 1 {
		t.Errorf("%d cancel events (err %v), want 1", events, err)
	}
}

func TestCancelNotificationTooLate(t *testing.T) {
	for _, status := range []string{"sending", "sent", "permanently_failed"} {
		t.Run(status, func(t *testing.T) {
			a := newAPI(t)
			id := createInStatus(t, a, status)

			resp := a.do(t, http.MethodPost, "/v1/notifications/"+id+"/cancel", "")

			if resp.status != http.StatusConflict || resp.body["type"] != "/problems/not-cancellable" ||
				resp.body["detail"] != "the notification is in status "+status {
				t.Errorf("cancel = %d %s, want a 409 not-cancellable problem naming %s", resp.status, resp.raw, status)
			}
			var current string
			if err := a.db.QueryRow(t.Context(), `SELECT status FROM notifications WHERE id = $1`, id).Scan(&current); err != nil || current != status {
				t.Errorf("status = %s (err %v) after a refused cancel, want %s", current, err, status)
			}
		})
	}
}

func TestCancelUnknownNotification(t *testing.T) {
	resp := newAPI(t).do(t, http.MethodPost, "/v1/notifications/"+uuid.Must(uuid.NewV7()).String()+"/cancel", "")
	if resp.status != http.StatusNotFound || resp.body["type"] != "/problems/not-found" {
		t.Errorf("cancel = %d %s, want a 404 not-found problem", resp.status, resp.raw)
	}
}
