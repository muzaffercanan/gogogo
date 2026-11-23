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

func TestCreateNotification(t *testing.T) {
	a := newAPI(t)
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	a.now.Store(&now)
	tests := []struct {
		name         string
		body         string
		wantPriority string
		wantSchedule any
	}{
		{"sms", `{"channel":"sms","recipient":"+905551234567","content":"Doğrulama kodunuz: 482913","priority":"high"}`, "high", nil},
		{"email with default priority", `{"channel":"email","recipient":"jane@example.com","content":"Your order has shipped."}`, "normal", nil},
		{
			"scheduled push in another time zone",
			`{"channel":"push","recipient":"f3c9a1b2","content":"Sale in one hour.","priority":"low","scheduled_at":"2026-12-01T12:00:00+03:00"}`,
			"low", "2026-12-01T09:00:00Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := a.do(t, http.MethodPost, "/v1/notifications", tt.body, "X-Correlation-ID", "corr-"+tt.name[:3])

			if resp.status != http.StatusAccepted {
				t.Fatalf("status = %d, want 202; body %s", resp.status, resp.raw)
			}
			id, err := uuid.Parse(fmt.Sprint(resp.body["id"]))
			if err != nil {
				t.Fatalf("id %v is not a UUID", resp.body["id"])
			}
			if loc := resp.header.Get("Location"); loc != "/v1/notifications/"+id.String() {
				t.Errorf("Location = %q, want /v1/notifications/%s", loc, id)
			}
			if got := resp.header.Get("Idempotent-Replayed"); got != "false" {
				t.Errorf("Idempotent-Replayed = %q, want false", got)
			}
			if got := resp.header.Get("X-Correlation-ID"); got != "corr-"+tt.name[:3] {
				t.Errorf("X-Correlation-ID = %q, want the request's", got)
			}
			for key, want := range map[string]any{
				"status": "pending", "priority": tt.wantPriority, "attempts": float64(0), "max_attempts": float64(5),
				"correlation_id": "corr-" + tt.name[:3], "scheduled_at": tt.wantSchedule,
				"batch_id": nil, "last_error": nil, "provider_message_id": nil, "sent_at": nil,
			} {
				got, present := resp.body[key]
				if !present || got != want {
					t.Errorf("%s = %v (present %v), want %v", key, got, present, want)
				}
			}

			var outbox int
			if err := a.db.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE notification_id = $1`, id).Scan(&outbox); err != nil || outbox != 1 {
				t.Errorf("%d outbox rows (err %v), want 1", outbox, err)
			}
		})
	}
}

func TestCreateNotificationRejectsBadRequests(t *testing.T) {
	a := newAPI(t)
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantType   string
		wantErrors []string
	}{
		{
			"invalid fields",
			`{"channel":"sms","recipient":"05551234567","content":"` + strings.Repeat("ş", 403) + `"}`,
			400, "/problems/validation-error", []string{"recipient invalid_format", "content too_long"},
		},
		{"missing fields", `{}`, 400, "/problems/validation-error", []string{"channel required", "recipient required", "content required"}},
		{"wrong type", `{"channel":1}`, 400, "/problems/validation-error", []string{"channel invalid_value"}},
		{"not JSON", `channel=sms`, 400, "/problems/validation-error", nil},
		{"body over 1 MiB", `{"content":"` + strings.Repeat("a", 1<<20) + `"}`, 413, "/problems/payload-too-large", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := a.do(t, http.MethodPost, "/v1/notifications", tt.body)

			if resp.status != tt.wantStatus || resp.body["type"] != tt.wantType || resp.header.Get("Content-Type") != "application/problem+json" {
				t.Fatalf("response = %d %s %v, want %d %s problem", resp.status, resp.header.Get("Content-Type"), resp.body["type"], tt.wantStatus, tt.wantType)
			}
			if got := fieldErrors(resp.body); fmt.Sprint(got) != fmt.Sprint(tt.wantErrors) {
				t.Errorf("errors = %v, want %v", got, tt.wantErrors)
			}
			if resp.body["instance"] != "/v1/notifications" {
				t.Errorf("instance = %v, want the request path", resp.body["instance"])
			}
		})
	}
	if n := a.count(t, "notifications"); n != 0 {
		t.Errorf("%d notifications stored by rejected requests, want 0", n)
	}
}

func TestUnknownRouteIsAProblem(t *testing.T) {
	resp := newAPI(t).do(t, http.MethodGet, "/v2/anything", "")

	if resp.status != http.StatusNotFound || resp.body["type"] != "/problems/not-found" {
		t.Errorf("response = %d %s, want a 404 not-found problem", resp.status, resp.raw)
	}
	if resp.header.Get("X-Correlation-ID") == "" {
		t.Error("response has no X-Correlation-ID")
	}
}
