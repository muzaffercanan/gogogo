//go:build integration

package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	smsBody = `{"channel":"sms","recipient":"+905551234567","content":"Kodunuz: 1234","scheduled_at":"2026-11-25T12:00:00+03:00"}`
	// sameSMS asks for the same notification as smsBody, written differently.
	sameSMS = `{ "scheduled_at": "2026-11-25T09:00:00Z", "priority": "normal", "content": "Kodunuz: 1234",
		"recipient": "+905551234567", "channel": "sms", "unknown": true }`
	otherSMS = `{"channel":"sms","recipient":"+905551234567","content":"Kodunuz: 9999","scheduled_at":"2026-11-25T09:00:00Z"}`
)

func TestIdempotencyKey(t *testing.T) {
	tests := []struct {
		name         string
		first, again string
		wantStatus   int
		wantType     string
	}{
		{"same body replays", smsBody, smsBody, http.StatusAccepted, ""},
		{"same request written differently replays", smsBody, sameSMS, http.StatusAccepted, ""},
		{"different body is refused", smsBody, otherSMS, http.StatusUnprocessableEntity, "/problems/idempotency-key-reused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t)
			a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
			first := a.do(t, http.MethodPost, "/v1/notifications", tt.first, "Idempotency-Key", "key-1")
			if first.status != http.StatusAccepted {
				t.Fatalf("first request: %d %s", first.status, first.raw)
			}

			again := a.do(t, http.MethodPost, "/v1/notifications", tt.again, "Idempotency-Key", "key-1")

			if again.status != tt.wantStatus {
				t.Fatalf("repeated request: %d %s, want %d", again.status, again.raw, tt.wantStatus)
			}
			if tt.wantType != "" && again.body["type"] != tt.wantType {
				t.Errorf("problem type = %v, want %s", again.body["type"], tt.wantType)
			}
			if tt.wantStatus == http.StatusAccepted {
				if again.body["id"] != first.body["id"] || again.header.Get("Idempotent-Replayed") != "true" ||
					again.header.Get("Location") != first.header.Get("Location") {
					t.Errorf("replay = id %v, Idempotent-Replayed %q, Location %q; want id %v, true, %q",
						again.body["id"], again.header.Get("Idempotent-Replayed"), again.header.Get("Location"),
						first.body["id"], first.header.Get("Location"))
				}
			}
			if n := a.count(t, "notifications"); n != 1 {
				t.Errorf("%d notifications, want 1", n)
			}
		})
	}
}

// TestIdempotentReplayAfterScheduledTimePassed replays a request whose scheduled_at is no longer in the
// future: the replay must not be validated again.
func TestIdempotentReplayAfterScheduledTimePassed(t *testing.T) {
	a := newAPI(t)
	a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	first := a.do(t, http.MethodPost, "/v1/notifications", smsBody, "Idempotency-Key", "key-1")

	a.setNow(time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC))
	again := a.do(t, http.MethodPost, "/v1/notifications", smsBody, "Idempotency-Key", "key-1")

	if again.status != http.StatusAccepted || again.body["id"] != first.body["id"] {
		t.Errorf("replay = %d %s, want 202 with id %v", again.status, again.raw, first.body["id"])
	}
	if fresh := a.do(t, http.MethodPost, "/v1/notifications", smsBody, "Idempotency-Key", "key-2"); fresh.status != http.StatusBadRequest {
		t.Errorf("new request with the passed scheduled_at = %d, want 400", fresh.status)
	}
}

func TestIdempotencyKeyFormat(t *testing.T) {
	a := newAPI(t)
	a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	tests := []struct {
		name       string
		key        string
		wantStatus int
		wantErrors []string
	}{
		{"255 characters", strings.Repeat("ş", 255), http.StatusAccepted, nil},
		{"256 characters", strings.Repeat("k", 256), http.StatusBadRequest, []string{"Idempotency-Key too_long"}},
		{"empty", "", http.StatusBadRequest, []string{"Idempotency-Key too_short"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := a.do(t, http.MethodPost, "/v1/notifications", smsBody, "Idempotency-Key", tt.key)
			if resp.status != tt.wantStatus || fmt.Sprint(fieldErrors(resp.body)) != fmt.Sprint(tt.wantErrors) {
				t.Errorf("response = %d %v, want %d %v", resp.status, fieldErrors(resp.body), tt.wantStatus, tt.wantErrors)
			}
		})
	}
}

func TestRejectedRequestDoesNotTakeTheKey(t *testing.T) {
	a := newAPI(t)
	a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	if resp := a.do(t, http.MethodPost, "/v1/notifications", `{"channel":"sms"}`, "Idempotency-Key", "key-1"); resp.status != http.StatusBadRequest {
		t.Fatalf("invalid request = %d, want 400", resp.status)
	}
	if resp := a.do(t, http.MethodPost, "/v1/notifications", smsBody, "Idempotency-Key", "key-1"); resp.status != http.StatusAccepted ||
		resp.header.Get("Idempotent-Replayed") != "false" {
		t.Errorf("valid request with the same key = %d, replayed %q; want 202, false", resp.status, resp.header.Get("Idempotent-Replayed"))
	}
}

func TestWithoutIdempotencyKeyEveryRequestCreates(t *testing.T) {
	a := newAPI(t)
	a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	for range 2 {
		if resp := a.do(t, http.MethodPost, "/v1/notifications", smsBody); resp.status != http.StatusAccepted {
			t.Fatalf("create = %d %s", resp.status, resp.raw)
		}
	}
	if n := a.count(t, "notifications"); n != 2 {
		t.Errorf("%d notifications, want 2", n)
	}
}

// TestConcurrentRequestsWithOneKey sends requests with the same key at once, half of them with another
// body: exactly one creates, the others with the winner's body replay it and the rest get 422.
func TestConcurrentRequestsWithOneKey(t *testing.T) {
	a := newAPI(t)
	a.setNow(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	const callers = 20

	type result struct {
		body     string
		status   int
		replayed string
		id       any
	}
	results := make([]result, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		body := smsBody
		if i%2 == 1 {
			body = otherSMS
		}
		wg.Go(func() {
			<-start
			resp := a.do(t, http.MethodPost, "/v1/notifications", body, "Idempotency-Key", "contended")
			results[i] = result{body, resp.status, resp.header.Get("Idempotent-Replayed"), resp.body["id"]}
		})
	}
	close(start)
	wg.Wait()

	var winner *result
	for i := range results {
		if results[i].status == http.StatusAccepted && results[i].replayed == "false" {
			if winner != nil {
				t.Fatalf("two requests created a notification: %+v and %+v", *winner, results[i])
			}
			winner = &results[i]
		}
	}
	if winner == nil {
		t.Fatalf("no request created the notification: %+v", results)
	}
	for _, r := range results {
		switch {
		case r == *winner:
		case r.body == winner.body && (r.status != http.StatusAccepted || r.replayed != "true" || r.id != winner.id):
			t.Errorf("request with the winning body = %+v, want a replay of %v", r, winner.id)
		case r.body != winner.body && r.status != http.StatusUnprocessableEntity:
			t.Errorf("request with the other body = %+v, want 422", r)
		}
	}
	if n := a.count(t, "notifications"); n != 1 {
		t.Errorf("%d notifications, want 1", n)
	}
}
