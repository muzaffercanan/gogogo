//go:build integration

package api_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func TestGetNotification(t *testing.T) {
	a := newAPI(t)
	created := a.do(t, http.MethodPost, "/v1/notifications", `{"channel":"email","recipient":"jane@example.com","content":"Hi"}`)
	id := fmt.Sprint(created.body["id"])

	resp := a.do(t, http.MethodGet, "/v1/notifications/"+id, "")

	if resp.status != http.StatusOK || resp.header.Get("Content-Type") != "application/json" {
		t.Fatalf("response = %d %s %s, want 200 JSON", resp.status, resp.header.Get("Content-Type"), resp.raw)
	}
	if string(resp.raw) != string(created.raw) {
		t.Errorf("body = %s, want what the create returned: %s", resp.raw, created.raw)
	}
}

func TestGetNotificationErrors(t *testing.T) {
	a := newAPI(t)
	tests := []struct {
		name       string
		id         string
		wantStatus int
		wantErrors []string
	}{
		{"unknown ID", uuid.Must(uuid.NewV7()).String(), http.StatusNotFound, nil},
		{"not a UUID", "123", http.StatusBadRequest, []string{"id invalid_format"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := a.do(t, http.MethodGet, "/v1/notifications/"+tt.id, "")

			if resp.status != tt.wantStatus || resp.header.Get("Content-Type") != "application/problem+json" ||
				fmt.Sprint(fieldErrors(resp.body)) != fmt.Sprint(tt.wantErrors) {
				t.Errorf("response = %d %s %v, want a %d problem with %v", resp.status, resp.header.Get("Content-Type"),
					fieldErrors(resp.body), tt.wantStatus, tt.wantErrors)
			}
		})
	}
}
