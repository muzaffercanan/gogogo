package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/provider"
	"github.com/muzaffercanan/gogogo/internal/provider/mockserver"
)

var message = provider.Message{
	NotificationID: "0195f3c1-7c2a-7000-8000-000000000001",
	Channel:        domain.ChannelSMS,
	To:             "+905551112233",
	Content:        "Your code is 1234",
	CorrelationID:  "corr-1",
}

func newClient(t *testing.T, url string, timeout time.Duration) *provider.Client {
	t.Helper()
	c, err := provider.NewClient(url, timeout, noop.NewTracerProvider())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// answering returns a provider that answers every request with status, headers and body.
func answering(t *testing.T, status int, header map[string]string, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func providerError(t *testing.T, err error) *provider.Error {
	t.Helper()
	var pe *provider.Error
	if !errors.As(err, &pe) {
		t.Fatalf("Send error = %v (%T), want a *provider.Error", err, err)
	}
	return pe
}

func TestSendPostsTheMessageWithItsIdempotencyKey(t *testing.T) {
	var got struct {
		method, contentType, key, correlation string
		body                                  map[string]string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method = r.Method
		got.contentType = r.Header.Get("Content-Type")
		got.key = r.Header.Get("Idempotency-Key")
		got.correlation = r.Header.Get("X-Correlation-ID")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"m-1","status":"accepted","timestamp":"2026-11-24T12:00:00Z"}`)
	}))
	defer srv.Close()

	a, err := newClient(t, srv.URL, time.Second).Send(t.Context(), message)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if a != (provider.Accepted{MessageID: "m-1", StatusCode: http.StatusAccepted}) {
		t.Errorf("Send = %+v, want message m-1 accepted with 202", a)
	}
	if got.method != http.MethodPost || got.contentType != "application/json" {
		t.Errorf("request was %s with Content-Type %q, want a JSON POST", got.method, got.contentType)
	}
	if got.key != message.NotificationID || got.correlation != message.CorrelationID {
		t.Errorf("Idempotency-Key %q, X-Correlation-ID %q; want %q and %q", got.key, got.correlation, message.NotificationID, message.CorrelationID)
	}
	want := map[string]string{"to": message.To, "channel": "sms", "content": message.Content}
	if len(got.body) != len(want) || got.body["to"] != want["to"] || got.body["channel"] != want["channel"] || got.body["content"] != want["content"] {
		t.Errorf("request body %v, want %v", got.body, want)
	}
}

func TestSendClassifiesProviderAnswers(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantMessageID string
		wantMalformed bool
		wantErr       bool
		wantRetryable bool
	}{
		{name: "accepted", status: 202, body: `{"messageId":"m-1"}`, wantMessageID: "m-1"},
		{name: "ok", status: 200, body: `{"messageId":"m-2"}`, wantMessageID: "m-2"},
		{name: "2xx with a body that is not JSON", status: 202, body: `<html>ok</html>`, wantMalformed: true},
		{name: "2xx without a message id", status: 202, body: `{"status":"accepted"}`, wantMalformed: true},
		{name: "no content", status: 204, wantMalformed: true},
		{name: "bad request", status: 400, body: `{"error":"recipient rejected"}`, wantErr: true},
		{name: "unauthorized", status: 401, wantErr: true},
		{name: "not found", status: 404, wantErr: true},
		{name: "unprocessable", status: 422, wantErr: true},
		{name: "request timeout", status: 408, wantErr: true, wantRetryable: true},
		{name: "too many requests", status: 429, wantErr: true, wantRetryable: true},
		{name: "internal error", status: 500, wantErr: true, wantRetryable: true},
		{name: "bad gateway", status: 502, wantErr: true, wantRetryable: true},
		{name: "unavailable", status: 503, wantErr: true, wantRetryable: true},
		{name: "redirect, not followed", status: 302, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := map[string]string{"Location": "/elsewhere"}
			srv := answering(t, tt.status, header, tt.body)
			a, err := newClient(t, srv.URL, time.Second).Send(t.Context(), message)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Send: %v", err)
				}
				if a.MessageID != tt.wantMessageID || a.Malformed != tt.wantMalformed || a.StatusCode != tt.status {
					t.Errorf("Send = %+v, want message id %q, malformed %t, status %d", a, tt.wantMessageID, tt.wantMalformed, tt.status)
				}
				return
			}
			pe := providerError(t, err)
			if pe.Retryable != tt.wantRetryable || pe.StatusCode != tt.status {
				t.Errorf("Send error %+v, want retryable %t with status %d", pe, tt.wantRetryable, tt.status)
			}
			if !strings.HasPrefix(pe.Reason, "provider answered "+strconv.Itoa(tt.status)) {
				t.Errorf("reason %q does not name status %d", pe.Reason, tt.status)
			}
		})
	}
}

func TestSendReadsRetryAfterOnRetryableAnswers(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		status int
		value  string
		want   time.Duration
	}{
		{name: "seconds", status: 429, value: "3", want: 3 * time.Second},
		{name: "http date", status: 503, value: now.Add(90 * time.Second).Format(http.TimeFormat), want: 90 * time.Second},
		{name: "date in the past", status: 429, value: now.Add(-time.Minute).Format(http.TimeFormat)},
		{name: "not a delay", status: 429, value: "soon"},
		{name: "negative", status: 429, value: "-5"},
		{name: "absent", status: 429},
		{name: "huge, left to the retry policy", status: 429, value: "7200", want: 2 * time.Hour},
		{name: "on a permanent answer", status: 400, value: "3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := map[string]string{}
			if tt.value != "" {
				header["Retry-After"] = tt.value
			}
			srv := answering(t, tt.status, header, "")
			c := newClient(t, srv.URL, time.Second)
			provider.SetClock(c, func() time.Time { return now })
			_, err := c.Send(t.Context(), message)
			if got := providerError(t, err).RetryAfter; got != tt.want {
				t.Errorf("RetryAfter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSendFailsRetryablyWhenTheProviderDoesNotAnswerInTime(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select { // answers after two seconds, long after the client's timeout
		case <-release:
		case <-time.After(2 * time.Second):
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	_, err := newClient(t, srv.URL, 50*time.Millisecond).Send(t.Context(), message)
	pe := providerError(t, err)
	if !pe.Retryable || pe.StatusCode != 0 || !strings.Contains(pe.Reason, "did not answer within 50ms") {
		t.Errorf("Send error %+v, want a retryable timeout without a status", pe)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Send took %v with a 50ms timeout", elapsed)
	}
}

func TestSendFailsRetryablyWhenTheProviderIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	_, err := newClient(t, url, time.Second).Send(t.Context(), message)
	if pe := providerError(t, err); !pe.Retryable || pe.StatusCode != 0 {
		t.Errorf("Send error %+v, want a retryable failure without a status", pe)
	}
}

func TestSendReturnsTheContextErrorWhenTheCallerGivesUp(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := newClient(t, srv.URL, 10*time.Second).Send(ctx, message)
	var pe *provider.Error
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &pe) {
		t.Errorf("Send = %v, want the caller's context error and no provider failure", err)
	}
}

func TestSendKeepsTheReasonShortAndStorable(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantPrefix string
	}{
		{name: "long text is clipped", body: strings.Repeat("ğ", 500), wantPrefix: "provider answered 500: ğğ"},
		{name: "invalid bytes and NUL are dropped", body: "bad \xff byte\x00 here", wantPrefix: "provider answered 500: bad byte here"},
		{name: "whitespace is collapsed", body: "line one\n\n\tline two", wantPrefix: "provider answered 500: line one line two"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := answering(t, http.StatusInternalServerError, nil, tt.body)
			_, err := newClient(t, srv.URL, time.Second).Send(t.Context(), message)
			r := providerError(t, err).Reason
			// PostgreSQL rejects text that is not valid UTF-8 or holds a NUL byte.
			if !utf8.ValidString(r) || strings.ContainsRune(r, 0) {
				t.Errorf("reason is not valid UTF-8 without NUL: %q", r)
			}
			if n := utf8.RuneCountInString(r); n > 250 {
				t.Errorf("reason has %d runes, want it clipped near 200", n)
			}
			if !strings.HasPrefix(r, tt.wantPrefix) {
				t.Errorf("reason %q, want it to start with %q", r, tt.wantPrefix)
			}
		})
	}
}

func TestSendClassifiesEachAnswerOfTheMockProvider(t *testing.T) {
	cfg := mockserver.Config{RejectRate: 0.25, ThrottleRate: 0.25, FailureRate: 0.25, RetryAfter: 2 * time.Second}
	tests := []struct {
		roll          float64
		wantErr       bool
		wantRetryable bool
		wantStatus    int
		wantRetry     time.Duration
	}{
		{roll: 0.1, wantErr: true, wantStatus: 400},
		{roll: 0.3, wantErr: true, wantRetryable: true, wantStatus: 429, wantRetry: 2 * time.Second},
		{roll: 0.6, wantErr: true, wantRetryable: true, wantStatus: 500},
		{roll: 0.9, wantStatus: 202},
	}
	for _, tt := range tests {
		mock, err := mockserver.New(cfg, prometheus.NewRegistry(), mockserver.WithRoll(func() float64 { return tt.roll }))
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(mock)
		a, err := newClient(t, srv.URL, time.Second).Send(t.Context(), message)
		srv.Close()
		if !tt.wantErr {
			if err != nil || a.MessageID == "" || a.Malformed || a.StatusCode != tt.wantStatus {
				t.Errorf("roll %v: Send = %+v, %v; want an accepted message with an id", tt.roll, a, err)
			}
			continue
		}
		pe := providerError(t, err)
		if pe.Retryable != tt.wantRetryable || pe.StatusCode != tt.wantStatus || pe.RetryAfter != tt.wantRetry {
			t.Errorf("roll %v: Send error %+v, want retryable %t, status %d, retry after %v", tt.roll, pe, tt.wantRetryable, tt.wantStatus, tt.wantRetry)
		}
	}
}

func TestNewClientRejectsBadURLsAndTimeouts(t *testing.T) {
	for _, tt := range []struct {
		url     string
		timeout time.Duration
	}{
		{url: "mock-provider:8080/send", timeout: time.Second},
		{url: "ftp://example.test/send", timeout: time.Second},
		{url: "http:///send", timeout: time.Second},
		{url: "http://example.test/send", timeout: 0},
	} {
		if _, err := provider.NewClient(tt.url, tt.timeout, noop.NewTracerProvider()); err == nil {
			t.Errorf("NewClient(%q, %v) succeeded, want an error", tt.url, tt.timeout)
		}
	}
	if _, err := provider.NewClient("http://mock-provider:8080/send", 10*time.Second, noop.NewTracerProvider()); err != nil {
		t.Errorf("NewClient with a valid url: %v", err)
	}
}
