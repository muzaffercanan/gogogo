// Package provider sends notifications to the external provider over HTTP and classifies its answers.
//
// The contract: a POST with {"to","channel","content"} is answered with 202
// {"messageId","status":"accepted","timestamp"}. Every request carries Idempotency-Key: <notification
// ID>, the same on every attempt (ADR 0007).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
)

// maxResponseBytes bounds how much of a response body is read.
const maxResponseBytes = 64 << 10

// maxReasonLen bounds the provider text kept in an Error's reason, which ends up in last_error.
const maxReasonLen = 200

// Message is one notification to send.
type Message struct {
	// NotificationID is sent as the Idempotency-Key.
	NotificationID string
	Channel        domain.Channel
	To             string
	Content        string
	// CorrelationID is sent as X-Correlation-ID when not empty.
	CorrelationID string
}

// Accepted is the provider's acceptance of a message.
type Accepted struct {
	// MessageID is the provider's ID for the message. It is empty when the provider answered 2xx with a
	// body that was not the expected JSON (Malformed).
	MessageID string
	// Malformed reports a 2xx answer whose body could not be read as {"messageId": ...}. The message
	// counts as sent: the provider accepted it, and sending it again could deliver it twice (ADR 0014).
	Malformed bool
	// StatusCode is the provider's 2xx status.
	StatusCode int
}

// Error is a send the provider did not accept.
type Error struct {
	// Retryable reports a transient failure (ADR 0009, ADR 0014): a 5xx, 408 or 429 answer, a timeout,
	// or a failed connection. Other answers are permanent.
	Retryable bool
	// StatusCode is the provider's status, or 0 if no answer arrived.
	StatusCode int
	// RetryAfter is the delay the provider asked for in a Retry-After header of a retryable answer, or 0.
	// The retry policy decides how much of it to honour.
	RetryAfter time.Duration
	// Reason describes the failure briefly, for last_error and logs.
	Reason string
	err    error
}

func (e *Error) Error() string { return e.Reason }

func (e *Error) Unwrap() error { return e.err }

// Client sends messages to the provider at one URL. It is safe for concurrent use. Create it with
// NewClient.
type Client struct {
	url     string
	timeout time.Duration
	http    *http.Client
	now     func() time.Time
}

// NewClient returns a client for the provider endpoint rawURL that gives each request timeout to
// complete. Each request gets a client span from tp, and its traceparent header continues that span's
// trace at the provider.
func NewClient(rawURL string, timeout time.Duration, tp trace.TracerProvider) (*Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("provider url %q is not an absolute http(s) url", rawURL)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("provider timeout %v is not positive", timeout)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Every send goes to the same host; the default of two idle connections would make most concurrent
	// sends open a new one.
	transport.MaxIdleConnsPerHost = 100
	return &Client{
		url:     rawURL,
		timeout: timeout,
		http: &http.Client{
			Transport: otelhttp.NewTransport(transport, otelhttp.WithTracerProvider(tp), otelhttp.WithPropagators(tracing.Propagator())),
			// A redirect is not part of the contract; following it would also turn the POST into a GET
			// for 301, 302 and 303.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: time.Now,
	}, nil
}

type request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type acceptance struct {
	MessageID string `json:"messageId"`
}

// Send posts m to the provider. It returns the acceptance for a 2xx answer, and otherwise an *Error
// that says whether the failure is retryable. If ctx ends first, it returns ctx.Err() instead: the
// outcome is unknown, so it is neither a failure nor an acceptance.
func (c *Client) Send(ctx context.Context, m Message) (Accepted, error) {
	body, err := json.Marshal(request{To: m.To, Channel: string(m.Channel), Content: m.Content})
	if err != nil {
		return Accepted{}, fmt.Errorf("encoding the request for %s: %w", m.NotificationID, err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Accepted{}, fmt.Errorf("building the request for %s: %w", m.NotificationID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Idempotency-Key", m.NotificationID)
	if m.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", m.CorrelationID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Accepted{}, ctxErr
		}
		reason := "provider request failed: " + err.Error()
		if errors.Is(err, context.DeadlineExceeded) {
			reason = fmt.Sprintf("provider did not answer within %v", c.timeout)
		}
		return Accepted{}, &Error{Retryable: true, Reason: reason, err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var a acceptance
		if readErr != nil || json.Unmarshal(raw, &a) != nil || a.MessageID == "" {
			return Accepted{Malformed: true, StatusCode: resp.StatusCode}, nil
		}
		return Accepted{MessageID: a.MessageID, StatusCode: resp.StatusCode}, nil
	}

	e := &Error{
		Retryable:  retryable(resp.StatusCode),
		StatusCode: resp.StatusCode,
		Reason:     reason(resp.StatusCode, raw),
	}
	if e.Retryable {
		e.RetryAfter = retryAfter(resp.Header.Get("Retry-After"), c.now())
	}
	return Accepted{}, e
}

// retryable reports whether an answer with this non-2xx status is worth retrying.
func retryable(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests || status == http.StatusRequestTimeout
}

// reason describes a non-2xx answer: its status and the provider's error text, if any.
func reason(status int, body []byte) string {
	text := string(body)
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		text = e.Error
	}
	if text = clip(text); text == "" {
		return fmt.Sprintf("provider answered %d", status)
	}
	return fmt.Sprintf("provider answered %d: %s", status, text)
}

// clip makes provider text safe to store in last_error: valid UTF-8 without NUL bytes (PostgreSQL text
// rejects both), whitespace collapsed, and at most maxReasonLen runes.
func clip(s string) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > maxReasonLen {
		s = string(r[:maxReasonLen]) + "..."
	}
	return s
}

// retryAfter reads a Retry-After value, either seconds or an HTTP date (RFC 9110), as a delay from now.
// A missing, invalid or past value is 0.
func retryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		const maxSecs = int64(1<<63-1) / int64(time.Second)
		return time.Duration(min(secs, maxSecs)) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
