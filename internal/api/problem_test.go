package api_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/muzaffercanan/gogogo/internal/api"
	"github.com/muzaffercanan/gogogo/internal/api/oapi"
	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
)

// want is the part of a problem a test checks; errors are compared as "field code" pairs.
type want struct {
	status int
	typ    string
	detail string
	errors []string
}

func check(t *testing.T, got oapi.Problem, w want) {
	t.Helper()
	var detail string
	if got.Detail != nil {
		detail = *got.Detail
	}
	var errs []string
	if got.Errors != nil {
		for _, e := range *got.Errors {
			errs = append(errs, e.Field+" "+string(e.Code))
		}
	}
	if got.Status != w.status || got.Type != w.typ || detail != w.detail || fmt.Sprint(errs) != fmt.Sprint(w.errors) {
		t.Errorf("problem = status %d, type %s, detail %q, errors %v; want status %d, type %s, detail %q, errors %v",
			got.Status, got.Type, detail, errs, w.status, w.typ, w.detail, w.errors)
	}
	if got.Title == "" {
		t.Error("problem has no title")
	}
}

func TestProblemFor(t *testing.T) {
	id := uuid.Must(uuid.NewV7())
	tests := []struct {
		name string
		err  error
		want want
	}{
		{
			name: "one invalid field",
			err: fmt.Errorf("creating: %w", &domain.ValidationError{Fields: []domain.FieldError{
				{Field: "recipient", Code: domain.CodeInvalidFormat, Message: "must be an E.164 phone number"},
			}}),
			want: want{400, "/problems/validation-error", "The request has 1 invalid field.", []string{"recipient invalid_format"}},
		},
		{
			name: "two invalid fields",
			err: &domain.ValidationError{Fields: []domain.FieldError{
				{Field: "recipient", Code: domain.CodeInvalidFormat, Message: "must be an E.164 phone number"},
				{Field: "content", Code: domain.CodeTooLong, Message: "needs 7 SMS segments (UCS-2), the limit is 6"},
			}},
			want: want{400, "/problems/validation-error", "The request has 2 invalid fields.", []string{"recipient invalid_format", "content too_long"}},
		},
		{
			name: "not found",
			err:  fmt.Errorf("notification %s: %w", id, domain.ErrNotFound),
			want: want{404, "/problems/not-found", "", nil},
		},
		{
			name: "cancel lost to a sent notification",
			err:  fmt.Errorf("cancelling: %w", &domain.StatusConflictError{ID: id, Current: domain.StatusSent, Wanted: domain.StatusCancelled}),
			want: want{409, "/problems/not-cancellable", "the notification is in status sent", nil},
		},
		{
			name: "conflict of a transition the API does not make",
			err:  &domain.StatusConflictError{ID: id, Current: domain.StatusSent, Wanted: domain.StatusSending},
			want: want{500, "/problems/internal-error", "", nil},
		},
		{
			name: "idempotency key reused",
			err:  fmt.Errorf("creating: %w", domain.ErrIdempotencyKeyReused),
			want: want{422, "/problems/idempotency-key-reused", "the key was first used with a different request body", nil},
		},
		{
			name: "body too large",
			err:  fmt.Errorf("reading body: %w", &http.MaxBytesError{Limit: 1024}),
			want: want{413, "/problems/payload-too-large", "the request body exceeds 1024 bytes", nil},
		},
		{
			name: "invalid transition is a bug",
			err:  fmt.Errorf("moving: %w", domain.ErrInvalidTransition),
			want: want{500, "/problems/internal-error", "", nil},
		},
		{
			name: "unknown error stays out of the response",
			err:  errors.New("dial tcp 10.0.0.7:5432: connection refused"),
			want: want{500, "/problems/internal-error", "", nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check(t, api.ProblemFor(tt.err), tt.want)
		})
	}
}

func TestRequestProblem(t *testing.T) {
	// decode reproduces how the generated server decodes and reports a request body.
	decode := func(body string, limit int64, into any) error {
		r := http.MaxBytesReader(httptest.NewRecorder(), io.NopCloser(strings.NewReader(body)), limit)
		if err := json.NewDecoder(r).Decode(into); err != nil {
			return fmt.Errorf("can't decode JSON body: %w", err)
		}
		return nil
	}
	single := func(body string) error { return decode(body, 1<<20, &oapi.NotificationCreate{}) }

	tests := []struct {
		name string
		err  error
		want want
	}{
		{"empty body", single(""), want{400, "/problems/validation-error", "the request body is empty", nil}},
		{"not JSON", single("channel=sms"), want{400, "/problems/validation-error", "the request body is not valid JSON", nil}},
		{"truncated JSON", single(`{"channel": "sms"`), want{400, "/problems/validation-error", "the request body is not valid JSON", nil}},
		{"wrong type", single(`{"channel": 5}`), want{400, "/problems/validation-error", "The request has 1 invalid field.", []string{"channel invalid_value"}}},
		{
			"wrong type in a batch item",
			decode(`{"notifications": [{"content": "ok"}, {"content": true}]}`, 1<<20, &oapi.BatchCreate{}),
			want{400, "/problems/validation-error", "The request has 1 invalid field.", []string{"notifications[1].content invalid_value"}},
		},
		{
			"date-time not in RFC 3339",
			single(`{"scheduled_at": "tomorrow"}`),
			want{400, "/problems/validation-error", "a date-time value is not in RFC 3339 format, e.g. 2026-12-01T09:00:00Z", nil},
		},
		{"body too large", decode(`{"content": "`+strings.Repeat("x", 100)+`"}`, 64, &oapi.NotificationCreate{}), want{413, "/problems/payload-too-large", "the request body exceeds 64 bytes", nil}},
		{
			"parameter in the wrong format",
			&oapi.InvalidParamFormatError{ParamName: "created_from", Err: timeParseError(t)},
			want{400, "/problems/validation-error", "The request has 1 invalid field.", []string{"created_from invalid_format"}},
		},
		{
			"missing parameter",
			&oapi.RequiredParamError{ParamName: "limit"},
			want{400, "/problems/validation-error", "The request has 1 invalid field.", []string{"limit required"}},
		},
		{
			"repeated header",
			&oapi.TooManyValuesForParamError{ParamName: "Idempotency-Key", Count: 2},
			want{400, "/problems/validation-error", "The request has 1 invalid field.", []string{"Idempotency-Key invalid_value"}},
		},
		{"anything else", errors.New("something odd"), want{400, "/problems/validation-error", "the request is malformed", nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("the request decoded without error")
			}
			check(t, api.RequestProblem(tt.err), tt.want)
		})
	}
}

func TestWriteProblem(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/notifications/123", nil)

	api.WriteProblem(rec, req, api.ProblemFor(domain.ErrNotFound))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body %s: %v", rec.Body, err)
	}
	if body["type"] != "/problems/not-found" || body["status"] != float64(404) || body["instance"] != "/v1/notifications/123" {
		t.Errorf("body = %v, want type /problems/not-found, status 404 and the request path as instance", body)
	}
}

func TestResponseErrorHandlerLogsOnlyInternalErrors(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantLog bool
	}{
		{"internal error", errors.New("connection refused"), true},
		{"client error", domain.ErrNotFound, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			handle := api.ResponseErrorHandler(logging.New(&logs, "debug"))
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications", nil)
			req = req.WithContext(logging.WithCorrelationID(req.Context(), "corr-1"))

			handle(httptest.NewRecorder(), req, tt.err)

			logged := logs.Len() > 0
			if logged != tt.wantLog {
				t.Fatalf("logged = %v, want %v; logs: %s", logged, tt.wantLog, logs.String())
			}
			if logged && (!strings.Contains(logs.String(), tt.err.Error()) || !strings.Contains(logs.String(), `"correlation_id":"corr-1"`)) {
				t.Errorf("log %s does not hold the error and the correlation ID", logs.String())
			}
		})
	}
}

// timeParseError returns the error the router wraps for a date-time parameter that does not parse.
func timeParseError(t *testing.T) error {
	t.Helper()
	_, err := time.Parse(time.RFC3339, "tomorrow")
	var parseErr *time.ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("time.Parse error = %v, want a *time.ParseError", err)
	}
	return err
}
