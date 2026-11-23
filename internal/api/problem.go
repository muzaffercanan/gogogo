// Package api implements the HTTP API on top of the server interface generated from api/openapi.yaml.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/muzaffercanan/gogogo/internal/api/oapi"
	"github.com/muzaffercanan/gogogo/internal/domain"
)

// Problem types: the stable identifiers listed in api/openapi.yaml.
const (
	problemValidation           = "/problems/validation-error"
	problemNotFound             = "/problems/not-found"
	problemNotCancellable       = "/problems/not-cancellable"
	problemPayloadTooLarge      = "/problems/payload-too-large"
	problemIdempotencyKeyReused = "/problems/idempotency-key-reused"
	problemInternal             = "/problems/internal-error"
)

// ProblemFor returns the problem that answers a request whose handling failed with err. An error it does
// not recognise is an internal error: its text may reveal internals, so it stays out of the response.
func ProblemFor(err error) oapi.Problem {
	var (
		invalid  *domain.ValidationError
		conflict *domain.StatusConflictError
		tooLarge *http.MaxBytesError
	)
	switch {
	case errors.As(err, &invalid):
		return validationProblem(invalid.Fields)
	case errors.Is(err, domain.ErrNotFound):
		return oapi.Problem{Type: problemNotFound, Title: "Not found", Status: http.StatusNotFound}
	case errors.As(err, &conflict) && conflict.Wanted == domain.StatusCancelled:
		return oapi.Problem{
			Type:   problemNotCancellable,
			Title:  "Notification cannot be cancelled",
			Status: http.StatusConflict,
			Detail: ptr(fmt.Sprintf("the notification is in status %s", conflict.Current)),
		}
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		return oapi.Problem{
			Type:   problemIdempotencyKeyReused,
			Title:  "Idempotency key reused with a different request",
			Status: http.StatusUnprocessableEntity,
			Detail: ptr("the key was first used with a different request body"),
		}
	case errors.As(err, &tooLarge):
		return payloadTooLarge(tooLarge.Limit)
	default:
		return oapi.Problem{Type: problemInternal, Title: "Internal server error", Status: http.StatusInternalServerError}
	}
}

// RequestProblem returns the problem that answers a request that could not be decoded: a parameter in
// the wrong format, or a body that is too large, empty, not JSON or of the wrong shape.
func RequestProblem(err error) oapi.Problem {
	var (
		badParam *oapi.InvalidParamFormatError
		missing  *oapi.RequiredParamError
		repeated *oapi.TooManyValuesForParamError
		tooLarge *http.MaxBytesError
		syntax   *json.SyntaxError
		wrong    *json.UnmarshalTypeError
		badTime  *time.ParseError
	)
	// Parameter errors come first: they wrap the parse error of the value, such as a *time.ParseError.
	switch {
	case errors.As(err, &badParam):
		return fieldProblem(badParam.ParamName, domain.CodeInvalidFormat, "has an invalid format")
	case errors.As(err, &missing):
		return fieldProblem(missing.ParamName, domain.CodeRequired, "is required")
	case errors.As(err, &repeated):
		return fieldProblem(repeated.ParamName, domain.CodeInvalidValue, "must be given once")
	case errors.As(err, &tooLarge):
		return payloadTooLarge(tooLarge.Limit)
	case errors.Is(err, io.EOF):
		return badRequest("the request body is empty")
	case errors.As(err, &syntax), errors.Is(err, io.ErrUnexpectedEOF):
		return badRequest("the request body is not valid JSON")
	case errors.As(err, &wrong):
		return fieldProblem(fieldPath(wrong.Field), domain.CodeInvalidValue, "must be "+jsonKind(wrong.Type))
	case errors.As(err, &badTime):
		return badRequest("a date-time value is not in RFC 3339 format, e.g. 2026-12-01T09:00:00Z")
	default:
		return badRequest("the request is malformed")
	}
}

// WriteProblem writes p as an application/problem+json response, with the request path as its instance
// unless p has one.
func WriteProblem(w http.ResponseWriter, r *http.Request, p oapi.Problem) {
	if p.Instance == nil {
		p.Instance = ptr(r.URL.Path)
	}
	body, err := json.Marshal(p)
	if err != nil {
		// Unreachable: a Problem holds only strings and integers.
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_, _ = w.Write(body)
}

// RequestErrorHandler answers a request that could not be decoded with RequestProblem. It fits both the
// router's ErrorHandlerFunc and the strict server's RequestErrorHandlerFunc.
func RequestErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	WriteProblem(w, r, RequestProblem(err))
}

// ResponseErrorHandler returns the strict server's ResponseErrorHandlerFunc. It answers with
// ProblemFor(err) and logs err when it becomes an internal error, since the response does not show it.
func ResponseErrorHandler(logger *slog.Logger) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		p := ProblemFor(err)
		if p.Status >= http.StatusInternalServerError {
			logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		}
		WriteProblem(w, r, p)
	}
}

func validationProblem(fields []domain.FieldError) oapi.Problem {
	errs := make([]oapi.FieldError, len(fields))
	for i, f := range fields {
		errs[i] = oapi.FieldError{Field: f.Field, Code: oapi.FieldErrorCode(f.Code), Message: f.Message}
	}
	noun := "fields"
	if len(fields) == 1 {
		noun = "field"
	}
	return oapi.Problem{
		Type:   problemValidation,
		Title:  "Validation failed",
		Status: http.StatusBadRequest,
		Detail: ptr(fmt.Sprintf("The request has %d invalid %s.", len(fields), noun)),
		Errors: &errs,
	}
}

func fieldProblem(field string, code domain.FieldErrorCode, message string) oapi.Problem {
	return validationProblem([]domain.FieldError{{Field: field, Code: code, Message: message}})
}

func badRequest(detail string) oapi.Problem {
	return oapi.Problem{Type: problemValidation, Title: "Validation failed", Status: http.StatusBadRequest, Detail: &detail}
}

func payloadTooLarge(limit int64) oapi.Problem {
	return oapi.Problem{
		Type:   problemPayloadTooLarge,
		Title:  "Request body too large",
		Status: http.StatusRequestEntityTooLarge,
		Detail: ptr(fmt.Sprintf("the request body exceeds %d bytes", limit)),
	}
}

// fieldPath turns a path of encoding/json, such as "notifications.3.content", into the form the API
// uses, "notifications[3].content".
func fieldPath(jsonPath string) string {
	var b strings.Builder
	for i, part := range strings.Split(jsonPath, ".") {
		switch {
		case isIndex(part):
			b.WriteString("[" + part + "]")
		case i > 0:
			b.WriteString("." + part)
		default:
			b.WriteString(part)
		}
	}
	return b.String()
}

func isIndex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// jsonKind names the JSON type that values of t are decoded from.
func jsonKind(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Bool:
		return "a boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "a number"
	case reflect.Slice, reflect.Array:
		return "an array"
	default:
		return "an object"
	}
}

func ptr[T any](v T) *T { return &v }
