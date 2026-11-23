package domain

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ErrNotFound reports that a notification or batch does not exist.
var ErrNotFound = errors.New("not found")

// ErrInvalidTransition reports a status change that the state machine does not allow (see AllowedFrom).
// It is a bug in the caller, unlike a *StatusConflictError, which reports a lost race.
var ErrInvalidTransition = errors.New("invalid status transition")

// ErrIdempotencyKeyReused reports that an Idempotency-Key was already used with a different request.
var ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different request")

// StatusConflictError reports that a status change was refused because the notification is no longer in
// a status the change is allowed from, typically because another actor changed it first.
type StatusConflictError struct {
	ID      uuid.UUID
	Current Status
	Wanted  Status
}

func (e *StatusConflictError) Error() string {
	return fmt.Sprintf("notification %s is %s and cannot become %s", e.ID, e.Current, e.Wanted)
}

// FieldErrorCode is the machine-readable reason why a field is invalid. The values are part of the API
// contract (FieldError.code in api/openapi.yaml).
type FieldErrorCode string

// Field error codes.
const (
	CodeRequired       FieldErrorCode = "required"
	CodeInvalidValue   FieldErrorCode = "invalid_value"
	CodeInvalidFormat  FieldErrorCode = "invalid_format"
	CodeTooShort       FieldErrorCode = "too_short"
	CodeTooLong        FieldErrorCode = "too_long"
	CodeTooManyItems   FieldErrorCode = "too_many_items"
	CodeNotInFuture    FieldErrorCode = "not_in_future"
	CodeTooFarInFuture FieldErrorCode = "too_far_in_future"
)

// FieldError reports one invalid field of a request.
type FieldError struct {
	// Field is the path of the field, for example "recipient" or "notifications[3].content".
	Field   string
	Code    FieldErrorCode
	Message string
}

// ValidationError reports every invalid field of a request. It always lists at least one field.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "invalid request: " + strings.Join(parts, "; ")
}
