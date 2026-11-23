package domain

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ErrNotFound reports that a notification or batch does not exist.
var ErrNotFound = errors.New("not found")

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
