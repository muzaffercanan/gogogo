package domain

import (
	"time"

	"github.com/google/uuid"
)

// MaxBatchItems is the largest number of notifications one batch request may hold.
const MaxBatchItems = 1000

// BatchItemResult is the outcome of one item of a batch request.
type BatchItemResult struct {
	// Index is the position of the item in the request, from 0.
	Index int
	// NotificationID is the ID of the notification created for an accepted item, nil for a rejected one.
	NotificationID *uuid.UUID
	// Errors lists why a rejected item is invalid; it is empty for an accepted one.
	Errors []FieldError
}

// NewBatch is a validated batch request that is about to be stored.
type NewBatch struct {
	ID uuid.UUID
	// IdempotencyKey is the client's Idempotency-Key header, or "" if none was sent.
	IdempotencyKey string
	// RequestHash identifies the request body that IdempotencyKey was first used with.
	RequestHash []byte
	// Items holds the outcome of every item, in request order.
	Items []BatchItemResult
	// Notifications holds the accepted items. Their BatchID is ignored: they belong to this batch.
	Notifications []NewNotification
}

// Batch is a stored batch request.
type Batch struct {
	ID             uuid.UUID
	IdempotencyKey string
	RequestHash    []byte
	Total          int
	Accepted       int
	Rejected       int
	// Items holds the outcome of every item, in request order.
	Items     []BatchItemResult
	CreatedAt time.Time
}
