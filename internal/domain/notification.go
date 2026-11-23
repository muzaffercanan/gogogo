// Package domain holds the notification model shared by the API, the relay and the worker.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Channel is a delivery channel.
type Channel string

// Delivery channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Priority orders notifications within a channel.
type Priority string

// Priorities, highest first.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// DefaultPriority is the priority of a notification whose request did not choose one.
const DefaultPriority = PriorityNormal

// Status is the delivery status of a notification.
type Status string

// Delivery statuses. See ADR 0007 for the allowed transitions.
const (
	StatusPending           Status = "pending"
	StatusQueued            Status = "queued"
	StatusSending           Status = "sending"
	StatusSent              Status = "sent"
	StatusRetryScheduled    Status = "retry_scheduled"
	StatusPermanentlyFailed Status = "permanently_failed"
	StatusCancelled         Status = "cancelled"
)

// NewNotification is a validated notification that is about to be stored.
type NewNotification struct {
	ID      uuid.UUID
	BatchID *uuid.UUID
	// IdempotencyKey is the client's Idempotency-Key header, or "" if none was sent.
	IdempotencyKey string
	// RequestHash identifies the request body that IdempotencyKey was first used with.
	RequestHash   []byte
	Channel       Channel
	Priority      Priority
	Recipient     string
	Content       string
	MaxAttempts   int
	CorrelationID string
	// ScheduledAt delays delivery until the given time; nil means as soon as possible.
	ScheduledAt *time.Time
}

// Notification is a stored notification and its delivery state.
// Optional text fields are "" when unset.
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	IdempotencyKey    string
	RequestHash       []byte
	Channel           Channel
	Priority          Priority
	Recipient         string
	Content           string
	Status            Status
	Attempts          int
	MaxAttempts       int
	LastError         string
	ProviderMessageID string
	CorrelationID     string
	ScheduledAt       *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
}

// Valid reports whether c is one of the delivery channels.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Valid reports whether s is one of the delivery statuses.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusQueued, StatusSending, StatusSent, StatusRetryScheduled, StatusPermanentlyFailed, StatusCancelled:
		return true
	}
	return false
}
