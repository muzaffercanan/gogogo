// Package service implements the use cases of the notification API on domain types: it validates
// requests, applies the idempotency and cancellation rules and stores the results. It knows nothing
// about HTTP.
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// Service runs the notification use cases.
type Service struct {
	store       *repository.Store
	maxAttempts int
	now         func() time.Time
}

// New returns a Service that stores notifications in store, gives each one maxAttempts delivery
// attempts and reads the current time from now.
func New(store *repository.Store, maxAttempts int, now func() time.Time) (*Service, error) {
	if maxAttempts < 1 {
		return nil, fmt.Errorf("max attempts must be at least 1, got %d", maxAttempts)
	}
	return &Service{store: store, maxAttempts: maxAttempts, now: now}, nil
}

// Create validates req and stores it as a pending notification, ready for dispatch. The notification
// carries the correlation ID of ctx. It returns a *domain.ValidationError if req is invalid.
func (s *Service) Create(ctx context.Context, req domain.NotificationRequest) (domain.Notification, error) {
	if err := req.Validate(s.now()); err != nil {
		return domain.Notification{}, err
	}
	n, err := s.newNotification(ctx, req)
	if err != nil {
		return domain.Notification{}, err
	}
	stored, _, err := s.store.CreateNotification(ctx, n)
	if err != nil {
		return domain.Notification{}, fmt.Errorf("storing notification: %w", err)
	}
	return stored, nil
}

// newNotification turns a valid request into a notification to store, with a new time-ordered ID.
func (s *Service) newNotification(ctx context.Context, req domain.NotificationRequest) (domain.NewNotification, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return domain.NewNotification{}, fmt.Errorf("generating notification ID: %w", err)
	}
	priority := req.Priority
	if priority == "" {
		priority = domain.DefaultPriority
	}
	return domain.NewNotification{
		ID:            id,
		Channel:       req.Channel,
		Priority:      priority,
		Recipient:     req.Recipient,
		Content:       req.Content,
		MaxAttempts:   s.maxAttempts,
		CorrelationID: logging.CorrelationID(ctx),
		ScheduledAt:   req.ScheduledAt,
	}, nil
}
