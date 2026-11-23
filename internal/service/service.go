// Package service implements the use cases of the notification API on domain types: it validates
// requests, applies the idempotency and cancellation rules and stores the results. It knows nothing
// about HTTP.
package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

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

// maxIdempotencyKeyLen is the longest Idempotency-Key the API accepts, in characters.
const maxIdempotencyKeyLen = 255

// Create validates req and stores it as a pending notification, ready for dispatch. The notification
// carries the correlation ID of ctx. It returns a *domain.ValidationError if req is invalid.
//
// key is the request's Idempotency-Key, or nil if it sent none. If a notification was already created
// with key, Create stores nothing: it returns that notification in its current state with replayed set
// if req asks for the same notification (see domain.NotificationRequest.Hash), and
// domain.ErrIdempotencyKeyReused otherwise. The lookup comes before validation, so a replay succeeds
// even when the original request would no longer be valid, for example because its scheduled_at has
// passed. Concurrent requests with the same key create one notification; the others replay it.
func (s *Service) Create(ctx context.Context, req domain.NotificationRequest, key *string) (n domain.Notification, replayed bool, err error) {
	var hash []byte
	if key != nil {
		if err := validateIdempotencyKey(*key); err != nil {
			return domain.Notification{}, false, err
		}
		hash = req.Hash()
		existing, err := s.store.GetNotificationByIdempotencyKey(ctx, *key)
		switch {
		case err == nil:
			return replay(existing, hash)
		case !errors.Is(err, domain.ErrNotFound):
			return domain.Notification{}, false, err
		}
	}

	if err := req.Validate(s.now()); err != nil {
		return domain.Notification{}, false, err
	}
	nn, err := s.newNotification(ctx, req)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if key != nil {
		nn.IdempotencyKey, nn.RequestHash = *key, hash
	}
	stored, created, err := s.store.CreateNotification(ctx, nn)
	if err != nil {
		return domain.Notification{}, false, fmt.Errorf("storing notification: %w", err)
	}
	if !created {
		// A concurrent request with the same key committed between the lookup and the insert.
		return replay(stored, hash)
	}
	return stored, false, nil
}

// replay answers a request whose idempotency key already created existing.
func replay(existing domain.Notification, hash []byte) (domain.Notification, bool, error) {
	if !bytes.Equal(existing.RequestHash, hash) {
		return domain.Notification{}, false, fmt.Errorf("notification %s: %w", existing.ID, domain.ErrIdempotencyKeyReused)
	}
	return existing, true, nil
}

func validateIdempotencyKey(key string) error {
	var code domain.FieldErrorCode
	var msg string
	switch n := utf8.RuneCountInString(key); {
	case n == 0:
		code, msg = domain.CodeTooShort, "must not be empty"
	case n > maxIdempotencyKeyLen:
		code, msg = domain.CodeTooLong, fmt.Sprintf("must be at most %d characters", maxIdempotencyKeyLen)
	default:
		return nil
	}
	return &domain.ValidationError{Fields: []domain.FieldError{{Field: "Idempotency-Key", Code: code, Message: msg}}}
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
