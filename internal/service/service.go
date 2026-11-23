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

// CreateBatch validates each of items on its own and stores the valid ones as pending notifications of
// one batch, in one transaction. Invalid items are rejected with their field errors; the batch is
// stored even if every item is rejected. It returns a *domain.ValidationError if items is empty or holds
// more than domain.MaxBatchItems.
//
// key works as for Create and covers the whole batch: a replay returns the stored batch, whose item
// results are those of the first request.
func (s *Service) CreateBatch(ctx context.Context, items []domain.NotificationRequest, key *string) (b domain.Batch, replayed bool, err error) {
	switch {
	case len(items) == 0:
		return domain.Batch{}, false, fieldError("notifications", domain.CodeTooShort, "must hold at least 1 notification")
	case len(items) > domain.MaxBatchItems:
		return domain.Batch{}, false, fieldError("notifications", domain.CodeTooManyItems,
			fmt.Sprintf("must hold at most %d notifications", domain.MaxBatchItems))
	}

	var hash []byte
	if key != nil {
		if err := validateIdempotencyKey(*key); err != nil {
			return domain.Batch{}, false, err
		}
		hash = domain.BatchHash(items)
		existing, err := s.store.GetBatchByIdempotencyKey(ctx, *key)
		switch {
		case err == nil:
			return replayBatch(existing, hash)
		case !errors.Is(err, domain.ErrNotFound):
			return domain.Batch{}, false, err
		}
	}

	id, err := uuid.NewV7()
	if err != nil {
		return domain.Batch{}, false, fmt.Errorf("generating batch ID: %w", err)
	}
	nb := domain.NewBatch{ID: id, Items: make([]domain.BatchItemResult, len(items))}
	if key != nil {
		nb.IdempotencyKey, nb.RequestHash = *key, hash
	}
	now := s.now()
	for i, item := range items {
		nb.Items[i].Index = i
		if err := item.Validate(now); err != nil {
			var invalid *domain.ValidationError
			if !errors.As(err, &invalid) {
				return domain.Batch{}, false, err
			}
			nb.Items[i].Errors = invalid.Fields
			continue
		}
		n, err := s.newNotification(ctx, item)
		if err != nil {
			return domain.Batch{}, false, err
		}
		n.BatchID = &id
		nb.Items[i].NotificationID = &n.ID
		nb.Notifications = append(nb.Notifications, n)
	}

	stored, created, err := s.store.CreateBatch(ctx, nb)
	if err != nil {
		return domain.Batch{}, false, fmt.Errorf("storing batch: %w", err)
	}
	if !created {
		return replayBatch(stored, hash)
	}
	return stored, false, nil
}

func replayBatch(existing domain.Batch, hash []byte) (domain.Batch, bool, error) {
	if !bytes.Equal(existing.RequestHash, hash) {
		return domain.Batch{}, false, fmt.Errorf("batch %s: %w", existing.ID, domain.ErrIdempotencyKeyReused)
	}
	return existing, true, nil
}

func fieldError(field string, code domain.FieldErrorCode, message string) error {
	return &domain.ValidationError{Fields: []domain.FieldError{{Field: field, Code: code, Message: message}}}
}

// Get returns the notification with the given ID, or an error wrapping domain.ErrNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.store.GetNotification(ctx, id)
}

// Page sizes of list endpoints.
const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// ListRequest selects one page of notifications as a client asked for it. Zero values and nil mean
// "no filter" or "the default".
type ListRequest struct {
	Status  domain.Status
	Channel domain.Channel
	// CreatedFrom is inclusive; CreatedTo is exclusive.
	CreatedFrom time.Time
	CreatedTo   time.Time
	Limit       *int
	// Cursor is the NextCursor of the previous page.
	Cursor *string
}

// Page is one page of a list. NextCursor continues it; it is "" on the last page.
type Page struct {
	Items      []domain.Notification
	NextCursor string
}

// List returns one page of the notifications that match req, newest first. It returns a
// *domain.ValidationError listing every invalid parameter.
func (s *Service) List(ctx context.Context, req ListRequest) (Page, error) {
	var errs []domain.FieldError
	if req.Status != "" && !req.Status.Valid() {
		errs = append(errs, domain.FieldError{Field: "status", Code: domain.CodeInvalidValue,
			Message: "must be one of pending, queued, sending, sent, retry_scheduled, permanently_failed, cancelled"})
	}
	if req.Channel != "" && !req.Channel.Valid() {
		errs = append(errs, domain.FieldError{Field: "channel", Code: domain.CodeInvalidValue, Message: "must be one of sms, email, push"})
	}
	limit, after, pageErrs := pageParams(req.Limit, req.Cursor)
	errs = append(errs, pageErrs...)
	if len(errs) > 0 {
		return Page{}, &domain.ValidationError{Fields: errs}
	}

	page, err := s.store.ListNotifications(ctx, repository.ListFilter{
		Status: req.Status, Channel: req.Channel, CreatedFrom: req.CreatedFrom, CreatedTo: req.CreatedTo,
		After: after, Limit: limit,
	})
	if err != nil {
		return Page{}, err
	}
	return Page{Items: page.Items, NextCursor: encodeCursor(page.Next)}, nil
}

// pageParams checks the limit and cursor of a list request and returns the page size and position.
func pageParams(limit *int, cursor *string) (int, *repository.Cursor, []domain.FieldError) {
	var errs []domain.FieldError
	size := defaultPageSize
	if limit != nil {
		if *limit < 1 || *limit > maxPageSize {
			errs = append(errs, domain.FieldError{Field: "limit", Code: domain.CodeInvalidValue,
				Message: fmt.Sprintf("must be between 1 and %d", maxPageSize)})
		}
		size = *limit
	}
	var after *repository.Cursor
	if cursor != nil {
		c, err := repository.DecodeCursor(*cursor)
		if err != nil {
			errs = append(errs, domain.FieldError{Field: "cursor", Code: domain.CodeInvalidValue,
				Message: "must be the next_cursor of a previous page"})
		}
		after = &c
	}
	return size, after, errs
}

func encodeCursor(c *repository.Cursor) string {
	if c == nil {
		return ""
	}
	return c.Encode()
}

// BatchPage is a batch with the number of its notifications in each status and one page of them,
// oldest first. NextCursor continues the page; it is "" on the last page.
type BatchPage struct {
	Batch      domain.Batch
	Counts     map[domain.Status]int
	Items      []domain.Notification
	NextCursor string
}

// GetBatch returns batch id with its status counts and one page of its notifications. limit and cursor
// work as for List. It returns an error wrapping domain.ErrNotFound if the batch does not exist.
func (s *Service) GetBatch(ctx context.Context, id uuid.UUID, limit *int, cursor *string) (BatchPage, error) {
	size, after, errs := pageParams(limit, cursor)
	if len(errs) > 0 {
		return BatchPage{}, &domain.ValidationError{Fields: errs}
	}
	page, err := s.store.GetBatchPage(ctx, id, after, size)
	if err != nil {
		return BatchPage{}, err
	}
	return BatchPage{Batch: page.Batch, Counts: page.Counts, Items: page.Items, NextCursor: encodeCursor(page.Next)}, nil
}

// Cancel cancels notification id if it has not been handed to the provider yet (it is pending, queued
// or retry_scheduled), and returns it. Cancelling a cancelled notification succeeds and returns it
// unchanged. Otherwise it returns a *domain.StatusConflictError, or an error wrapping
// domain.ErrNotFound if the notification does not exist.
func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := s.store.TransitionNotification(ctx, repository.Transition{
		ID:     id,
		From:   domain.AllowedFrom(domain.StatusCancelled),
		To:     domain.StatusCancelled,
		Reason: "cancelled by client",
	})
	var conflict *domain.StatusConflictError
	if errors.As(err, &conflict) && conflict.Current == domain.StatusCancelled {
		// Cancelled is terminal, so the notification cannot have changed since.
		return s.store.GetNotification(ctx, id)
	}
	return n, err
}
