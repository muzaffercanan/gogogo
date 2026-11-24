package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/platform/backoff"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/platform/tracing"
	"github.com/muzaffercanan/gogogo/internal/provider"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// Store is the part of the repository a delivery writes through. *repository.Store is one.
type Store interface {
	TransitionNotification(ctx context.Context, t repository.Transition) (domain.Notification, error)
	ScheduleRetry(ctx context.Context, r repository.Retry) (domain.Notification, error)
}

// Sender sends a notification to the provider. *provider.Client is one.
type Sender interface {
	Send(ctx context.Context, m provider.Message) (provider.Accepted, error)
}

// DeadLetters takes the deliveries that failed for good. *queue.Publisher is one.
type DeadLetters interface {
	PublishDeadLetter(ctx context.Context, m queue.Message) error
}

// Outcome is how a delivery ended.
type Outcome string

// The outcomes of Deliver.
const (
	// OutcomeSent: the provider accepted the notification and it is now sent.
	OutcomeSent Outcome = "sent"
	// OutcomeRetry: the attempt failed with a retryable error; the notification is retry_scheduled.
	OutcomeRetry Outcome = "retry_scheduled"
	// OutcomeFailed: the provider refused the notification for good; it is permanently_failed and
	// went to the dead-letter queue.
	OutcomeFailed Outcome = "permanently_failed"
	// OutcomeExhausted: the last attempt failed with a retryable error; the notification is
	// permanently_failed and went to the dead-letter queue.
	OutcomeExhausted Outcome = "attempts_exhausted"
	// OutcomeSkipped: the notification was not queued (cancelled, or a duplicate message), so nothing
	// was sent.
	OutcomeSkipped Outcome = "skipped"
	// OutcomeRejected: the message names no notification; it went to the dead-letter queue.
	OutcomeRejected Outcome = "rejected"
	// OutcomeAbandoned: the outcome could not be recorded, or the worker stopped before sending. The
	// broker (not yet sending) or the reaper (sending) takes it from there.
	OutcomeAbandoned Outcome = "abandoned"
)

// Reasons recorded on the status events a delivery writes.
const (
	reasonAccepted  = "provider accepted"
	reasonRetryable = "retryable failure"
	reasonPermanent = "permanent failure"
	reasonExhausted = "attempts exhausted"
)

// Bounds for writing a delivery's outcome while PostgreSQL is unreachable. The total stays well below
// the reaper's 60 s sending lease, so the reaper does not resend a delivery that is still being recorded.
const (
	writeMinBackoff = 100 * time.Millisecond
	writeMaxBackoff = 5 * time.Second
	outcomeTimeout  = 30 * time.Second
	deadLetterWait  = 10 * time.Second
)

// Deliverer delivers one notification: it moves it from queued to sending, acknowledges the message,
// calls the provider and records the outcome (ADR 0007, ADR 0015). It is safe for concurrent use.
type Deliverer struct {
	store       Store
	sender      Sender
	deadLetters DeadLetters
	retry       RetryPolicy
	metrics     *Metrics
	log         *slog.Logger
}

// NewDeliverer returns a deliverer that writes through store, sends through sender, dead-letters
// through deadLetters, spaces retries with retry and reports to metrics.
func NewDeliverer(store Store, sender Sender, deadLetters DeadLetters, retry RetryPolicy, metrics *Metrics, log *slog.Logger) *Deliverer {
	return &Deliverer{store: store, sender: sender, deadLetters: deadLetters, retry: retry, metrics: metrics, log: log}
}

// Deliver delivers the notification of d. Until it has moved the notification to sending it gives up
// when ctx ends, leaving d unacknowledged for the broker to hand out again. From then on it finishes
// even if ctx ends: it has acknowledged d, and stopping halfway would leave the outcome to the reaper.
//
// The span of ctx is the delivery's: a retry continues its trace, and so does the dead-letter message.
func (dl *Deliverer) Deliver(ctx context.Context, d queue.Delivery) Outcome {
	ctx = logging.WithCorrelationID(ctx, d.CorrelationID)
	log := dl.log.With("notification_id", d.NotificationID, "channel", string(d.Channel), "priority", string(d.Priority))

	id, err := uuid.Parse(d.NotificationID)
	if err != nil {
		log.WarnContext(ctx, "rejecting a message that names no notification", "error", err)
		dl.settle(ctx, log, d.Reject())
		dl.metrics.skippedDelivery(d.Channel, skipInvalidMessage)
		return OutcomeRejected
	}

	var n domain.Notification
	err = dl.write(ctx, 0, func(ctx context.Context) error {
		var err error
		n, err = dl.store.TransitionNotification(ctx, repository.Transition{
			ID: id, From: []domain.Status{domain.StatusQueued}, To: domain.StatusSending,
		})
		return err
	})
	if final(err) {
		// Cancelled, sent already (a duplicate message), or unknown: nothing to send.
		dl.settle(ctx, log, d.Ack())
		dl.metrics.skippedDelivery(d.Channel, skipNotQueued)
		log.InfoContext(ctx, "skipped a notification that is not queued", "reason", err.Error())
		return OutcomeSkipped
	}
	if err != nil {
		log.WarnContext(ctx, "could not start the delivery; the message goes back to its queue", "error", err)
		dl.metrics.skippedDelivery(d.Channel, skipNotStarted)
		return OutcomeAbandoned
	}
	// The notification is sending now, so a redelivery of the message would be skipped, and the
	// reaper recovers the delivery if this process dies (ADR 0015).
	dl.settle(ctx, log, d.Ack())

	ctx = context.WithoutCancel(ctx)
	start := time.Now()
	accepted, sendErr := dl.sender.Send(ctx, provider.Message{
		NotificationID: n.ID.String(),
		Channel:        n.Channel,
		To:             n.Recipient,
		Content:        n.Content,
		CorrelationID:  n.CorrelationID,
	})
	dl.metrics.providerCall(n.Channel, time.Since(start))
	if sendErr == nil {
		return dl.sent(ctx, log, n, accepted)
	}
	var pe *provider.Error
	if !errors.As(sendErr, &pe) {
		// Not an answer from the provider (the request could not be built): retry like a transient
		// failure.
		pe = &provider.Error{Retryable: true, Reason: sendErr.Error()}
	}
	if pe.Retryable && n.Attempts < n.MaxAttempts {
		return dl.retryLater(ctx, log, n, pe)
	}
	return dl.fail(ctx, log, d, n, pe)
}

func (dl *Deliverer) sent(ctx context.Context, log *slog.Logger, n domain.Notification, a provider.Accepted) Outcome {
	var row domain.Notification
	if err := dl.write(ctx, outcomeTimeout, func(ctx context.Context) error {
		var err error
		row, err = dl.store.TransitionNotification(ctx, repository.Transition{
			ID: n.ID, From: []domain.Status{domain.StatusSending}, To: domain.StatusSent,
			ProviderMessageID: a.MessageID, Reason: reasonAccepted,
		})
		return err
	}); err != nil {
		return dl.abandon(ctx, log, n.Channel, "sent", err)
	}
	dl.metrics.delivered(row, a.Malformed)
	if a.Malformed {
		log.WarnContext(ctx, "notification sent; the provider's answer had no message id", "status", a.StatusCode, "attempt", n.Attempts)
	} else {
		log.InfoContext(ctx, "notification sent", "provider_message_id", a.MessageID, "attempt", n.Attempts)
	}
	return OutcomeSent
}

func (dl *Deliverer) retryLater(ctx context.Context, log *slog.Logger, n domain.Notification, pe *provider.Error) Outcome {
	delay := dl.retry.Delay(n.Attempts, pe.RetryAfter)
	if err := dl.write(ctx, outcomeTimeout, func(ctx context.Context) error {
		_, err := dl.store.ScheduleRetry(ctx, repository.Retry{
			ID: n.ID, Delay: delay, LastError: pe.Reason, Reason: reasonRetryable, TraceParent: tracing.TraceParent(ctx),
		})
		return err
	}); err != nil {
		return dl.abandon(ctx, log, n.Channel, "retry_scheduled", err)
	}
	dl.metrics.retried(n.Channel, pe)
	log.InfoContext(ctx, "delivery failed; retry scheduled", "error", pe.Reason, "attempt", n.Attempts,
		"max_attempts", n.MaxAttempts, "retry_in_ms", delay.Milliseconds())
	return OutcomeRetry
}

func (dl *Deliverer) fail(ctx context.Context, log *slog.Logger, d queue.Delivery, n domain.Notification, pe *provider.Error) Outcome {
	reason, outcome := reasonPermanent, OutcomeFailed
	if pe.Retryable {
		reason, outcome = reasonExhausted, OutcomeExhausted
	}
	if err := dl.write(ctx, outcomeTimeout, func(ctx context.Context) error {
		_, err := dl.store.TransitionNotification(ctx, repository.Transition{
			ID: n.ID, From: []domain.Status{domain.StatusSending}, To: domain.StatusPermanentlyFailed,
			LastError: pe.Reason, Reason: reason,
		})
		return err
	}); err != nil {
		return dl.abandon(ctx, log, n.Channel, "permanently_failed", err)
	}
	dl.metrics.failedForGood(n.Channel)
	log.WarnContext(ctx, "delivery failed for good", "error", pe.Reason, "reason", reason, "attempt", n.Attempts,
		"max_attempts", n.MaxAttempts)

	// PostgreSQL holds the outcome; the dead-letter queue is the operational view of it (ADR 0009), so
	// a failed publish is only logged.
	headers := amqp.Table{"failure_reason": pe.Reason, "attempts": strconv.Itoa(n.Attempts)}
	maps.Copy(headers, queue.Headers(d.CorrelationID, tracing.TraceParent(ctx)))
	pubCtx, cancel := context.WithTimeout(ctx, deadLetterWait)
	defer cancel()
	if err := dl.deadLetters.PublishDeadLetter(pubCtx, queue.Message{
		Channel: n.Channel, Priority: n.Priority, ID: n.ID.String(), Body: d.Body, Headers: headers,
	}); err != nil {
		log.WarnContext(ctx, "could not publish the failed delivery to the dead-letter queue", "error", err)
	}
	return outcome
}

func (dl *Deliverer) abandon(ctx context.Context, log *slog.Logger, c domain.Channel, status string, err error) Outcome {
	dl.metrics.outcomeNotRecorded(c)
	if final(err) {
		// Someone else moved the notification on (the reaper, after its lease expired).
		log.WarnContext(ctx, "could not record the outcome: the notification is no longer sending", "outcome", status, "error", err)
	} else {
		log.ErrorContext(ctx, "could not record the outcome; the reaper will retry the delivery", "outcome", status, "error", err)
	}
	return OutcomeAbandoned
}

// settle logs a failed ack or reject. A delivery whose channel closed is handed out again by the broker,
// and its notification is no longer queued, so the redelivery is skipped.
func (dl *Deliverer) settle(ctx context.Context, log *slog.Logger, err error) {
	if err != nil {
		log.DebugContext(ctx, "could not settle the message", "error", err)
	}
}

// write runs fn until it succeeds or fails for a reason retrying cannot change, retrying other errors
// (PostgreSQL unreachable) with backoff. It gives up when ctx ends, and after limit if limit is positive.
func (dl *Deliverer) write(ctx context.Context, limit time.Duration, fn func(context.Context) error) error {
	if limit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limit)
		defer cancel()
	}
	for failures := 0; ; failures++ {
		err := fn(ctx)
		if err == nil || final(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (gave up: %w)", err, ctx.Err())
		case <-time.After(backoff.FullJitter(failures, writeMinBackoff, writeMaxBackoff)):
		}
	}
}

// final reports whether a write failed for a reason retrying cannot change: the notification is in
// another status, does not exist, or the transition is not allowed.
func final(err error) bool {
	var conflict *domain.StatusConflictError
	return errors.As(err, &conflict) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalidTransition)
}
