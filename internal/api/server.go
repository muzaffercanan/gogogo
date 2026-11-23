package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/muzaffercanan/gogogo/internal/api/oapi"
	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/service"
)

// Request body limits. A single notification is at most about 600 KB of JSON (100 KB of content written
// as \u escapes); a batch of SMS or push notifications fits easily in 10 MiB, while a batch of large
// emails has to be split.
const (
	maxBodyBytes      = 1 << 20
	maxBatchBodyBytes = 10 << 20
	batchPath         = "/v1/notifications/batch"
)

// errNotImplemented answers operations that are not implemented yet.
var errNotImplemented = errors.New("operation not implemented")

// Server implements the operations of api/openapi.yaml on top of the service. Handlers translate
// between API and domain types and return errors as they are: ResponseErrorHandler maps them to
// problem+json in one place.
type Server struct {
	svc *service.Service
}

var _ oapi.StrictServerInterface = (*Server)(nil)

// NewHandler returns the HTTP handler of the API: the routes generated from the spec behind the
// middleware chain. Errors are answered as problem+json; metrics are registered with reg.
func NewHandler(svc *service.Service, logger *slog.Logger, reg prometheus.Registerer) http.Handler {
	strict := oapi.NewStrictHandlerWithOptions(&Server{svc: svc}, nil, oapi.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  RequestErrorHandler,
		ResponseErrorHandlerFunc: ResponseErrorHandler(logger),
	})
	mux := http.NewServeMux()
	oapi.HandlerWithOptions(strict, oapi.StdHTTPServerOptions{BaseRouter: mux, ErrorHandlerFunc: RequestErrorHandler})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		WriteProblem(w, r, ProblemFor(domain.ErrNotFound))
	})

	// CorrelationID replaces the request, so it goes outside the middleware that reads r.Pattern.
	var h http.Handler = mux
	h = Recover(logger)(h)
	h = NewMetrics(reg).Middleware(h)
	h = AccessLog(logger)(h)
	h = MaxBytes(bodyLimit)(h)
	return CorrelationID(h)
}

func bodyLimit(r *http.Request) int64 {
	if r.URL.Path == batchPath {
		return maxBatchBodyBytes
	}
	return maxBodyBytes
}

// CreateNotification implements POST /v1/notifications.
func (s *Server) CreateNotification(ctx context.Context, req oapi.CreateNotificationRequestObject) (oapi.CreateNotificationResponseObject, error) {
	n, replayed, err := s.svc.Create(ctx, toRequest(*req.Body), req.Params.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	return oapi.CreateNotification202JSONResponse{
		Body:    toNotification(n),
		Headers: oapi.CreateNotification202ResponseHeaders{Location: ptr(notificationPath(n)), IdempotentReplayed: &replayed},
	}, nil
}

// CreateNotificationBatch implements POST /v1/notifications/batch.
func (s *Server) CreateNotificationBatch(context.Context, oapi.CreateNotificationBatchRequestObject) (oapi.CreateNotificationBatchResponseObject, error) {
	return nil, errNotImplemented
}

// GetNotification implements GET /v1/notifications/{id}.
func (s *Server) GetNotification(context.Context, oapi.GetNotificationRequestObject) (oapi.GetNotificationResponseObject, error) {
	return nil, errNotImplemented
}

// ListNotifications implements GET /v1/notifications.
func (s *Server) ListNotifications(context.Context, oapi.ListNotificationsRequestObject) (oapi.ListNotificationsResponseObject, error) {
	return nil, errNotImplemented
}

// CancelNotification implements POST /v1/notifications/{id}/cancel.
func (s *Server) CancelNotification(context.Context, oapi.CancelNotificationRequestObject) (oapi.CancelNotificationResponseObject, error) {
	return nil, errNotImplemented
}

// GetBatch implements GET /v1/batches/{id}.
func (s *Server) GetBatch(context.Context, oapi.GetBatchRequestObject) (oapi.GetBatchResponseObject, error) {
	return nil, errNotImplemented
}

func toRequest(b oapi.NotificationCreate) domain.NotificationRequest {
	r := domain.NotificationRequest{
		Channel:     domain.Channel(b.Channel),
		Recipient:   b.Recipient,
		Content:     b.Content,
		ScheduledAt: b.ScheduledAt,
	}
	if b.Priority != nil {
		r.Priority = domain.Priority(*b.Priority)
	}
	return r
}

func toNotification(n domain.Notification) oapi.Notification {
	return oapi.Notification{
		Id:                n.ID,
		BatchId:           n.BatchID,
		Channel:           oapi.Channel(n.Channel),
		Priority:          oapi.Priority(n.Priority),
		Recipient:         n.Recipient,
		Content:           n.Content,
		Status:            oapi.NotificationStatus(n.Status),
		Attempts:          n.Attempts,
		MaxAttempts:       n.MaxAttempts,
		LastError:         nilIfEmpty(n.LastError),
		ProviderMessageId: nilIfEmpty(n.ProviderMessageID),
		CorrelationId:     nilIfEmpty(n.CorrelationID),
		ScheduledAt:       utc(n.ScheduledAt),
		CreatedAt:         n.CreatedAt.UTC(),
		UpdatedAt:         n.UpdatedAt.UTC(),
		SentAt:            utc(n.SentAt),
	}
}

func notificationPath(n domain.Notification) string {
	return "/v1/notifications/" + n.ID.String()
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// utc returns t in UTC, so responses do not depend on the server's time zone.
func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
