package api

import (
	"context"
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
	mux.HandleFunc("GET /docs", serveDocs)
	mux.HandleFunc("GET /openapi.yaml", serveSpec)
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
func (s *Server) CreateNotificationBatch(ctx context.Context, req oapi.CreateNotificationBatchRequestObject) (oapi.CreateNotificationBatchResponseObject, error) {
	items := make([]domain.NotificationRequest, len(req.Body.Notifications))
	for i, n := range req.Body.Notifications {
		items[i] = toRequest(n)
	}
	b, replayed, err := s.svc.CreateBatch(ctx, items, req.Params.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	return oapi.CreateNotificationBatch202JSONResponse{
		Body:    toBatchResult(b),
		Headers: oapi.CreateNotificationBatch202ResponseHeaders{Location: ptr("/v1/batches/" + b.ID.String()), IdempotentReplayed: &replayed},
	}, nil
}

func toBatchResult(b domain.Batch) oapi.BatchCreateResult {
	items := make([]oapi.BatchItemResult, len(b.Items))
	for i, it := range b.Items {
		items[i] = oapi.BatchItemResult{Index: it.Index, Result: oapi.Accepted, NotificationId: it.NotificationID}
		if it.NotificationID == nil {
			errs := toFieldErrors(it.Errors)
			items[i].Result, items[i].Errors = oapi.Rejected, &errs
		}
	}
	return oapi.BatchCreateResult{BatchId: b.ID, Total: b.Total, Accepted: b.Accepted, Rejected: b.Rejected, Items: items}
}

// GetNotification implements GET /v1/notifications/{id}.
func (s *Server) GetNotification(ctx context.Context, req oapi.GetNotificationRequestObject) (oapi.GetNotificationResponseObject, error) {
	n, err := s.svc.Get(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return oapi.GetNotification200JSONResponse(toNotification(n)), nil
}

// ListNotifications implements GET /v1/notifications.
func (s *Server) ListNotifications(ctx context.Context, req oapi.ListNotificationsRequestObject) (oapi.ListNotificationsResponseObject, error) {
	p := req.Params
	lr := service.ListRequest{Limit: p.Limit, Cursor: p.Cursor}
	if p.Status != nil {
		lr.Status = domain.Status(*p.Status)
	}
	if p.Channel != nil {
		lr.Channel = domain.Channel(*p.Channel)
	}
	if p.CreatedFrom != nil {
		lr.CreatedFrom = *p.CreatedFrom
	}
	if p.CreatedTo != nil {
		lr.CreatedTo = *p.CreatedTo
	}
	page, err := s.svc.List(ctx, lr)
	if err != nil {
		return nil, err
	}
	return oapi.ListNotifications200JSONResponse{Items: toNotifications(page.Items), NextCursor: nilIfEmpty(page.NextCursor)}, nil
}

func toNotifications(ns []domain.Notification) []oapi.Notification {
	items := make([]oapi.Notification, len(ns))
	for i, n := range ns {
		items[i] = toNotification(n)
	}
	return items
}

// CancelNotification implements POST /v1/notifications/{id}/cancel.
func (s *Server) CancelNotification(ctx context.Context, req oapi.CancelNotificationRequestObject) (oapi.CancelNotificationResponseObject, error) {
	n, err := s.svc.Cancel(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return oapi.CancelNotification200JSONResponse(toNotification(n)), nil
}

// GetBatch implements GET /v1/batches/{id}.
func (s *Server) GetBatch(ctx context.Context, req oapi.GetBatchRequestObject) (oapi.GetBatchResponseObject, error) {
	page, err := s.svc.GetBatch(ctx, req.Id, req.Params.Limit, req.Params.Cursor)
	if err != nil {
		return nil, err
	}
	b, c := page.Batch, page.Counts
	return oapi.GetBatch200JSONResponse{
		Id:        b.ID,
		Total:     b.Total,
		Accepted:  b.Accepted,
		Rejected:  b.Rejected,
		CreatedAt: b.CreatedAt.UTC(),
		StatusCounts: oapi.StatusCounts{
			Pending:           c[domain.StatusPending],
			Queued:            c[domain.StatusQueued],
			Sending:           c[domain.StatusSending],
			Sent:              c[domain.StatusSent],
			RetryScheduled:    c[domain.StatusRetryScheduled],
			PermanentlyFailed: c[domain.StatusPermanentlyFailed],
			Cancelled:         c[domain.StatusCancelled],
		},
		Items:      toNotifications(page.Items),
		NextCursor: nilIfEmpty(page.NextCursor),
	}, nil
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
