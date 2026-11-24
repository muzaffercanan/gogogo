package worker

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/provider"
)

// sendTo returns the *provider.Error a real client gets from a provider at url.
func sendTo(t *testing.T, url string, timeout time.Duration) *provider.Error {
	t.Helper()
	c, err := provider.NewClient(url, timeout, noop.NewTracerProvider())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Send(t.Context(), provider.Message{NotificationID: "n1", Channel: domain.ChannelSMS, To: "+905551112233", Content: "hi"})
	var pe *provider.Error
	if !errors.As(err, &pe) {
		t.Fatalf("Send = %v, want a *provider.Error", err)
	}
	return pe
}

func answeringStatus(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestRetryReasonNamesTheKindOfFailure(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
	}))
	defer slow.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()

	tests := []struct {
		name string
		err  *provider.Error
		want string
	}{
		{name: "500", err: sendTo(t, answeringStatus(t, 500), time.Second), want: "5xx"},
		{name: "503", err: sendTo(t, answeringStatus(t, 503), time.Second), want: "5xx"},
		{name: "429", err: sendTo(t, answeringStatus(t, 429), time.Second), want: "429"},
		{name: "408", err: sendTo(t, answeringStatus(t, 408), time.Second), want: "408"},
		{name: "no answer in time", err: sendTo(t, slow.URL, 50*time.Millisecond), want: "timeout"},
		{name: "connection refused", err: sendTo(t, closedURL, time.Second), want: "connection"},
		{name: "not from the provider", err: &provider.Error{Retryable: true, Reason: "encoding failed"}, want: "other"},
	}
	for _, tt := range tests {
		if got := retryReason(tt.err); got != tt.want {
			t.Errorf("%s: retryReason = %q, want %q (error %v)", tt.name, got, tt.want, tt.err)
		}
	}
}
