package api

import (
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/muzaffercanan/gogogo/internal/platform/logging"
)

// CorrelationHeader carries the correlation ID of a request and of its response.
const CorrelationHeader = "X-Correlation-ID"

// maxCorrelationIDLen bounds a client-supplied correlation ID.
const maxCorrelationIDLen = 128

// CorrelationID propagates the request's X-Correlation-ID if it is valid (see validCorrelationID) and
// generates one otherwise. The ID goes into the request context, where logging.CorrelationID finds it,
// and into the response header.
//
// It replaces the request with one that carries the new context, so it must wrap middleware that reads
// the request after the router has filled in r.Pattern (AccessLog, Metrics).
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationHeader)
		if !validCorrelationID(id) {
			id = newCorrelationID()
		}
		w.Header().Set(CorrelationHeader, id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelationID(r.Context(), id)))
	})
}

// validCorrelationID reports whether id has 1 to 128 characters, each a letter, a digit or one of
// ". _ : -". UUIDs and trace IDs fit; anything else could bloat logs, rows and AMQP headers.
func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDLen {
		return false
	}
	for i := range len(id) {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == ':', c == '-':
		default:
			return false
		}
	}
	return true
}

func newCorrelationID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// Only possible if the system's random source fails; the time still tells requests apart.
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return id.String()
}

// MaxBytes limits the request body to limit(r) bytes. Reading past the limit fails with an
// *http.MaxBytesError, which RequestProblem answers with 413.
func MaxBytes(limit func(*http.Request) int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit(r))
			next.ServeHTTP(w, r)
		})
	}
}

// Recover answers a request whose handler panicked with a 500 problem, unless the response has already
// started, and logs the panic with its stack. http.ErrAbortHandler is re-raised, as net/http expects.
func Recover(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := wrap(w)
			defer answerPanic(logger, rec, r)
			next.ServeHTTP(rec, r)
		})
	}
}

// answerPanic is deferred by Recover; recover only works in the deferred function itself.
func answerPanic(logger *slog.Logger, rec *recorder, r *http.Request) {
	v := recover()
	if v == nil {
		return
	}
	if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		panic(v)
	}
	logger.ErrorContext(r.Context(), "handler panicked",
		"method", r.Method, "path", r.URL.Path, "panic", v, "stack", string(debug.Stack()))
	if !rec.wroteHeader {
		WriteProblem(rec, r, ProblemFor(errors.New("panic")))
	}
}

// AccessLog logs every request once it has been answered.
func AccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := wrap(w)
			next.ServeHTTP(rec, r)
			logger.InfoContext(r.Context(), "request",
				"method", r.Method, "route", route(r), "path", r.URL.Path, "status", rec.status,
				"bytes", rec.bytes, "duration_ms", float64(time.Since(start).Microseconds())/1000)
		})
	}
}

// Metrics counts requests and measures their duration per route.
type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewMetrics registers the HTTP metrics with reg:
//   - http_requests_total{method, route, status}
//   - http_request_duration_seconds{method, route}
//
// route is the pattern that matched, such as "POST /v1/notifications", never the raw path, so the
// number of series stays bounded.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests answered, by method, route and status code.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Time to answer an HTTP request, by method and route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// Middleware records the metrics of every request.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := wrap(w)
		next.ServeHTTP(rec, r)
		rt := route(r)
		m.requests.WithLabelValues(r.Method, rt, strconv.Itoa(rec.status)).Inc()
		m.duration.WithLabelValues(r.Method, rt).Observe(time.Since(start).Seconds())
	})
}

// route returns the pattern the router matched for r, or "unmatched". It is only known once the router
// has run, and only on the request value the router received.
func route(r *http.Request) string {
	if r.Pattern == "" {
		return "unmatched"
	}
	return r.Pattern
}

// recorder remembers the status code and body size of a response.
type recorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

// wrap returns w as a *recorder, reusing w if it already is one.
func wrap(w http.ResponseWriter) *recorder {
	if rec, ok := w.(*recorder); ok {
		return rec
	}
	return &recorder{ResponseWriter: w, status: http.StatusOK}
}

func (r *recorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
