// Package mockserver implements a fake notification provider for local runs, load tests and integration tests.
//
// It follows the provider contract: a POST with {"to","channel","content"} is answered with
// 202 {"messageId","status":"accepted","timestamp"}. Latency and the share of rejected (400),
// throttled (429) and failed (500) responses are configurable, so failure handling can be exercised
// deterministically.
package mockserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const maxBodyBytes = 1 << 20

// Config controls the simulated behaviour. Rates are probabilities in [0, 1] and must not sum above 1.
type Config struct {
	Latency      time.Duration
	Jitter       time.Duration
	RejectRate   float64
	ThrottleRate float64
	FailureRate  float64
	RetryAfter   time.Duration
}

func (c Config) validate() error {
	for name, r := range map[string]float64{"reject": c.RejectRate, "throttle": c.ThrottleRate, "failure": c.FailureRate} {
		if r < 0 || r > 1 {
			return fmt.Errorf("%s rate %v is outside [0, 1]", name, r)
		}
	}
	if c.RejectRate+c.ThrottleRate+c.FailureRate > 1 {
		return errors.New("reject, throttle and failure rates sum above 1")
	}
	if c.Latency < 0 || c.Jitter < 0 || c.RetryAfter < 0 {
		return errors.New("durations must not be negative")
	}
	return nil
}

// Server is an http.Handler that behaves like the external provider.
type Server struct {
	cfg      Config
	roll     func() float64
	now      func() time.Time
	newID    func() string
	requests *prometheus.CounterVec
}

// Option customises a Server, mainly for tests.
type Option func(*Server)

// WithRoll replaces the random source that decides each response's outcome.
func WithRoll(f func() float64) Option { return func(s *Server) { s.roll = f } }

// WithClock replaces the clock used for response timestamps.
func WithClock(f func() time.Time) Option { return func(s *Server) { s.now = f } }

// New validates cfg and returns a Server whose request counter is registered on reg.
func New(cfg Config, reg prometheus.Registerer, opts ...Option) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("mock provider config: %w", err)
	}
	s := &Server{
		cfg:   cfg,
		roll:  rand.Float64,
		now:   time.Now,
		newID: uuid.NewString,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mock_provider_requests_total",
			Help: "Requests received by the mock provider, by channel and simulated result.",
		}, []string{"channel", "result"}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := reg.Register(s.requests); err != nil {
		return nil, fmt.Errorf("registering mock provider metrics: %w", err)
	}
	return s, nil
}

type sendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type sendResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

type errorResponse struct {
	Error string `json:"error"`
}

var validChannels = map[string]bool{"sms": true, "email": true, "push": true}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
		return
	}

	var req sendRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		s.requests.WithLabelValues("unknown", "invalid").Inc()
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if req.To == "" || req.Content == "" || !validChannels[req.Channel] {
		s.requests.WithLabelValues("unknown", "invalid").Inc()
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "to, content and a valid channel are required"})
		return
	}

	if d := s.delay(); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-r.Context().Done():
			return
		}
	}

	switch roll := s.roll(); {
	case roll < s.cfg.RejectRate:
		s.requests.WithLabelValues(req.Channel, "rejected").Inc()
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "recipient rejected"})
	case roll < s.cfg.RejectRate+s.cfg.ThrottleRate:
		s.requests.WithLabelValues(req.Channel, "throttled").Inc()
		w.Header().Set("Retry-After", strconv.Itoa(int(s.cfg.RetryAfter.Seconds())))
		writeJSON(w, http.StatusTooManyRequests, errorResponse{Error: "rate limited"})
	case roll < s.cfg.RejectRate+s.cfg.ThrottleRate+s.cfg.FailureRate:
		s.requests.WithLabelValues(req.Channel, "failed").Inc()
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
	default:
		s.requests.WithLabelValues(req.Channel, "accepted").Inc()
		writeJSON(w, http.StatusAccepted, sendResponse{
			MessageID: s.newID(),
			Status:    "accepted",
			Timestamp: s.now().UTC().Format(time.RFC3339Nano),
		})
	}
}

func (s *Server) delay() time.Duration {
	d := s.cfg.Latency
	if s.cfg.Jitter > 0 {
		d += time.Duration(rand.Int64N(int64(s.cfg.Jitter))) //nolint:gosec // simulation only
	}
	return d
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
