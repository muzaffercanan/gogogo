//go:build integration

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/muzaffercanan/gogogo/internal/api"
	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/provider"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/ratelimit"
	"github.com/muzaffercanan/gogogo/internal/repository"
	"github.com/muzaffercanan/gogogo/internal/service"
	"github.com/muzaffercanan/gogogo/internal/testutil/pgtest"
	"github.com/muzaffercanan/gogogo/internal/testutil/rabbittest"
	"github.com/muzaffercanan/gogogo/internal/testutil/redistest"
	"github.com/muzaffercanan/gogogo/internal/testutil/tcretry"
	"github.com/muzaffercanan/gogogo/internal/worker"
)

var (
	pg  *pgtest.Server
	mq  *rabbittest.Server
	rdb *redistest.Server
)

func TestMain(m *testing.M) { os.Exit(testMain(m)) }

func testMain(m *testing.M) int {
	ctx := context.Background()
	var err error
	if pg, err = tcretry.Start(ctx, pgtest.Start); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		return 1
	}
	defer func() { _ = pg.Close() }()
	if mq, err = tcretry.Start(ctx, rabbittest.Start); err != nil {
		fmt.Fprintf(os.Stderr, "rabbittest: %v\n", err)
		return 1
	}
	defer func() { _ = mq.Close() }()
	if rdb, err = tcretry.Start(ctx, redistest.Start); err != nil {
		fmt.Fprintf(os.Stderr, "redistest: %v\n", err)
		return 1
	}
	defer func() { _ = rdb.Close() }()
	return m.Run()
}

// maxAttempts is the API's MAX_ATTEMPTS in these tests.
const maxAttempts = 5

// testRetryPolicy keeps retries quick: at most 100 ms after the first attempt, 400 ms later on.
var testRetryPolicy = worker.RetryPolicy{Base: 100 * time.Millisecond, Cap: 400 * time.Millisecond, MaxRetryAfter: time.Second}

// system is the service assembled as the api, relay and worker processes assemble it, each part with a
// broker connection of its own, on a database, virtual host and Redis database of the test's own.
type system struct {
	t        *testing.T
	db       *pgxpool.Pool
	store    *repository.Store
	vhost    string
	redisURL string
	api      *httptest.Server
	provider *scriptedProvider
	reaper   *outbox.Reaper
	log      *slog.Logger
}

// newSystem starts the API and the relay (with pub wrapping its publisher, if given) and returns the
// system; workers start with startWorker.
func newSystem(t *testing.T, wrapPublisher func(outbox.Publisher) outbox.Publisher) *system {
	t.Helper()
	s := &system{t: t, db: pg.NewDatabase(t), vhost: mq.NewVHost(t), redisURL: rdb.NewURL(t)}
	s.log = slog.New(slog.NewTextHandler(t.Output(), nil))
	s.store = repository.New(s.db)
	tp := noop.NewTracerProvider()

	svc, err := service.New(s.store, maxAttempts, time.Now, tp)
	if err != nil {
		t.Fatal(err)
	}
	s.api = httptest.NewServer(api.NewHandler(svc, s.log.With("service", "api"), prometheus.NewRegistry(), tp))
	t.Cleanup(s.api.Close)

	relayLog := s.log.With("service", "relay")
	sup := s.supervise(context.Background(), "relay", relayLog)
	var pub outbox.Publisher = queue.NewPublisher(sup)
	if wrapPublisher != nil {
		pub = wrapPublisher(pub)
	}
	metrics := outbox.NewMetrics(prometheus.NewRegistry())
	relay := outbox.NewRelay(s.store, pub, metrics, tp, relayLog)
	s.reaper = outbox.NewReaper(s.store, metrics, relayLog)
	s.run(func(ctx context.Context) { relay.Run(ctx) })

	s.provider = newScriptedProvider(t)
	return s
}

// run runs f until the test ends.
func (s *system) run(f func(ctx context.Context)) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f(ctx)
	}()
	s.t.Cleanup(func() {
		cancel()
		<-done
	})
}

// supervise connects to the system's virtual host until ctx ends or the test does, and waits until the
// topology is declared.
func (s *system) supervise(ctx context.Context, name string, log *slog.Logger) *queue.Supervisor {
	s.t.Helper()
	sup := queue.NewSupervisor(s.vhost, s.t.Name()+"/"+name, log)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.Run(ctx)
	}()
	s.t.Cleanup(func() {
		cancel()
		<-done
	})
	waitFor(s.t, 20*time.Second, "the "+name+" broker connection is ready", func() bool { return sup.Check(ctx) == nil })
	return sup
}

// workerConfig tunes one worker process.
type workerConfig struct {
	name string
	// rate is the sends per second per channel, shared by every worker of the system; 100 if zero.
	rate int
	// providerURL is where the worker sends; the system's provider if empty.
	providerURL string
	opts        worker.Options
}

// startWorker starts a worker process: every channel, with a broker connection of its own and the
// system's Redis database. It runs until the returned stop is called or the test ends; stop asks the
// worker to stop and returns at once (sends in flight finish in the background).
func (s *system) startWorker(cfg workerConfig) (stop func()) {
	s.t.Helper()
	if cfg.name == "" {
		cfg.name = "worker"
	}
	if cfg.rate == 0 {
		cfg.rate = 100
	}
	if cfg.providerURL == "" {
		cfg.providerURL = s.provider.srv.URL
	}
	if cfg.opts.Prefetch == 0 {
		cfg.opts = worker.Options{Prefetch: 5, Concurrency: 8, Weights: worker.DefaultWeights()}
	}
	log := s.log.With("service", cfg.name)
	tp := noop.NewTracerProvider()

	// As in the worker process, the broker connection outlives the dispatchers.
	sup := s.supervise(context.Background(), cfg.name, log)
	opts, err := redis.ParseURL(s.redisURL)
	if err != nil {
		s.t.Fatal(err)
	}
	client := redis.NewClient(opts)
	s.t.Cleanup(func() { _ = client.Close() })
	limiter, err := ratelimit.New(client, cfg.rate)
	if err != nil {
		s.t.Fatal(err)
	}
	sender, err := provider.NewClient(cfg.providerURL, 10*time.Second, tp)
	if err != nil {
		s.t.Fatal(err)
	}
	metrics := worker.NewMetrics(prometheus.NewRegistry())
	deliverer := worker.NewDeliverer(s.store, sender, queue.NewPublisher(sup), testRetryPolicy, metrics, log)

	ctx, cancel := context.WithCancel(context.Background())
	var running sync.WaitGroup
	for _, c := range queue.Channels() {
		ch, err := worker.NewChannel(c, sup, limiter, deliverer, metrics, cfg.opts, tp, log)
		if err != nil {
			s.t.Fatal(err)
		}
		running.Go(func() { ch.Run(ctx) })
	}
	s.t.Cleanup(func() {
		cancel()
		running.Wait()
	})
	return cancel
}

// request sends method path with body (JSON-encoded unless nil) and headers, and decodes the JSON
// answer into out unless out is nil. It returns the status code.
func (s *system) request(method, path string, body any, headers map[string]string, out any) int {
	s.t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			s.t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(s.t.Context(), method, s.api.URL+path, r)
	if err != nil {
		s.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		s.t.Fatalf("%s %s: reading the answer: %v", method, path, err)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			s.t.Fatalf("%s %s: decoding %s: %v", method, path, raw, err)
		}
	}
	return resp.StatusCode
}

// notification is the API's view of a notification, as far as the tests look at it.
type notification struct {
	ID                string  `json:"id"`
	BatchID           *string `json:"batch_id"`
	Channel           string  `json:"channel"`
	Priority          string  `json:"priority"`
	Recipient         string  `json:"recipient"`
	Content           string  `json:"content"`
	Status            string  `json:"status"`
	Attempts          int     `json:"attempts"`
	MaxAttempts       int     `json:"max_attempts"`
	LastError         *string `json:"last_error"`
	ProviderMessageID *string `json:"provider_message_id"`
	SentAt            *string `json:"sent_at"`
}

type create struct {
	Channel   string `json:"channel"`
	Priority  string `json:"priority"`
	Recipient string `json:"recipient"`
	Content   string `json:"content"`
}

// create posts n and expects it accepted.
func (s *system) create(n create, headers map[string]string) notification {
	s.t.Helper()
	var got notification
	if code := s.request(http.MethodPost, "/v1/notifications", n, headers, &got); code != http.StatusAccepted {
		s.t.Fatalf("POST /v1/notifications %+v answered %d, want 202", n, code)
	}
	return got
}

// get returns notification id.
func (s *system) get(id string) notification {
	s.t.Helper()
	var got notification
	if code := s.request(http.MethodGet, "/v1/notifications/"+id, nil, nil, &got); code != http.StatusOK {
		s.t.Fatalf("GET /v1/notifications/%s answered %d, want 200", id, code)
	}
	return got
}

// waitForStatus polls notification id until its status is want, and returns it.
func (s *system) waitForStatus(id, want string, timeout time.Duration) notification {
	s.t.Helper()
	var last notification
	waitFor(s.t, timeout, "notification "+id+" is "+want, func() bool {
		last = s.get(id)
		return last.Status == want
	})
	return last
}

// waitFor polls cond every 20 ms until it holds, failing the test after timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting until %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// answer is how the provider answers one request.
type answer struct {
	status int
	// hold, if not nil, keeps the request waiting until it is closed.
	hold chan struct{}
}

// providerRequest is a request the provider received.
type providerRequest struct {
	at      time.Time
	key     string // Idempotency-Key
	to      string
	channel string
	content string
}

// scriptedProvider answers each recipient's requests in turn from that recipient's script, and accepts
// every request beyond it (and every request of a recipient without one).
type scriptedProvider struct {
	mu       sync.Mutex
	scripts  map[string][]answer
	requests []providerRequest
	srv      *httptest.Server
}

func newScriptedProvider(t *testing.T) *scriptedProvider {
	t.Helper()
	p := &scriptedProvider{scripts: map[string][]answer{}}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ To, Channel, Content string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.requests = append(p.requests, providerRequest{at: time.Now(), key: r.Header.Get("Idempotency-Key"), to: body.To, channel: body.Channel, content: body.Content})
		a := answer{status: http.StatusAccepted}
		if script := p.scripts[body.To]; len(script) > 0 {
			a, p.scripts[body.To] = script[0], script[1:]
		}
		p.mu.Unlock()
		if a.hold != nil {
			select {
			case <-a.hold:
			case <-r.Context().Done():
				return
			}
		}
		if a.status != http.StatusAccepted {
			http.Error(w, http.StatusText(a.status), a.status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"messageId":"msg-%s-%d","status":"accepted","timestamp":%q}`,
			r.Header.Get("Idempotency-Key"), p.count(r.Header.Get("Idempotency-Key")), time.Now().UTC().Format(time.RFC3339))
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// received returns the requests so far whose Idempotency-Key is key, or all of them if key is empty.
func (p *scriptedProvider) received(key string) []providerRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []providerRequest
	for _, r := range p.requests {
		if key == "" || r.key == key {
			out = append(out, r)
		}
	}
	return out
}

// count returns how many requests carried key so far.
func (p *scriptedProvider) count(key string) int { return len(p.received(key)) }
