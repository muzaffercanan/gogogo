//go:build integration

package worker_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/provider"
	"github.com/muzaffercanan/gogogo/internal/queue"
	"github.com/muzaffercanan/gogogo/internal/ratelimit"
	"github.com/muzaffercanan/gogogo/internal/repository"
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

// answer is how the fake provider responds to one request.
type answer struct {
	status int
	header map[string]string
	body   string
	delay  time.Duration
}

// received is one request the fake provider got.
type received struct {
	key  string
	body string
}

// fakeProvider records the requests it gets and answers them with answerFor.
type fakeProvider struct {
	mu        sync.Mutex
	requests  []received
	answerFor func(n int) answer // n counts requests from 0
	srv       *httptest.Server
}

func newFakeProvider(t *testing.T, answerFor func(n int) answer) *fakeProvider {
	t.Helper()
	p := &fakeProvider{answerFor: answerFor}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		p.mu.Lock()
		n := len(p.requests)
		p.requests = append(p.requests, received{key: r.Header.Get("Idempotency-Key"), body: string(body)})
		p.mu.Unlock()
		a := p.answerFor(n)
		if a.delay > 0 {
			time.Sleep(a.delay)
		}
		for k, v := range a.header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(a.status)
		_, _ = io.WriteString(w, a.body)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// always answers every request the same way.
func always(a answer) func(int) answer { return func(int) answer { return a } }

func accepted(id string) answer {
	return answer{status: http.StatusAccepted, body: `{"messageId":"` + id + `","status":"accepted"}`}
}

func (p *fakeProvider) got() []received {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]received(nil), p.requests...)
}

// harness is one isolated system: a database, a virtual host, a Redis database and a fake provider,
// with a relay to dispatch outbox rows and a worker for the sms channel.
type harness struct {
	db       *pgxpool.Pool
	store    *repository.Store
	vhost    string
	sup      *queue.Supervisor
	pub      *queue.Publisher
	relay    *outbox.Relay
	provider *fakeProvider
	log      *slog.Logger
	// rate is the sends per second the worker's limiter allows; 100 unless a test changes it.
	rate int
	logs syncBuffer // everything logged, also written to the test output
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a logger.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitLogged waits until a line holding every one of parts has been logged.
func (h *harness) waitLogged(t *testing.T, parts ...string) {
	t.Helper()
	waitFor(t, fmt.Sprintf("a log line with %q", parts), func() bool {
		for line := range strings.Lines(h.logs.String()) {
			found := true
			for _, p := range parts {
				found = found && strings.Contains(line, p)
			}
			if found {
				return true
			}
		}
		return false
	})
}

func newHarness(t *testing.T, answerFor func(int) answer) *harness {
	t.Helper()
	h := &harness{db: pg.NewDatabase(t), vhost: mq.NewVHost(t), rate: 100}
	h.log = slog.New(slog.NewTextHandler(io.MultiWriter(t.Output(), &h.logs), nil))
	h.store = repository.New(h.db)
	h.sup = queue.NewSupervisor(h.vhost, t.Name(), h.log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.sup.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	waitFor(t, "the supervisor is ready", func() bool { return h.sup.Check(context.Background()) == nil })
	h.pub = queue.NewPublisher(h.sup)
	h.relay = outbox.NewRelay(h.store, h.pub, outbox.NewMetrics(prometheus.NewRegistry()), h.log)
	h.provider = newFakeProvider(t, answerFor)
	return h
}

// startWorker runs the sms channel of a worker until the test ends or the returned stop is called;
// stop returns once the worker has.
func (h *harness) startWorker(t *testing.T) (stop func()) {
	t.Helper()
	limiter, err := ratelimit.New(rdb.NewClient(t), h.rate)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := provider.NewClient(h.provider.srv.URL, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	retry := worker.WithJitter(worker.DefaultRetryPolicy(), func(window time.Duration) time.Duration { return window })
	deliverer := worker.NewDeliverer(h.store, sender, h.pub, retry, h.log)
	ch, err := worker.NewChannel(domain.ChannelSMS, h.sup, 5, worker.DefaultWeights(), limiter, deliverer, 4, h.log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ch.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Error("the worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// create stores an sms notification and returns it; modify may change it first.
func (h *harness) create(t *testing.T, modify func(*domain.NewNotification)) domain.Notification {
	t.Helper()
	nn := domain.NewNotification{
		ID:            uuid.Must(uuid.NewV7()),
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityHigh,
		Recipient:     "+905551234567",
		Content:       "Your code is 1234",
		MaxAttempts:   5,
		CorrelationID: "corr-" + t.Name(),
	}
	if modify != nil {
		modify(&nn)
	}
	n, _, err := h.store.CreateNotification(t.Context(), nn)
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	return n
}

// dispatch runs the relay once, so due outbox rows become queued messages.
func (h *harness) dispatch(t *testing.T) {
	t.Helper()
	if _, _, err := h.relay.Once(t.Context()); err != nil {
		t.Fatalf("relay.Once: %v", err)
	}
}

// waitStatus waits until notification id is in status and returns it.
func (h *harness) waitStatus(t *testing.T, id uuid.UUID, status domain.Status) domain.Notification {
	t.Helper()
	var n domain.Notification
	waitFor(t, fmt.Sprintf("notification %s is %s", id, status), func() bool {
		var err error
		n, err = h.store.GetNotification(context.Background(), id)
		if err != nil {
			t.Fatalf("GetNotification: %v", err)
		}
		return n.Status == status
	})
	return n
}

// statuses returns the statuses notification id went through, in order.
func (h *harness) statuses(t *testing.T, id uuid.UUID) []string {
	t.Helper()
	rows, err := h.db.Query(context.Background(), `SELECT to_status FROM notification_events WHERE notification_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

// lastReason returns the reason of the last status event of notification id.
func (h *harness) lastReason(t *testing.T, id uuid.UUID) string {
	t.Helper()
	var reason string
	if err := h.db.QueryRow(context.Background(), `SELECT coalesce(reason, '') FROM notification_events
		WHERE notification_id = $1 ORDER BY id DESC LIMIT 1`, id).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	return reason
}

// outboxDue returns how long after the database clock the outbox row of id becomes due, and whether
// there is one.
func (h *harness) outboxDue(t *testing.T, id uuid.UUID) (time.Duration, bool) {
	t.Helper()
	var seconds float64
	err := h.db.QueryRow(context.Background(), `SELECT extract(epoch FROM available_at - now()) FROM outbox WHERE notification_id = $1`, id).Scan(&seconds)
	if err != nil {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}

// channel opens an AMQP channel on the harness's virtual host for inspecting queues.
func (h *harness) channel(t *testing.T) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(h.vhost)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// queueDepth returns the ready and unacknowledged messages of queue name.
func queueDepth(t *testing.T, ch *amqp.Channel, name string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("inspecting %s: %v", name, err)
	}
	return q.Messages
}

// assertAcknowledged stops the worker and checks that sms.high is empty. A queue's message count leaves
// out the messages a consumer holds unacknowledged; stopping the worker closes its channel, which puts
// those back, so only acknowledged messages stay gone.
func assertAcknowledged(t *testing.T, ch *amqp.Channel, stop func()) {
	t.Helper()
	stop()
	time.Sleep(300 * time.Millisecond) // the broker requeues a closed channel's messages asynchronously
	if depth := queueDepth(t, ch, "sms.high"); depth != 0 {
		t.Errorf("after the worker stopped sms.high holds %d messages: they were never acknowledged", depth)
	}
}

// deadLetter waits up to ten seconds for a message in sms.dlq and returns it.
func deadLetter(t *testing.T, ch *amqp.Channel) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		d, ok, err := ch.Get("sms.dlq", true)
		if err != nil {
			t.Fatalf("getting from sms.dlq: %v", err)
		}
		if ok {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatal("nothing arrived in sms.dlq")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitFor polls cond for up to ten seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
