//go:build integration

package e2e_test

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func (s *system) cancel(id string) int {
	s.t.Helper()
	return s.request(http.MethodPost, "/v1/notifications/"+id+"/cancel", nil, nil, nil)
}

func TestACancelledNotificationIsNeverSent(t *testing.T) {
	s := newSystem(t, nil)

	// No worker yet: the relay queues the notification and nothing takes it.
	n := s.create(create{Channel: "sms", Priority: "normal", Recipient: "+905550000020", Content: "cancel me"}, nil)
	s.waitForStatus(n.ID, "queued", 10*time.Second)
	if code := s.cancel(n.ID); code != http.StatusOK {
		t.Fatalf("cancelling a queued notification answered %d, want 200", code)
	}

	// The worker takes the cancelled notification's message before the next one's on the same queue.
	s.startWorker(workerConfig{opts: oneAtATime()})
	next := s.create(create{Channel: "sms", Priority: "normal", Recipient: "+905550000021", Content: "after"}, nil)
	s.waitForStatus(next.ID, "sent", 15*time.Second)

	if got := s.get(n.ID); got.Status != "cancelled" || got.Attempts != 0 {
		t.Errorf("cancelled notification is %s after %d attempts, want cancelled after 0", got.Status, got.Attempts)
	}
	if got := len(s.provider.received(n.ID)); got != 0 {
		t.Errorf("the provider got %d requests for the cancelled notification, want 0", got)
	}
}

func TestANotificationBeingSentCannotBeCancelled(t *testing.T) {
	s := newSystem(t, nil)
	const recipient = "+905550000022"
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	s.provider.script(recipient, answer{status: http.StatusAccepted, hold: release})
	s.startWorker(workerConfig{})

	n := s.create(create{Channel: "sms", Priority: "high", Recipient: recipient, Content: "on its way"}, nil)
	waitFor(t, 10*time.Second, "the provider holds the request", func() bool { return len(s.provider.received(n.ID)) == 1 })
	if got := s.get(n.ID); got.Status != "sending" {
		t.Fatalf("notification is %s while the provider holds its request, want sending", got.Status)
	}
	if code := s.cancel(n.ID); code != http.StatusConflict {
		t.Errorf("cancelling a notification being sent answered %d, want 409", code)
	}
	once.Do(func() { close(release) })
	s.waitForStatus(n.ID, "sent", 10*time.Second)
}

func TestCancelsRacingTheWorkerNeverLoseNorDoubleASend(t *testing.T) {
	s := newSystem(t, nil)
	s.startWorker(workerConfig{})

	// The rate limit spreads the 40 sends over about 400 ms, and each notification is cancelled 0 to
	// 585 ms after it is created, in an order unrelated to its turn, so some cancels come first and
	// some come late. Whichever wins, the answer to the cancel says what happened: 200 and never sent,
	// or 409 and sent exactly once.
	const total = 40
	type result struct {
		id   string
		code int
	}
	results := make([]result, total)
	var wg sync.WaitGroup
	for i := range total {
		wg.Go(func() {
			var n notification
			body := create{Channel: "push", Priority: "high", Recipient: fmt.Sprintf("race-token-%d", i), Content: "race"}
			if code := s.request(http.MethodPost, "/v1/notifications", body, nil, &n); code != http.StatusAccepted {
				t.Errorf("creating notification %d answered %d, want 202", i, code)
				return
			}
			time.Sleep(time.Duration(i*37%total) * 15 * time.Millisecond)
			results[i] = result{id: n.ID, code: s.cancel(n.ID)}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	outcomes := map[int]int{}
	for _, r := range results {
		outcomes[r.code]++
		var n notification
		waitFor(t, 15*time.Second, "notification "+r.id+" is final", func() bool {
			n = s.get(r.id)
			return n.Status == "cancelled" || n.Status == "sent"
		})
		sends := len(s.provider.received(r.id))
		switch r.code {
		case http.StatusOK:
			// A late message of a cancelled notification is skipped, so give it time to be.
			time.Sleep(100 * time.Millisecond)
			if n.Status != "cancelled" || len(s.provider.received(r.id)) != 0 {
				t.Errorf("notification %s: cancel answered 200, but it is %s after %d sends", r.id, n.Status, sends)
			}
		case http.StatusConflict:
			if n.Status != "sent" || sends != 1 {
				t.Errorf("notification %s: cancel answered 409, but it is %s after %d sends, want sent once", r.id, n.Status, sends)
			}
		default:
			t.Errorf("notification %s: cancel answered %d, want 200 or 409", r.id, r.code)
		}
	}
	t.Logf("cancels: %d answered 200, %d answered 409", outcomes[http.StatusOK], outcomes[http.StatusConflict])
}

func TestConcurrentCreatesWithOneKeySendOneNotification(t *testing.T) {
	s := newSystem(t, nil)
	s.startWorker(workerConfig{})

	key := uuid.NewString()
	body := create{Channel: "email", Priority: "normal", Recipient: "once@example.com", Content: "only once"}
	const clients = 10
	ids := make([]string, clients)
	codes := make([]int, clients)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Go(func() {
			var n notification
			codes[i] = s.request(http.MethodPost, "/v1/notifications", body, map[string]string{"Idempotency-Key": key}, &n)
			ids[i] = n.ID
		})
	}
	wg.Wait()
	for i := range clients {
		if codes[i] != http.StatusAccepted || ids[i] != ids[0] {
			t.Errorf("request %d answered %d with notification %q, want 202 with %q like the first", i, codes[i], ids[i], ids[0])
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// The same key with another body is refused.
	other := body
	other.Content = "something else"
	if code := s.request(http.MethodPost, "/v1/notifications", other, map[string]string{"Idempotency-Key": key}, nil); code != http.StatusUnprocessableEntity {
		t.Errorf("reusing the key for another body answered %d, want 422", code)
	}

	s.waitForStatus(ids[0], "sent", 15*time.Second)
	time.Sleep(200 * time.Millisecond) // time for a second send, if there were one
	var page struct {
		Items []notification `json:"items"`
	}
	if code := s.request(http.MethodGet, "/v1/notifications", nil, nil, &page); code != http.StatusOK || len(page.Items) != 1 {
		t.Errorf("GET /v1/notifications answered %d with %d notifications, want 200 with 1", code, len(page.Items))
	}
	if got := len(s.provider.received("")); got != 1 {
		t.Errorf("the provider got %d requests, want 1", got)
	}
}
