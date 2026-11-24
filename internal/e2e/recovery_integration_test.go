//go:build integration

package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/outbox"
	"github.com/muzaffercanan/gogogo/internal/queue"
)

func TestTheReaperRecoversADeliveryWhoseWorkerDied(t *testing.T) {
	s := newSystem(t, nil)
	const recipient = "+905550000030"
	release := make(chan struct{})
	// The first request never gets an answer while the test runs: the worker sending it is as good as
	// dead, holding the notification in sending without ever recording an outcome.
	s.provider.script(recipient, answer{status: http.StatusAccepted, hold: release})
	stopA := s.startWorker(workerConfig{name: "worker-a"})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) }) // before worker-a's cleanup waits for it

	n := s.create(create{Channel: "sms", Priority: "high", Recipient: recipient, Content: "recover me"}, nil)
	waitFor(t, 10*time.Second, "the provider holds the first request", func() bool { return len(s.provider.received(n.ID)) == 1 })
	stopA()
	if got := s.get(n.ID); got.Status != "sending" || got.Attempts != 1 {
		t.Fatalf("notification is %s after %d attempts while its worker is gone, want sending after 1", got.Status, got.Attempts)
	}

	// The lease runs out: the delivery started longer ago than the reaper waits.
	if _, err := s.db.Exec(t.Context(), `UPDATE notifications SET sending_started_at = now() - interval '2 minutes' WHERE id = $1`, n.ID); err != nil {
		t.Fatal(err)
	}
	if retried, failed, err := s.reaper.Once(t.Context()); err != nil || retried != 1 || failed != 0 {
		t.Fatalf("reaper Once = (%d retried, %d failed, %v), want (1, 0, nil)", retried, failed, err)
	}

	s.startWorker(workerConfig{name: "worker-b"})
	sent := s.waitForStatus(n.ID, "sent", 15*time.Second)
	if sent.Attempts != 2 || deref(sent.ProviderMessageID) != "msg-"+n.ID+"-2" {
		t.Errorf("recovered notification sent after %d attempts with provider message id %q, want 2 attempts and the second request's", sent.Attempts, deref(sent.ProviderMessageID))
	}
	reqs := s.provider.received(n.ID)
	if len(reqs) != 2 {
		t.Fatalf("the provider got %d requests, want 2 (the dead worker's and the retry)", len(reqs))
	}

	// The provider finally answers the dead worker's request; its late outcome changes nothing.
	once.Do(func() { close(release) })
	time.Sleep(500 * time.Millisecond)
	if got := s.get(n.ID); got.Status != "sent" || got.Attempts != 2 || deref(got.ProviderMessageID) != deref(sent.ProviderMessageID) {
		t.Errorf("after the dead worker's answer the notification is %s after %d attempts with %q, want it unchanged", got.Status, got.Attempts, deref(got.ProviderMessageID))
	}
}

// crashingPublisher publishes the first batch it is given and then fails, as a relay that dies after
// publishing and before committing would; later batches go through.
type crashingPublisher struct {
	inner outbox.Publisher
	mu    sync.Mutex
	calls map[string]int // per message id
}

func (p *crashingPublisher) Publish(ctx context.Context, msgs []queue.Message) error {
	err := p.inner.Publish(ctx, msgs)
	p.mu.Lock()
	defer p.mu.Unlock()
	first := len(p.calls) == 0
	for _, m := range msgs {
		p.calls[m.ID]++
	}
	if err == nil && first {
		return errors.New("the relay died after publishing, before committing")
	}
	return err
}

func (p *crashingPublisher) published(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[id]
}

func TestARelayDyingBetweenPublishAndCommitSendsOnce(t *testing.T) {
	var pub *crashingPublisher
	s := newSystem(t, func(inner outbox.Publisher) outbox.Publisher {
		pub = &crashingPublisher{inner: inner, calls: map[string]int{}}
		return pub
	})
	n := s.create(create{Channel: "email", Priority: "high", Recipient: "twice@example.com", Content: "published twice"}, nil)
	// The relay publishes, fails to commit, and publishes again: two messages wait for one queued
	// notification. The worker starts only now, so it finds both (a worker already running could take
	// the first while the rollback left the notification pending, and skip it for that reason alone).
	s.waitForStatus(n.ID, "queued", 15*time.Second)
	s.startWorker(workerConfig{opts: oneAtATime()})
	s.waitForStatus(n.ID, "sent", 15*time.Second)
	time.Sleep(500 * time.Millisecond) // time for the duplicate message to be taken

	if got := pub.published(n.ID); got != 2 {
		t.Errorf("the relay published the notification %d times, want 2 (before the rolled back commit and after)", got)
	}
	if got := len(s.provider.received(n.ID)); got != 1 {
		t.Errorf("the provider got %d requests, want 1", got)
	}
	if got := s.get(n.ID); got.Status != "sent" || got.Attempts != 1 {
		t.Errorf("notification is %s after %d attempts, want sent after 1", got.Status, got.Attempts)
	}
	if depth := s.queueDepth(t, queue.QueueName(domain.ChannelEmail, domain.PriorityHigh)); depth != 0 {
		t.Errorf("%d messages left in the work queue, want none", depth)
	}
}
