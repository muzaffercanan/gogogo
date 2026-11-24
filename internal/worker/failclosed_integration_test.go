//go:build integration

package worker_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

// tcpProxy forwards TCP connections to target. Switched off, it drops every open connection and refuses
// new ones, which is how a Redis outage looks to a client.
type tcpProxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	up    bool
	conns []net.Conn
}

// proxied starts a proxy in front of the Redis of rawURL and returns it with the URL that reaches the
// same database through it.
func proxied(t *testing.T, rawURL string) (*tcpProxy, string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: u.Host, up: true}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.set(false)
	})
	u.Host = ln.Addr().String()
	return p, u.String()
}

func (p *tcpProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if !p.up {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		server, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go pipe(server, client)
		go pipe(client, server)
	}
}

func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

// set switches the proxy on or off; switching it off closes every connection through it.
func (p *tcpProxy) set(up bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up = up
	if !up {
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
	}
}

// metric returns the value of the series of family name with label channel=sms, or -1 if there is none.
func metric(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if hasLabel(m, "channel", "sms") {
				switch {
				case m.GetGauge() != nil:
					return m.GetGauge().GetValue()
				case m.GetCounter() != nil:
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return -1
}

func hasLabel(m *dto.Metric, name, value string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}

func TestWorkerSendsNothingWhileTheRateLimiterIsUnreachable(t *testing.T) {
	h := newHarness(t, always(accepted("m-8")))
	proxy, url := proxied(t, h.redisURL)
	h.redisURL = url
	proxy.set(false)
	h.startWorker(t)

	n := h.create(t, nil)
	h.dispatch(t)
	waitFor(t, "the channel reports the limiter unavailable", func() bool {
		return metric(t, h.reg, "ratelimit_unavailable") == 1
	})
	// Fail closed: nothing goes out while no token can be taken (ADR 0004).
	time.Sleep(500 * time.Millisecond)
	if reqs := h.provider.got(); len(reqs) != 0 {
		t.Fatalf("provider got %d requests without a rate-limit token, want none", len(reqs))
	}
	if got, _ := h.store.GetNotification(t.Context(), n.ID); got.Status != domain.StatusQueued {
		t.Errorf("notification is %s while the limiter is down, want queued", got.Status)
	}
	if err := h.metrics.CheckLimiter(t.Context()); err == nil || !strings.Contains(err.Error(), "sms") {
		t.Errorf("readiness check = %v, want a failure naming sms", err)
	}
	if errors := metric(t, h.reg, "ratelimit_errors_total"); errors < 1 {
		t.Errorf("ratelimit_errors_total = %v, want at least 1", errors)
	}

	proxy.set(true)
	h.waitStatus(t, n.ID, domain.StatusSent)
	if down := metric(t, h.reg, "ratelimit_unavailable"); down != 0 {
		t.Errorf("ratelimit_unavailable = %v after the limiter came back, want 0", down)
	}
	if err := h.metrics.CheckLimiter(t.Context()); err != nil {
		t.Errorf("readiness check after the limiter came back = %v, want nil", err)
	}
}
