//go:build integration

// Package rabbittest runs a disposable RabbitMQ broker for integration tests.
//
// The broker starts once per test binary (see Main). Each test gets its own virtual host, so the
// exchanges, queues and messages of one test are invisible to the others and tests can run in parallel.
package rabbittest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/muzaffercanan/gogogo/internal/testutil/tcretry"
)

// image matches the rabbitmq service in docker-compose.yml. The management plugin it ships is used to
// create and delete virtual hosts.
const image = "rabbitmq:4-management-alpine"

// Server is a running RabbitMQ container.
type Server struct {
	container *rabbitmq.RabbitMQContainer
	amqpURL   string // without a virtual host, e.g. amqp://guest:guest@127.0.0.1:32771
	httpURL   string // management API, e.g. http://127.0.0.1:32772
	seq       atomic.Int64
}

// Main starts a Server, stores it in *srv, runs the tests and stops the server. Use it from TestMain:
//
//	var mq *rabbittest.Server
//
//	func TestMain(m *testing.M) { os.Exit(rabbittest.Main(m, &mq)) }
func Main(m *testing.M, srv **Server) int {
	ctx := context.Background()
	s, err := tcretry.Start(ctx, Start)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rabbittest: %v\n", err)
		return 1
	}
	*srv = s
	code := m.Run()
	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "rabbittest: %v\n", err)
	}
	return code
}

// Start runs a RabbitMQ container and waits until it accepts connections.
func Start(ctx context.Context) (*Server, error) {
	ctr, err := rabbitmq.Run(ctx, image)
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
		return nil, fmt.Errorf("starting rabbitmq container: %w", err)
	}
	amqpURL, err := ctr.AmqpURL(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("reading amqp url: %w", err)
	}
	httpURL, err := ctr.HttpURL(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("reading management url: %w", err)
	}
	return &Server{container: ctr, amqpURL: amqpURL, httpURL: httpURL}, nil
}

// Close stops the container.
func (s *Server) Close() error {
	if err := testcontainers.TerminateContainer(s.container); err != nil {
		return fmt.Errorf("terminating rabbitmq container: %w", err)
	}
	return nil
}

// NewVHost creates an empty virtual host, returns the AMQP URL that connects to it, and deletes it when
// the test ends.
func (s *Server) NewVHost(tb testing.TB) string {
	tb.Helper()
	name := fmt.Sprintf("test_%d", s.seq.Add(1))
	path := url.PathEscape(name)

	// The management API grants the user that creates a virtual host full permissions on it, so the
	// admin user's AMQP connections need no separate grant.
	if err := s.manage(tb.Context(), http.MethodPut, "/api/vhosts/"+path, ""); err != nil {
		tb.Fatalf("creating virtual host %s: %v", name, err)
	}

	tb.Cleanup(func() {
		// Like pgtest's drop: the container is discarded at the end anyway, so a failed delete says
		// nothing about the code under test and is only logged.
		ctx, cancel := context.WithTimeout(context.Background(), deleteTimeout)
		defer cancel()
		if err := s.manage(ctx, http.MethodDelete, "/api/vhosts/"+path, ""); err != nil {
			tb.Logf("rabbittest: could not delete virtual host %s, leaving it to the container's end: %v", name, err)
		}
	})
	return s.amqpURL + "/" + path
}

// SetPolicy applies a queue policy named name, with a definition such as
// {"max-length":0,"overflow":"reject-publish"}, to the queues of vhostURL (a URL from NewVHost) whose
// names match pattern. The broker applies policies asynchronously, shortly after this returns.
func (s *Server) SetPolicy(tb testing.TB, vhostURL, name, pattern, definition string) {
	tb.Helper()
	u, err := url.Parse(vhostURL)
	if err != nil {
		tb.Fatalf("parsing %s: %v", vhostURL, err)
	}
	vhost := strings.TrimPrefix(u.EscapedPath(), "/")
	body, err := json.Marshal(struct {
		Pattern    string          `json:"pattern"`
		Definition json.RawMessage `json:"definition"`
		ApplyTo    string          `json:"apply-to"`
	}{pattern, json.RawMessage(definition), "queues"})
	if err != nil {
		tb.Fatalf("encoding policy %s: %v", name, err)
	}
	if err := s.manage(tb.Context(), http.MethodPut, "/api/policies/"+vhost+"/"+url.PathEscape(name), string(body)); err != nil {
		tb.Fatalf("setting policy %s on %s: %v", name, vhost, err)
	}
}

// deleteTimeout bounds deleting a virtual host at the end of a test.
const deleteTimeout = time.Minute

// manage sends a request to the management API and fails on any status other than 2xx.
func (s *Server) manage(ctx context.Context, method, path, body string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, s.httpURL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.container.AdminUsername, s.container.AdminPassword)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}
