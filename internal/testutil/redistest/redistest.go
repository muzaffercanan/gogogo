//go:build integration

// Package redistest runs a disposable Redis server for integration tests.
//
// The server starts once per test binary (see Main). Each test gets a client on a logical database of
// its own, so the keys of one test are invisible to the others and tests can run in parallel.
package redistest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/muzaffercanan/gogogo/internal/testutil/tcretry"
)

// image matches the redis service in docker-compose.yml.
const image = "redis:7-alpine"

// databases is the number of logical databases the server is started with, and so the number of tests
// one test binary can run against it.
const databases = 1024

// flushTimeout bounds emptying a test's database at the end of the test.
const flushTimeout = time.Minute

// Server is a running Redis container.
type Server struct {
	container *tcredis.RedisContainer
	url       string // without a database, e.g. redis://127.0.0.1:32768
	seq       atomic.Int64
}

// Main starts a Server, stores it in *srv, runs the tests and stops the server. Use it from TestMain:
//
//	var rdb *redistest.Server
//
//	func TestMain(m *testing.M) { os.Exit(redistest.Main(m, &rdb)) }
func Main(m *testing.M, srv **Server) int {
	ctx := context.Background()
	s, err := tcretry.Start(ctx, Start)
	if err != nil {
		fmt.Fprintf(os.Stderr, "redistest: %v\n", err)
		return 1
	}
	*srv = s
	code := m.Run()
	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "redistest: %v\n", err)
	}
	return code
}

// Start runs a Redis container and waits until it accepts connections.
func Start(ctx context.Context) (*Server, error) {
	ctr, err := tcredis.Run(ctx, image, testcontainers.WithCmdArgs("--databases", strconv.Itoa(databases)))
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
		return nil, fmt.Errorf("starting redis container: %w", err)
	}
	url, err := ctr.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("reading redis url: %w", err)
	}
	return &Server{container: ctr, url: url}, nil
}

// Close stops the container.
func (s *Server) Close() error {
	if err := testcontainers.TerminateContainer(s.container); err != nil {
		return fmt.Errorf("terminating redis container: %w", err)
	}
	return nil
}

// NewURL returns the URL of an empty logical database of the test's own, and empties it when the test
// ends.
func (s *Server) NewURL(tb testing.TB) string {
	tb.Helper()
	db := s.seq.Add(1)
	if db >= databases {
		tb.Fatalf("redistest: all %d databases are in use; raise databases", databases)
	}
	url := s.url + "/" + strconv.FormatInt(db, 10)

	tb.Cleanup(func() {
		// Like pgtest's drop: the container is discarded at the end anyway, so a failed flush says
		// nothing about the code under test and is only logged.
		ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		defer cancel()
		opts, err := redis.ParseURL(url)
		if err == nil {
			c := redis.NewClient(opts)
			err = c.FlushDB(ctx).Err()
			_ = c.Close()
		}
		if err != nil {
			tb.Logf("redistest: could not empty database %d, leaving it to the container's end: %v", db, err)
		}
	})
	return url
}

// NewClient returns a client on an empty logical database of the test's own and closes it when the test
// ends.
func (s *Server) NewClient(tb testing.TB) *redis.Client {
	tb.Helper()
	opts, err := redis.ParseURL(s.NewURL(tb))
	if err != nil {
		tb.Fatalf("redistest: parsing url: %v", err)
	}
	c := redis.NewClient(opts)
	tb.Cleanup(func() { _ = c.Close() })
	return c
}
