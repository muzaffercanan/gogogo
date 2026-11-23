//go:build integration

// Package pgtest runs a disposable PostgreSQL server for integration tests.
//
// The server starts once per test binary (see Main). Every migrations/*.up.sql file is applied to a
// template database at startup, and each test gets its own database cloned from that template, so tests
// start from an empty, fully migrated schema and can run in parallel without seeing each other's rows.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	// image matches the postgres service in docker-compose.yml.
	image        = "postgres:17-alpine"
	templateName = "notify_template"
)

// Server is a running PostgreSQL container whose template database holds the migrated schema.
type Server struct {
	container *postgres.PostgresContainer
	admin     *pgxpool.Pool // connected to the "postgres" database, never to the template
	config    *pgxpool.Config
	seq       atomic.Int64
}

// Main starts a Server, stores it in *srv, runs the tests and stops the server. Use it from TestMain:
//
//	var pg *pgtest.Server
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &pg)) }
func Main(m *testing.M, srv **Server) int {
	ctx := context.Background()
	s, err := Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		return 1
	}
	*srv = s
	code := m.Run()
	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
	}
	return code
}

// Start runs a PostgreSQL container and applies the repository's migrations to the template database.
func Start(ctx context.Context) (*Server, error) {
	scripts, err := upMigrations()
	if err != nil {
		return nil, err
	}

	ctr, err := postgres.Run(ctx, image,
		postgres.WithDatabase(templateName),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.WithOrderedInitScripts(scripts...),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr)
		}
		return nil, fmt.Errorf("starting postgres container: %w", err)
	}

	s, err := connect(ctx, ctr)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, err
	}
	return s, nil
}

func connect(ctx context.Context, ctr *postgres.PostgresContainer) (*Server, error) {
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("reading connection string: %w", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}

	adminCfg := cfg.Copy()
	adminCfg.ConnConfig.Database = "postgres"
	adminCfg.MaxConns = 2
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return &Server{container: ctr, admin: admin, config: cfg}, nil
}

// Close stops the container.
func (s *Server) Close() error {
	s.admin.Close()
	if err := testcontainers.TerminateContainer(s.container); err != nil {
		return fmt.Errorf("terminating postgres container: %w", err)
	}
	return nil
}

// NewDatabase creates a database with the migrated schema and no rows, returns a pool connected to it,
// and drops it when the test ends.
func (s *Server) NewDatabase(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	ctx := context.Background()

	name := fmt.Sprintf("test_%d", s.seq.Add(1))
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := s.admin.Exec(ctx, "CREATE DATABASE "+ident+" TEMPLATE "+pgx.Identifier{templateName}.Sanitize()); err != nil {
		tb.Fatalf("creating database %s: %v", name, err)
	}

	cfg := s.config.Copy()
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		tb.Fatalf("connecting to database %s: %v", name, err)
	}

	tb.Cleanup(func() {
		pool.Close()
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := s.admin.Exec(dropCtx, "DROP DATABASE "+ident+" WITH (FORCE)"); err != nil {
			tb.Errorf("dropping database %s: %v", name, err)
		}
	})
	return pool
}

// upMigrations returns the absolute paths of migrations/*.up.sql in version order.
func upMigrations() ([]string, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, err
	}
	scripts, err := filepath.Glob(filepath.Join(root, "migrations", "*.up.sql"))
	if err != nil {
		return nil, fmt.Errorf("listing migrations: %w", err)
	}
	if len(scripts) == 0 {
		return nil, errors.New("no migrations found in " + filepath.Join(root, "migrations"))
	}
	// golang-migrate versions are zero-padded, so lexical order is version order.
	sort.Strings(scripts)
	return scripts, nil
}

// moduleRoot walks up from the working directory (the test's package directory) to the directory
// that holds go.mod.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getting working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the working directory")
		}
		dir = parent
	}
}
