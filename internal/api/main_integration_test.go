//go:build integration

package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/muzaffercanan/gogogo/internal/api"
	"github.com/muzaffercanan/gogogo/internal/platform/logging"
	"github.com/muzaffercanan/gogogo/internal/repository"
	"github.com/muzaffercanan/gogogo/internal/service"
	"github.com/muzaffercanan/gogogo/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &pg)) }

// testAPI is the API served over HTTP against its own migrated database.
type testAPI struct {
	url string
	db  *pgxpool.Pool
	// now, when set, replaces the wall clock of the service.
	now atomic.Pointer[time.Time]
}

func newAPI(t *testing.T) *testAPI {
	t.Helper()
	a := &testAPI{db: pg.NewDatabase(t)}
	svc, err := service.New(repository.New(a.db), 5, func() time.Time {
		if now := a.now.Load(); now != nil {
			return *now
		}
		return time.Now()
	})
	if err != nil {
		t.Fatalf("service.New: %v", err)
	}
	srv := httptest.NewServer(api.NewHandler(svc, logging.New(io.Discard, "error"), prometheus.NewRegistry()))
	t.Cleanup(srv.Close)
	a.url = srv.URL
	return a
}

// response is an answered request with its decoded JSON body.
type response struct {
	status int
	header http.Header
	raw    []byte
	body   map[string]any
}

// do sends a request with body (none if empty) and the given headers, and decodes the JSON answer.
func (a *testAPI) do(t *testing.T, method, path, body string, headers ...string) response {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.url+path, r)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	out := response{status: resp.StatusCode, header: resp.Header, raw: raw}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.body); err != nil {
			t.Fatalf("%s %s: decoding %s: %v", method, path, raw, err)
		}
	}
	return out
}

// count returns the number of rows in table.
func (a *testAPI) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := a.db.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

// fieldErrors returns the errors of a problem body as "field code" pairs.
func fieldErrors(body map[string]any) []string {
	list, _ := body["errors"].([]any)
	var out []string
	for _, e := range list {
		m, _ := e.(map[string]any)
		out = append(out, m["field"].(string)+" "+m["code"].(string))
	}
	return out
}

// setNow fixes the service's clock at now.
func (a *testAPI) setNow(now time.Time) { a.now.Store(&now) }
