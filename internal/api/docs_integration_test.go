//go:build integration

package api_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// get fetches path without decoding the body, for responses that are not JSON.
func (a *testAPI) get(t *testing.T, path string) (status int, header http.Header, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.url+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(raw)
}

func TestServesTheSpec(t *testing.T) {
	want, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}

	status, header, body := newAPI(t).get(t, "/openapi.yaml")

	if status != http.StatusOK || header.Get("Content-Type") != "application/yaml" || body != string(want) {
		t.Errorf("GET /openapi.yaml = %d %s, %d bytes; want 200 application/yaml with api/openapi.yaml (%d bytes)",
			status, header.Get("Content-Type"), len(body), len(want))
	}
}

func TestServesSwaggerUI(t *testing.T) {
	status, header, body := newAPI(t).get(t, "/docs")

	if status != http.StatusOK || !strings.HasPrefix(header.Get("Content-Type"), "text/html") {
		t.Fatalf("GET /docs = %d %s, want 200 HTML", status, header.Get("Content-Type"))
	}
	for _, want := range []string{
		`url: "/openapi.yaml"`,
		`swagger-ui-dist@5.17.14/swagger-ui-bundle.js`,
		`integrity="sha256-`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("docs page does not contain %s", want)
		}
	}
}
