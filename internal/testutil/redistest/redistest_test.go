//go:build integration

package redistest_test

import (
	"errors"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/muzaffercanan/gogogo/internal/testutil/redistest"
)

var rdb *redistest.Server

func TestMain(m *testing.M) { os.Exit(redistest.Main(m, &rdb)) }

func TestDatabasesAreIsolated(t *testing.T) {
	first := rdb.NewClient(t)
	second := rdb.NewClient(t)

	if err := first.Set(t.Context(), "only-in-first", "1", 0).Err(); err != nil {
		t.Fatalf("setting a key in the first database: %v", err)
	}
	if got, err := first.Get(t.Context(), "only-in-first").Result(); err != nil || got != "1" {
		t.Errorf("first database: Get = %q, %v; want 1", got, err)
	}
	if _, err := second.Get(t.Context(), "only-in-first").Result(); !errors.Is(err, redis.Nil) {
		t.Errorf("second database: Get error = %v, want redis.Nil", err)
	}
}

func TestDatabaseIsEmptiedWhenTheTestEnds(t *testing.T) {
	var url string
	t.Run("writer", func(t *testing.T) {
		url = rdb.NewURL(t)
		opts, err := redis.ParseURL(url)
		if err != nil {
			t.Fatal(err)
		}
		c := redis.NewClient(opts)
		defer c.Close()
		if err := c.Set(t.Context(), "left-behind", "1", 0).Err(); err != nil {
			t.Fatal(err)
		}
	})

	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(opts)
	defer c.Close()
	if n, err := c.DBSize(t.Context()).Result(); err != nil || n != 0 {
		t.Errorf("after the writer ended its database holds %d keys (%v), want 0", n, err)
	}
}
