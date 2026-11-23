//go:build integration

package pgtest_test

import (
	"context"
	"os"
	"testing"

	"github.com/muzaffercanan/gogogo/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &pg)) }

func TestNewDatabaseHasMigratedSchema(t *testing.T) {
	db := pg.NewDatabase(t)
	ctx := context.Background()

	for _, table := range []string{"batches", "notifications", "outbox", "notification_events"} {
		var rows int
		if err := db.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
			t.Fatalf("querying %s: %v", table, err)
		}
		if rows != 0 {
			t.Errorf("%s has %d rows, want 0", table, rows)
		}
	}
}

func TestDatabasesAreIsolated(t *testing.T) {
	ctx := context.Background()
	first := pg.NewDatabase(t)
	second := pg.NewDatabase(t)

	_, err := first.Exec(ctx, `INSERT INTO batches (id, total_count, accepted_count, rejected_count, item_results)
		VALUES ('0193a5b0-7c2e-7d41-9a3b-5f2e8c1d4a60', 1, 1, 0, '[]')`)
	if err != nil {
		t.Fatalf("inserting into the first database: %v", err)
	}

	var rows int
	if err := second.QueryRow(ctx, "SELECT count(*) FROM batches").Scan(&rows); err != nil {
		t.Fatalf("querying the second database: %v", err)
	}
	if rows != 0 {
		t.Errorf("second database sees %d batches, want 0", rows)
	}
}
