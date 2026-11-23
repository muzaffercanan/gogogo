//go:build integration

package repository_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository"
)

// listed is the part of a notification that the list filters and ordering depend on.
type listed struct {
	id        uuid.UUID
	channel   domain.Channel
	status    domain.Status
	createdAt time.Time
}

func TestListNotificationsFiltersAndPages(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)

	// Twelve notifications one hour apart, cycling through the channels; even ones pending, odd ones sent.
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	var all []listed
	for i := range 12 {
		status := domain.StatusPending
		if i%2 == 1 {
			status = domain.StatusSent
		}
		all = append(all, seed(ctx, t, store, db, channels[i%3], status, base.Add(time.Duration(i)*time.Hour)))
	}

	tests := []struct {
		name   string
		filter repository.ListFilter
		match  func(listed) bool
	}{
		{"no filter", repository.ListFilter{}, func(listed) bool { return true }},
		{"status", repository.ListFilter{Status: domain.StatusSent},
			func(n listed) bool { return n.status == domain.StatusSent }},
		{"channel", repository.ListFilter{Channel: domain.ChannelEmail},
			func(n listed) bool { return n.channel == domain.ChannelEmail }},
		{"channel and status", repository.ListFilter{Channel: domain.ChannelEmail, Status: domain.StatusSent},
			func(n listed) bool { return n.channel == domain.ChannelEmail && n.status == domain.StatusSent }},
		{"created range, from inclusive and to exclusive",
			repository.ListFilter{CreatedFrom: base.Add(3 * time.Hour), CreatedTo: base.Add(6 * time.Hour)},
			func(n listed) bool {
				return !n.createdAt.Before(base.Add(3*time.Hour)) && n.createdAt.Before(base.Add(6*time.Hour))
			}},
		{"status and created from", repository.ListFilter{Status: domain.StatusPending, CreatedFrom: base.Add(6 * time.Hour)},
			func(n listed) bool {
				return n.status == domain.StatusPending && !n.createdAt.Before(base.Add(6*time.Hour))
			}},
		{"nothing matches", repository.ListFilter{CreatedFrom: base.Add(48 * time.Hour)},
			func(listed) bool { return false }},
	}
	for _, tt := range tests {
		want := expectedOrder(all, tt.match)
		for _, limit := range []int{1, 4, 5, 100} {
			t.Run(fmt.Sprintf("%s/limit %d", tt.name, limit), func(t *testing.T) {
				f := tt.filter
				f.Limit = limit
				got := listAllPages(ctx, t, store, f)
				if !slices.Equal(got, want) {
					t.Errorf("listed %v\nwant   %v", got, want)
				}
			})
		}
	}
}

// TestListNotificationsPagesThroughEqualTimestamps covers notifications created in one transaction, which
// share created_at: the ID must order them so that no page skips or repeats one.
func TestListNotificationsPagesThroughEqualTimestamps(t *testing.T) {
	ctx := context.Background()
	db := pg.NewDatabase(t)
	store := repository.New(db)
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	var all []listed
	for range 6 {
		all = append(all, seed(ctx, t, store, db, domain.ChannelSMS, domain.StatusPending, at))
	}

	got := listAllPages(ctx, t, store, repository.ListFilter{Limit: 4})
	if want := expectedOrder(all, func(listed) bool { return true }); !slices.Equal(got, want) {
		t.Errorf("listed %v\nwant   %v", got, want)
	}
}

func TestListNotificationsRequiresPositiveLimit(t *testing.T) {
	store := repository.New(pg.NewDatabase(t))
	if _, err := store.ListNotifications(context.Background(), repository.ListFilter{}); err == nil {
		t.Error("ListNotifications with limit 0 succeeded, want an error")
	}
}

// seed stores a notification and then sets its status and creation time directly.
func seed(ctx context.Context, t *testing.T, store *repository.Store, db *pgxpool.Pool,
	channel domain.Channel, status domain.Status, createdAt time.Time,
) listed {
	t.Helper()
	n, _, err := store.CreateNotification(ctx, newNotification(t, func(n *domain.NewNotification) { n.Channel = channel }))
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE notifications SET status = $2, created_at = $3 WHERE id = $1`,
		n.ID, string(status), createdAt); err != nil {
		t.Fatalf("seeding notification: %v", err)
	}
	return listed{id: n.ID, channel: channel, status: status, createdAt: createdAt}
}

// expectedOrder returns the IDs of the notifications that match, newest first, ties broken by ID.
func expectedOrder(all []listed, match func(listed) bool) []uuid.UUID {
	var sel []listed
	for _, n := range all {
		if match(n) {
			sel = append(sel, n)
		}
	}
	slices.SortFunc(sel, func(a, b listed) int {
		if c := b.createdAt.Compare(a.createdAt); c != 0 {
			return c
		}
		return bytes.Compare(b.id[:], a.id[:])
	})
	ids := []uuid.UUID{}
	for _, n := range sel {
		ids = append(ids, n.id)
	}
	return ids
}

// listAllPages follows the cursors until the last page and returns the IDs in the order they were listed.
// It also checks the page contract: no page exceeds the limit, only the last page lacks a cursor, and no
// page is empty unless the whole result is.
func listAllPages(ctx context.Context, t *testing.T, store *repository.Store, f repository.ListFilter) []uuid.UUID {
	t.Helper()
	ids := []uuid.UUID{}
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatal("more than 100 pages, the cursor does not advance")
		}
		page, err := store.ListNotifications(ctx, f)
		if err != nil {
			t.Fatalf("ListNotifications: %v", err)
		}
		if len(page.Items) > f.Limit {
			t.Fatalf("page has %d items, limit is %d", len(page.Items), f.Limit)
		}
		if len(page.Items) == 0 && pages > 0 {
			t.Fatal("got an empty page after a page that had a next cursor")
		}
		for _, n := range page.Items {
			ids = append(ids, n.ID)
		}
		if page.Next == nil {
			return ids
		}
		if len(page.Items) < f.Limit {
			t.Fatalf("a page with %d of %d items has a next cursor", len(page.Items), f.Limit)
		}
		// Go through the opaque form, as API clients do.
		next, err := repository.DecodeCursor(page.Next.Encode())
		if err != nil {
			t.Fatalf("DecodeCursor: %v", err)
		}
		f.After = &next
	}
}
