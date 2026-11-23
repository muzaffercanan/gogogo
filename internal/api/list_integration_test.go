//go:build integration

package api_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// seedList creates one notification per entry (channel, status, created_at) and returns their IDs in
// the same order. Statuses and creation times are set directly, as if time had passed.
func seedList(t *testing.T, a *testAPI, rows []seedRow) []string {
	t.Helper()
	recipients := map[string]string{"sms": "+905551234567", "email": "jane@example.com", "push": "tok"}
	ids := make([]string, len(rows))
	for i, r := range rows {
		resp := a.do(t, http.MethodPost, "/v1/notifications",
			fmt.Sprintf(`{"channel":%q,"recipient":%q,"content":"n%d"}`, r.channel, recipients[r.channel], i))
		if resp.status != http.StatusAccepted {
			t.Fatalf("creating row %d: %d %s", i, resp.status, resp.raw)
		}
		ids[i] = fmt.Sprint(resp.body["id"])
		if _, err := a.db.Exec(t.Context(), `UPDATE notifications SET status = $2, created_at = $3 WHERE id = $1`,
			ids[i], r.status, r.createdAt); err != nil {
			t.Fatalf("seeding row %d: %v", i, err)
		}
	}
	return ids
}

type seedRow struct {
	channel, status string
	createdAt       time.Time
}

func listIDs(t *testing.T, a *testAPI, query string) (ids []string, next any) {
	t.Helper()
	resp := a.do(t, http.MethodGet, "/v1/notifications?"+query, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET ?%s = %d %s", query, resp.status, resp.raw)
	}
	next, present := resp.body["next_cursor"]
	if !present {
		t.Fatalf("GET ?%s: no next_cursor in %s", query, resp.raw)
	}
	items, _ := resp.body["items"].([]any)
	if items == nil {
		t.Fatalf("GET ?%s: items is not an array: %s", query, resp.raw)
	}
	for _, it := range items {
		ids = append(ids, fmt.Sprint(it.(map[string]any)["id"]))
	}
	return ids, next
}

func TestListNotifications(t *testing.T) {
	a := newAPI(t)
	day := func(d int) time.Time { return time.Date(2026, 11, d, 12, 0, 0, 0, time.UTC) }
	ids := seedList(t, a, []seedRow{
		{"sms", "pending", day(1)},
		{"email", "queued", day(2)},
		{"sms", "queued", day(3)},
		{"push", "sent", day(4)},
		{"sms", "sent", day(5)},
	})
	tests := []struct {
		name  string
		query string
		want  []int // indexes into ids, in the expected order
	}{
		{"everything, newest first", "", []int{4, 3, 2, 1, 0}},
		{"by channel", "channel=sms", []int{4, 2, 0}},
		{"by status", "status=queued", []int{2, 1}},
		{"by channel and status", "channel=sms&status=sent", []int{4}},
		{"from is inclusive, to exclusive", "created_from=" + url.QueryEscape(day(2).Format(time.RFC3339)) +
			"&created_to=" + url.QueryEscape(day(4).Format(time.RFC3339)), []int{2, 1}},
		{"offsets are honoured", "created_from=" + url.QueryEscape("2026-11-05T14:00:00+03:00"), []int{4}},
		{"nothing matches", "status=cancelled", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, next := listIDs(t, a, tt.query)
			var want []string
			for _, i := range tt.want {
				want = append(want, ids[i])
			}
			if fmt.Sprint(got) != fmt.Sprint(want) || next != nil {
				t.Errorf("ids = %v, next_cursor %v; want %v and null", got, next, want)
			}
		})
	}
}

func TestListNotificationsPages(t *testing.T) {
	a := newAPI(t)
	var rows []seedRow
	for d := 1; d <= 5; d++ {
		rows = append(rows, seedRow{"sms", "pending", time.Date(2026, 11, d, 12, 0, 0, 0, time.UTC)})
	}
	ids := seedList(t, a, rows)

	var got []string
	query := "channel=sms&limit=2"
	for page := 1; ; page++ {
		pageIDs, next := listIDs(t, a, query)
		got = append(got, pageIDs...)
		if next == nil {
			if page != 3 || len(pageIDs) != 1 {
				t.Errorf("last page is page %d with %d items, want page 3 with 1", page, len(pageIDs))
			}
			break
		}
		if page > 3 {
			t.Fatal("more than 3 pages")
		}
		query = "channel=sms&limit=2&cursor=" + url.QueryEscape(fmt.Sprint(next))
	}
	want := []string{ids[4], ids[3], ids[2], ids[1], ids[0]}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("pages hold %v, want every notification once, newest first: %v", got, want)
	}
}

func TestListNotificationsRejectsBadParameters(t *testing.T) {
	a := newAPI(t)
	tests := []struct {
		query      string
		wantErrors []string
	}{
		{"status=delivered", []string{"status invalid_value"}},
		{"channel=fax", []string{"channel invalid_value"}},
		{"limit=0", []string{"limit invalid_value"}},
		{"limit=101", []string{"limit invalid_value"}},
		{"limit=ten", []string{"limit invalid_format"}},
		{"cursor=not-a-cursor", []string{"cursor invalid_value"}},
		{"cursor=", []string{"cursor invalid_value"}},
		{"created_from=yesterday", []string{"created_from invalid_format"}},
		{"status=delivered&channel=fax&limit=500", []string{"status invalid_value", "channel invalid_value", "limit invalid_value"}},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			resp := a.do(t, http.MethodGet, "/v1/notifications?"+tt.query, "")
			if resp.status != http.StatusBadRequest || fmt.Sprint(fieldErrors(resp.body)) != fmt.Sprint(tt.wantErrors) {
				t.Errorf("response = %d %v, want 400 %v", resp.status, fieldErrors(resp.body), tt.wantErrors)
			}
		})
	}
}
