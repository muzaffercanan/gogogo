package api

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

// TestToNotificationWritesUTC checks that responses do not depend on the time zone the database driver
// returns times in, which is the server's local zone.
func TestToNotificationWritesUTC(t *testing.T) {
	trt := time.FixedZone("TRT", 3*60*60)
	at := time.Date(2026, 12, 1, 12, 0, 0, 0, trt)

	got := toNotification(domain.Notification{
		ID: uuid.Must(uuid.NewV7()), ScheduledAt: &at, CreatedAt: at, UpdatedAt: at, SentAt: &at,
	})

	for name, ts := range map[string]time.Time{
		"scheduled_at": *got.ScheduledAt, "created_at": got.CreatedAt, "updated_at": got.UpdatedAt, "sent_at": *got.SentAt,
	} {
		if ts.Location() != time.UTC || !ts.Equal(at) {
			t.Errorf("%s = %v, want %v in UTC", name, ts, at.UTC())
		}
	}
}
