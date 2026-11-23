package repository

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/muzaffercanan/gogogo/internal/domain"
	"github.com/muzaffercanan/gogogo/internal/repository/sqlc"
)

// ErrInvalidCursor reports a cursor string that was not produced by Cursor.Encode.
var ErrInvalidCursor = errors.New("invalid cursor")

// Cursor is a position in a list of notifications ordered by (created_at, id). Rows created in one
// transaction share created_at, so the ID breaks ties and every position is unique.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// cursorLen is 8 bytes of Unix microseconds (PostgreSQL's timestamp precision) and 16 bytes of UUID.
const cursorLen = 8 + 16

// Encode returns c as an opaque, URL-safe string.
func (c Cursor) Encode() string {
	var b [cursorLen]byte
	binary.BigEndian.PutUint64(b[:8], uint64(c.CreatedAt.UnixMicro()))
	copy(b[8:], c.ID[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// DecodeCursor parses a string produced by Cursor.Encode. It returns an error wrapping ErrInvalidCursor
// for anything else.
func DecodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != cursorLen {
		return Cursor{}, fmt.Errorf("%w: %q", ErrInvalidCursor, s)
	}
	micros := int64(binary.BigEndian.Uint64(b[:8])) //nolint:gosec // G115: inverse of the conversion in Encode
	id, err := uuid.FromBytes(b[8:])
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %q", ErrInvalidCursor, s)
	}
	return Cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: id}, nil
}

// ListFilter selects and pages notifications. Zero values mean "no filter".
type ListFilter struct {
	Status  domain.Status
	Channel domain.Channel
	// CreatedFrom is inclusive; CreatedTo is exclusive.
	CreatedFrom time.Time
	CreatedTo   time.Time
	// After continues a previous page: only notifications after this position are returned.
	After *Cursor
	// Limit is the page size; it must be positive.
	Limit int
}

// NotificationPage is one page of a notification list.
type NotificationPage struct {
	Items []domain.Notification
	// Next is the position to continue from, or nil on the last page.
	Next *Cursor
}

// notificationColumns lists the columns in the order scanNotification reads them.
const notificationColumns = `id, batch_id, idempotency_key, request_hash, channel, priority, recipient, content,
	status, attempts, max_attempts, last_error, provider_message_id, correlation_id, scheduled_at,
	sending_started_at, created_at, updated_at, sent_at`

// ListNotifications returns notifications that match f, newest first.
//
// The query is assembled from only the conditions that are set, instead of one static query with
// "$1 IS NULL OR column = $1" for every filter. Once PostgreSQL switches a prepared statement to its
// generic plan, such disjunctions cannot use the (status, created_at, id) or (channel, status, created_at,
// id) indexes: the plan walks the created_at index and filters, which reads ~100 rows per row returned for
// a status that 1% of rows have. Every shape built here is an index range scan without a sort step.
// Pagination is keyset-based on (created_at, id), so a page costs the same however deep it is and rows
// created meanwhile do not shift later pages.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) (NotificationPage, error) {
	if f.Limit <= 0 {
		return NotificationPage{}, fmt.Errorf("list limit must be positive, got %d", f.Limit)
	}
	query, args := buildListQuery(f)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return NotificationPage{}, fmt.Errorf("listing notifications: %w", err)
	}
	items, err := pgx.CollectRows(rows, scanNotification)
	if err != nil {
		return NotificationPage{}, fmt.Errorf("reading notification list: %w", err)
	}

	page := NotificationPage{Items: items}
	// The query asks for one row more than the limit to learn whether another page exists.
	if len(items) > f.Limit {
		page.Items = items[:f.Limit]
		last := page.Items[f.Limit-1]
		page.Next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

// buildListQuery returns the SQL and arguments for f. Only placeholders are added for values, never the
// values themselves.
func buildListQuery(f ListFilter) (string, []any) {
	var (
		conds []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if f.Status != "" {
		conds = append(conds, "status = "+arg(string(f.Status)))
	}
	if f.Channel != "" {
		conds = append(conds, "channel = "+arg(string(f.Channel)))
	}
	if !f.CreatedFrom.IsZero() {
		conds = append(conds, "created_at >= "+arg(f.CreatedFrom))
	}
	if !f.CreatedTo.IsZero() {
		conds = append(conds, "created_at < "+arg(f.CreatedTo))
	}
	if f.After != nil {
		conds = append(conds, "(created_at, id) < ("+arg(f.After.CreatedAt)+", "+arg(f.After.ID)+")")
	}

	var b strings.Builder
	b.WriteString("SELECT " + notificationColumns + " FROM notifications")
	if len(conds) > 0 {
		b.WriteString(" WHERE " + strings.Join(conds, " AND "))
	}
	b.WriteString(" ORDER BY created_at DESC, id DESC LIMIT " + arg(f.Limit+1))
	return b.String(), args
}

func scanNotification(row pgx.CollectableRow) (domain.Notification, error) {
	var n sqlc.Notification
	err := row.Scan(
		&n.ID, &n.BatchID, &n.IdempotencyKey, &n.RequestHash, &n.Channel, &n.Priority, &n.Recipient,
		&n.Content, &n.Status, &n.Attempts, &n.MaxAttempts, &n.LastError, &n.ProviderMessageID,
		&n.CorrelationID, &n.ScheduledAt, &n.SendingStartedAt, &n.CreatedAt, &n.UpdatedAt, &n.SentAt,
	)
	if err != nil {
		return domain.Notification{}, err
	}
	return toDomain(n), nil
}
