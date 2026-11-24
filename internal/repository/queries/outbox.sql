-- name: InsertOutbox :exec
-- Requests a dispatch of the notification once available_at has passed (first send, scheduled send
-- or retry). A NULL available_at means now by the database clock, the clock the relay compares against.
INSERT INTO outbox (notification_id, payload, available_at)
VALUES (@notification_id, @payload, COALESCE(sqlc.narg(available_at)::timestamptz, now()));

-- name: NotifyOutbox :exec
-- Wakes up relays listening on the outbox channel. Inside a transaction, PostgreSQL delivers the
-- notification only when the transaction commits.
SELECT pg_notify('outbox', '');

-- name: ClaimDueOutbox :many
-- Locks up to row_limit due outbox rows in available_at order, skipping rows another relay is
-- already dispatching.
SELECT id, notification_id, payload
FROM outbox
WHERE available_at <= now()
ORDER BY available_at, id
LIMIT @row_limit
FOR UPDATE SKIP LOCKED;

-- name: QueueNotifications :many
-- Moves notifications that are currently in one of from_statuses to queued and records a status
-- event for each one, in one statement. Notifications in any other status (for example cancelled)
-- are left untouched and omitted from the returned IDs. Rows are locked in ID order so concurrent
-- transactions cannot deadlock.
WITH locked AS (
    SELECT cur.id, cur.status
    FROM notifications AS cur
    WHERE cur.id = ANY(@ids::uuid[]) AND cur.status = ANY(@from_statuses::text[])
    ORDER BY cur.id
    FOR UPDATE
), updated AS (
    UPDATE notifications AS n
    SET status     = 'queued',
        updated_at = now()
    FROM locked
    WHERE n.id = locked.id
    RETURNING n.id, locked.status AS from_status
), events AS (
    INSERT INTO notification_events (notification_id, from_status, to_status)
    SELECT updated.id, updated.from_status, 'queued'
    FROM updated
)
SELECT id FROM updated;

-- name: DeleteOutbox :exec
-- Deletes outbox rows that have been processed (published, or skipped because the notification was
-- already cancelled).
DELETE FROM outbox
WHERE id = ANY(@ids::bigint[]);

-- name: ReapExpiredSending :many
-- Moves up to row_limit deliveries whose sending lease has expired to retry_status (when
-- attempts < max_attempts) or failed_status (when attempts are exhausted) and records a status event
-- for each one, in one statement.
WITH expired AS (
    SELECT cur.id
    FROM notifications AS cur
    WHERE cur.status = 'sending'
      AND cur.sending_started_at <= now() - make_interval(secs => @lease_seconds::double precision)
    ORDER BY cur.sending_started_at, cur.id
    LIMIT @row_limit
    FOR UPDATE SKIP LOCKED
), updated AS (
    UPDATE notifications AS n
    SET status             = CASE WHEN n.attempts < n.max_attempts THEN @retry_status::text ELSE @failed_status::text END,
        sending_started_at = NULL,
        last_error         = @last_error::text,
        updated_at         = now()
    FROM expired
    WHERE n.id = expired.id
    RETURNING n.id, n.channel, n.priority, n.correlation_id, n.status
), events AS (
    INSERT INTO notification_events (notification_id, from_status, to_status, reason)
    SELECT updated.id, 'sending', updated.status, @reason::text
    FROM updated
)
SELECT id, channel, priority, correlation_id, status
FROM updated;

-- name: InsertDueOutboxBatch :exec
-- Inserts immediately due outbox rows (available_at = now()) for a batch of notifications in one
-- statement.
INSERT INTO outbox (notification_id, payload, available_at)
SELECT t.id, t.payload::jsonb, now()
FROM (
    SELECT unnest(@ids::uuid[]) AS id, unnest(@payloads::text[]) AS payload
) AS t;

-- name: CountOutboxBacklog :one
-- Counts outbox rows that are due (available_at has passed) and scheduled (available_at is still in
-- the future), for the outbox_backlog metric.
SELECT count(*) FILTER (WHERE available_at <= now()) AS due,
       count(*) FILTER (WHERE available_at > now())  AS scheduled
FROM outbox;
