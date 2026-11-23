-- name: InsertOutbox :exec
-- Requests a dispatch of the notification once available_at has passed (first send, scheduled send
-- or retry). A NULL available_at means now by the database clock, the clock the relay compares against.
INSERT INTO outbox (notification_id, payload, available_at)
VALUES (@notification_id, @payload, COALESCE(sqlc.narg(available_at)::timestamptz, now()));

-- name: NotifyOutbox :exec
-- Wakes up relays listening on the outbox channel. Inside a transaction, PostgreSQL delivers the
-- notification only when the transaction commits.
SELECT pg_notify('outbox', '');
