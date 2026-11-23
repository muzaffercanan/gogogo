-- name: InsertNotification :one
-- Inserts a new notification in status pending. Returns no row when the idempotency key is
-- already taken; the caller then reads the existing notification (ADR 0008).
INSERT INTO notifications (
    id, batch_id, idempotency_key, request_hash, channel, priority, recipient, content,
    status, max_attempts, correlation_id, scheduled_at
) VALUES (
    @id, @batch_id, @idempotency_key, @request_hash, @channel, @priority, @recipient, @content,
    'pending', @max_attempts, @correlation_id, @scheduled_at
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;

-- name: GetNotification :one
SELECT * FROM notifications
WHERE id = @id;

-- name: GetNotificationByIdempotencyKey :one
SELECT * FROM notifications
WHERE idempotency_key = @idempotency_key::text;

-- name: TransitionNotification :one
-- Moves a notification from one of the allowed statuses to to_status and records the status event, in
-- one statement. Returns no row when the notification does not exist or is in none of the allowed
-- statuses, so concurrent transitions of the same notification have exactly one winner.
-- Column side effects follow the target status: entering sending counts an attempt and starts the
-- lease, entering sent stamps sent_at, and leaving sending ends the lease.
WITH locked AS (
    SELECT cur.id, cur.status
    FROM notifications AS cur
    WHERE cur.id = @id AND cur.status = ANY(@from_statuses::text[])
    FOR UPDATE
), updated AS (
    UPDATE notifications AS n
    SET status              = @to_status::text,
        updated_at          = now(),
        attempts            = n.attempts + CASE WHEN @to_status::text = 'sending' THEN 1 ELSE 0 END,
        sending_started_at  = CASE WHEN @to_status::text = 'sending' THEN now() END,
        sent_at             = CASE WHEN @to_status::text = 'sent' THEN now() ELSE n.sent_at END,
        last_error          = COALESCE(sqlc.narg(last_error)::text, n.last_error),
        provider_message_id = COALESCE(sqlc.narg(provider_message_id)::text, n.provider_message_id)
    FROM locked
    WHERE n.id = locked.id
    RETURNING n.id, n.batch_id, n.idempotency_key, n.request_hash, n.channel, n.priority, n.recipient,
              n.content, n.status, n.attempts, n.max_attempts, n.last_error, n.provider_message_id,
              n.correlation_id, n.scheduled_at, n.sending_started_at, n.created_at, n.updated_at,
              n.sent_at, locked.status AS from_status
), event AS (
    INSERT INTO notification_events (notification_id, from_status, to_status, reason)
    SELECT updated.id, updated.from_status, updated.status, sqlc.narg(reason)::text
    FROM updated
)
SELECT id, batch_id, idempotency_key, request_hash, channel, priority, recipient, content, status,
       attempts, max_attempts, last_error, provider_message_id, correlation_id, scheduled_at,
       sending_started_at, created_at, updated_at, sent_at, from_status
FROM updated;
