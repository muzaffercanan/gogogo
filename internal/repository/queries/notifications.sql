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
