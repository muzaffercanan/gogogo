-- name: InsertOutbox :exec
-- Requests a dispatch of the notification once available_at has passed (first send, scheduled send
-- or retry).
INSERT INTO outbox (notification_id, payload, available_at)
VALUES (@notification_id, @payload, @available_at);
