-- name: InsertBatch :one
-- Inserts a batch with the outcome of each of its items. Returns no row when the idempotency key is
-- already taken; the caller then reads the existing batch (ADR 0008).
INSERT INTO batches (
    id, idempotency_key, request_hash, total_count, accepted_count, rejected_count, item_results
) VALUES (
    @id, @idempotency_key, @request_hash, @total_count, @accepted_count, @rejected_count, @item_results
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;

-- name: GetBatch :one
SELECT * FROM batches
WHERE id = @id;

-- name: GetBatchByIdempotencyKey :one
SELECT * FROM batches
WHERE idempotency_key = @idempotency_key::text;

-- name: InsertBatchNotifications :exec
-- Inserts the accepted notifications of a batch in one statement: the arrays hold one element per
-- notification. Arrays cannot hold NULL, so an empty correlation ID stands for none, and scheduled_ats is
-- only read where is_scheduled is true.
INSERT INTO notifications (
    id, batch_id, channel, priority, recipient, content, status, max_attempts, correlation_id, scheduled_at
)
SELECT t.id, @batch_id, t.channel, t.priority, t.recipient, t.content, 'pending', t.max_attempts,
       NULLIF(t.correlation_id, ''), CASE WHEN t.is_scheduled THEN t.scheduled_at END
FROM (
    -- Set-returning functions in one SELECT list advance together, pairing the arrays element by element.
    SELECT unnest(@ids::uuid[]) AS id, unnest(@channels::text[]) AS channel,
           unnest(@priorities::text[]) AS priority, unnest(@recipients::text[]) AS recipient,
           unnest(@contents::text[]) AS content, unnest(@max_attempts::integer[]) AS max_attempts,
           unnest(@correlation_ids::text[]) AS correlation_id, unnest(@is_scheduled::boolean[]) AS is_scheduled,
           unnest(@scheduled_ats::timestamptz[]) AS scheduled_at
) AS t;

-- name: InsertBatchOutbox :exec
-- Requests the dispatch of the accepted notifications of a batch, each once its scheduled time (or now,
-- by the database clock) has passed.
INSERT INTO outbox (notification_id, payload, available_at)
SELECT t.id, t.payload::jsonb, CASE WHEN t.is_scheduled THEN t.scheduled_at ELSE now() END
FROM (
    SELECT unnest(@ids::uuid[]) AS id, unnest(@payloads::text[]) AS payload,
           unnest(@is_scheduled::boolean[]) AS is_scheduled, unnest(@scheduled_ats::timestamptz[]) AS scheduled_at
) AS t;

-- name: InsertBatchEvents :exec
-- Records the initial pending status of every accepted notification of a batch.
INSERT INTO notification_events (notification_id, to_status)
SELECT t.id, 'pending'
FROM unnest(@ids::uuid[]) AS t(id);
