-- The per-item outcome of a batch request (accepted with its notification ID, or rejected with its field
-- errors), so that a request repeated with the same Idempotency-Key gets the original result back.
-- Rejected items are not stored anywhere else.
ALTER TABLE batches ADD COLUMN item_results jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE batches ALTER COLUMN item_results DROP DEFAULT;
