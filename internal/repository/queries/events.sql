-- name: InsertNotificationEvent :exec
-- Appends a status transition to the audit trail. from_status is NULL for the initial pending event.
INSERT INTO notification_events (notification_id, from_status, to_status, reason)
VALUES (@notification_id, @from_status, @to_status, @reason);
