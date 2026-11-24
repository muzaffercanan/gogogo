# 0014. Provider response classification and Retry-After

- **Status:** Accepted. Amends [0009](0009-retry-and-failure-handling.md).
- **Date:** 2026-11-24

## Context

ADR 0009 retries 5xx, 429, timeouts and connection errors, treats other 4xx and malformed provider
responses as permanent, and waits at least `Retry-After` on a 429. Writing the provider client left three
cases open:

- A 2xx whose body is not the expected `{"messageId": ...}`. A 2xx means the provider accepted the
  message, and the providers used here (webhook.site) ignore `Idempotency-Key`.
- 4xx statuses other than 429 that describe a transient condition, such as 408 Request Timeout.
- A `Retry-After` far larger than the 60 s backoff cap, or one that is plainly wrong.

## Decision

- **A 2xx is an acceptance whatever its body.** The notification becomes `sent`; if the body could not be
  read, `provider_message_id` stays empty, and the worker logs a warning and counts it.
- **Retryable statuses:** 5xx, 429 and 408. All other non-2xx answers are permanent, including 3xx:
  redirects are not followed.
- **Retry-After** is read on retryable answers, as seconds or an HTTP date (RFC 9110). The retry delay
  is at least that value, clamped to one hour.
- The failure reason stored in `last_error` is clipped to 200 characters of valid UTF-8 without NUL bytes,
  which PostgreSQL text rejects.

## Alternatives considered

- **Malformed 2xx as a permanent failure (0009 as written).** Marks a message that was most likely
  delivered as failed, and resending it from the dead-letter queue would deliver it twice.
- **Malformed 2xx as retryable.** Sends an accepted message again: a duplicate with providers that ignore
  the idempotency key.
- **Only 429 among 4xx.** Turns a transient 408 into a permanent failure.
- **Retry-After capped at the 60 s backoff cap.** Retries before the provider asked and burns an attempt
  on another 429. **Unbounded Retry-After:** a bogus value would hold a notification indefinitely.

## Consequences

- If a provider answers 2xx without actually taking the message, the notification shows as `sent`; the
  malformed-response counter is the only signal.
- A throttled notification can wait up to an hour before its next attempt.
