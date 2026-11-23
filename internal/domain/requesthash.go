package domain

import (
	"crypto/sha256"
	"encoding/json"
	"time"
)

// canonicalRequest is the form of a NotificationRequest that its hash is computed from: JSON with fixed
// field order, the default priority filled in and scheduled_at in UTC.
//
// Hashes are stored with idempotency keys, so changing this form makes a replay of an earlier request
// look like a different request (422). A field may only be added with omitempty, so that requests
// without it keep their hash.
type canonicalRequest struct {
	Channel     Channel  `json:"channel"`
	Recipient   string   `json:"recipient"`
	Content     string   `json:"content"`
	Priority    Priority `json:"priority"`
	ScheduledAt string   `json:"scheduled_at,omitempty"`
}

func (r NotificationRequest) canonical() canonicalRequest {
	c := canonicalRequest{Channel: r.Channel, Recipient: r.Recipient, Content: r.Content, Priority: r.Priority}
	if c.Priority == "" {
		c.Priority = PriorityNormal
	}
	if r.ScheduledAt != nil {
		c.ScheduledAt = r.ScheduledAt.UTC().Format(time.RFC3339Nano)
	}
	return c
}

// Hash returns the SHA-256 hash that identifies the request an Idempotency-Key was first used with.
//
// Requests that ask for the same notification have the same hash: an omitted priority equals normal,
// and scheduled_at times that denote the same instant are equal whatever their time zone offset. As
// the hash is computed from decoded values, the order of JSON fields, whitespace and unknown fields do
// not matter either. r need not be valid.
func (r NotificationRequest) Hash() []byte {
	return hashJSON(r.canonical())
}

// BatchHash returns the hash of a batch request with the given items, in order. It follows the rules of
// NotificationRequest.Hash, so invalid items count as they were sent.
func BatchHash(items []NotificationRequest) []byte {
	batch := struct {
		Notifications []canonicalRequest `json:"notifications"`
	}{Notifications: make([]canonicalRequest, len(items))}
	for i, item := range items {
		batch.Notifications[i] = item.canonical()
	}
	return hashJSON(batch)
}

func hashJSON(v any) []byte {
	// Marshal fails only for values JSON cannot represent; the canonical forms hold strings only.
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return sum[:]
}
