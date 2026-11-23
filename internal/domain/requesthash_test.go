package domain_test

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

var at = time.Date(2026, 12, 1, 6, 0, 0, 0, time.UTC)

func request(modify func(*domain.NotificationRequest)) domain.NotificationRequest {
	r := domain.NotificationRequest{
		Channel:   domain.ChannelSMS,
		Recipient: "+905551234567",
		Content:   "Doğrulama kodunuz: 482913",
		Priority:  domain.PriorityHigh,
	}
	if modify != nil {
		modify(&r)
	}
	return r
}

// TestHashCanonicalForm pins the canonical form. If it fails, stored idempotency keys no longer match
// replays of their requests: change the form only by adding omitempty fields.
func TestHashCanonicalForm(t *testing.T) {
	scheduled := request(func(r *domain.NotificationRequest) { r.ScheduledAt = &at })
	tests := []struct {
		name string
		got  []byte
		json string
	}{
		{
			"notification",
			request(nil).Hash(),
			`{"channel":"sms","recipient":"+905551234567","content":"Doğrulama kodunuz: 482913","priority":"high"}`,
		},
		{
			"scheduled notification",
			scheduled.Hash(),
			`{"channel":"sms","recipient":"+905551234567","content":"Doğrulama kodunuz: 482913","priority":"high","scheduled_at":"2026-12-01T06:00:00Z"}`,
		},
		{
			"batch",
			domain.BatchHash([]domain.NotificationRequest{request(nil), scheduled}),
			`{"notifications":[` +
				`{"channel":"sms","recipient":"+905551234567","content":"Doğrulama kodunuz: 482913","priority":"high"},` +
				`{"channel":"sms","recipient":"+905551234567","content":"Doğrulama kodunuz: 482913","priority":"high","scheduled_at":"2026-12-01T06:00:00Z"}` +
				`]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if want := sha256.Sum256([]byte(tt.json)); !bytes.Equal(tt.got, want[:]) {
				t.Errorf("hash = %x, want the SHA-256 of %s (%x)", tt.got, tt.json, want)
			}
		})
	}
}

func TestHashTreatsEquivalentRequestsAlike(t *testing.T) {
	trt := time.FixedZone("TRT", 3*60*60)
	tests := []struct {
		name string
		a, b domain.NotificationRequest
	}{
		{
			"omitted priority is normal",
			request(func(r *domain.NotificationRequest) { r.Priority = "" }),
			request(func(r *domain.NotificationRequest) { r.Priority = domain.PriorityNormal }),
		},
		{
			"same instant in another time zone",
			request(func(r *domain.NotificationRequest) { r.ScheduledAt = &at }),
			request(func(r *domain.NotificationRequest) { local := at.In(trt); r.ScheduledAt = &local }),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !bytes.Equal(tt.a.Hash(), tt.b.Hash()) {
				t.Errorf("Hash differs for %+v and %+v", tt.a, tt.b)
			}
			if !bytes.Equal(domain.BatchHash([]domain.NotificationRequest{tt.a}), domain.BatchHash([]domain.NotificationRequest{tt.b})) {
				t.Errorf("BatchHash differs for %+v and %+v", tt.a, tt.b)
			}
		})
	}
}

func TestHashTellsDifferentRequestsApart(t *testing.T) {
	base := request(func(r *domain.NotificationRequest) { r.ScheduledAt = &at })
	later := at.Add(time.Microsecond)
	tests := []struct {
		name   string
		modify func(*domain.NotificationRequest)
	}{
		{"channel", func(r *domain.NotificationRequest) { r.Channel = domain.ChannelPush }},
		{"recipient", func(r *domain.NotificationRequest) { r.Recipient = "+905551234568" }},
		{"content", func(r *domain.NotificationRequest) { r.Content = "doğrulama kodunuz: 482913" }},
		{"priority", func(r *domain.NotificationRequest) { r.Priority = domain.PriorityNormal }},
		{"scheduled_at by a microsecond", func(r *domain.NotificationRequest) { r.ScheduledAt = &later }},
		{"scheduled_at removed", func(r *domain.NotificationRequest) { r.ScheduledAt = nil }},
		{"text moved between fields", func(r *domain.NotificationRequest) {
			r.Recipient, r.Content = "+905551234567D", "oğrulama kodunuz: 482913"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other := base
			tt.modify(&other)
			if bytes.Equal(base.Hash(), other.Hash()) {
				t.Errorf("Hash is equal for %+v and %+v", base, other)
			}
		})
	}
}

func TestBatchHashTellsDifferentBatchesApart(t *testing.T) {
	a := request(nil)
	b := request(func(r *domain.NotificationRequest) { r.Channel = "fax" }) // invalid items count too
	tests := []struct {
		name string
		x, y []domain.NotificationRequest
	}{
		{"order", []domain.NotificationRequest{a, b}, []domain.NotificationRequest{b, a}},
		{"extra item", []domain.NotificationRequest{a}, []domain.NotificationRequest{a, b}},
		{"repeated item", []domain.NotificationRequest{a}, []domain.NotificationRequest{a, a}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if bytes.Equal(domain.BatchHash(tt.x), domain.BatchHash(tt.y)) {
				t.Errorf("BatchHash is equal for %v and %v", tt.x, tt.y)
			}
		})
	}
}
