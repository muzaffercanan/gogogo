package domain_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

var now = time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

// valid returns a valid request for channel; modify adjusts it.
func valid(channel domain.Channel, modify func(*domain.NotificationRequest)) domain.NotificationRequest {
	r := domain.NotificationRequest{Channel: channel, Content: "Doğrulama kodunuz: 482913"}
	switch channel {
	case domain.ChannelSMS:
		r.Recipient = "+905551234567"
	case domain.ChannelEmail:
		r.Recipient = "jane@example.com"
	case domain.ChannelPush:
		r.Recipient = "f3c9a1b2e4d5c6b7a8f9e0d1c2b3a4f5"
	}
	if modify != nil {
		modify(&r)
	}
	return r
}

func recipient(s string) func(*domain.NotificationRequest) {
	return func(r *domain.NotificationRequest) { r.Recipient = s }
}

func content(s string) func(*domain.NotificationRequest) {
	return func(r *domain.NotificationRequest) { r.Content = s }
}

func scheduledAt(t time.Time) func(*domain.NotificationRequest) {
	return func(r *domain.NotificationRequest) { r.ScheduledAt = &t }
}

func TestValidate(t *testing.T) {
	const (
		sms   = domain.ChannelSMS
		email = domain.ChannelEmail
		push  = domain.ChannelPush
	)
	tests := []struct {
		name string
		req  domain.NotificationRequest
		// want lists the invalid fields as "field code"; empty means valid.
		want []string
	}{
		{"valid sms", valid(sms, nil), nil},
		{"valid email", valid(email, nil), nil},
		{"valid push", valid(push, nil), nil},
		{"every priority", valid(sms, func(r *domain.NotificationRequest) { r.Priority = domain.PriorityLow }), nil},
		{"empty request", domain.NotificationRequest{}, []string{"channel required", "recipient required", "content required"}},
		{"unknown channel skips channel rules", valid("fax", recipient("not a number")), []string{"channel invalid_value"}},
		{"unknown priority", valid(sms, func(r *domain.NotificationRequest) { r.Priority = "urgent" }), []string{"priority invalid_value"}},
		{"several fields at once", valid(sms, func(r *domain.NotificationRequest) {
			r.Recipient, r.Content, r.Priority = "05551234567", "", "urgent"
		}), []string{"recipient invalid_format", "content required", "priority invalid_value"}},

		// SMS recipients: E.164 format only.
		{"E.164 shortest", valid(sms, recipient("+90")), nil},
		{"E.164 longest", valid(sms, recipient("+123456789012345")), nil},
		{"E.164 too long", valid(sms, recipient("+1234567890123456")), []string{"recipient invalid_format"}},
		{"E.164 one digit", valid(sms, recipient("+9")), []string{"recipient invalid_format"}},
		{"E.164 without plus", valid(sms, recipient("905551234567")), []string{"recipient invalid_format"}},
		{"E.164 leading zero", valid(sms, recipient("+05551234567")), []string{"recipient invalid_format"}},
		{"E.164 with spaces", valid(sms, recipient("+90 555 123 4567")), []string{"recipient invalid_format"}},
		{"E.164 with a letter", valid(sms, recipient("+90555123456a")), []string{"recipient invalid_format"}},
		{"E.164 with a newline", valid(sms, recipient("+905551234567\n")), []string{"recipient invalid_format"}},

		// Email recipients: one bare address.
		{"email with tag and subdomain", valid(email, recipient("jane.doe+news@mail.example.co.uk")), nil},
		{"email with a local domain", valid(email, recipient("jane@localhost")), nil},
		{"email with display name", valid(email, recipient("Jane <jane@example.com>")), []string{"recipient invalid_format"}},
		{"email in angle brackets", valid(email, recipient("<jane@example.com>")), []string{"recipient invalid_format"}},
		{"email with surrounding space", valid(email, recipient(" jane@example.com")), []string{"recipient invalid_format"}},
		{"two emails", valid(email, recipient("jane@example.com, joe@example.com")), []string{"recipient invalid_format"}},
		{"email without domain", valid(email, recipient("jane@")), []string{"recipient invalid_format"}},
		{"email without at sign", valid(email, recipient("jane.example.com")), []string{"recipient invalid_format"}},

		// Push recipients: a device token of at most 4096 bytes without whitespace.
		{"push token at the byte limit", valid(push, recipient(strings.Repeat("a", 4096))), nil},
		{"push token over the byte limit", valid(push, recipient(strings.Repeat("a", 4097))), []string{"recipient too_long"}},
		{"push token over the byte limit in fewer characters", valid(push, recipient(strings.Repeat("ş", 2049))), []string{"recipient too_long"}},
		{"push token with a space", valid(push, recipient("abc def")), []string{"recipient invalid_format"}},
		{"push token with a tab", valid(push, recipient("abc\tdef")), []string{"recipient invalid_format"}},
		{"push token with a no-break space", valid(push, recipient("abc def")), []string{"recipient invalid_format"}},
		{"recipient over 4096 characters", valid(email, recipient(strings.Repeat("ş", 4097))), []string{"recipient too_long"}},

		// Content limits per channel.
		{"sms at 6 GSM-7 segments", valid(sms, content(strings.Repeat("a", 918))), nil},
		{"sms at 7 GSM-7 segments", valid(sms, content(strings.Repeat("a", 919))), []string{"content too_long"}},
		{"sms at 6 UCS-2 segments", valid(sms, content(strings.Repeat("ş", 402))), nil},
		{"sms at 7 UCS-2 segments", valid(sms, content(strings.Repeat("ş", 403))), []string{"content too_long"}},
		{"email at the byte limit", valid(email, content(strings.Repeat("a", 102400))), nil},
		{"email over the byte limit", valid(email, content(strings.Repeat("a", 102401))), []string{"content too_long"}},
		{"push at the byte limit", valid(push, content(strings.Repeat("ş", 2048))), nil},
		{"push over the byte limit", valid(push, content(strings.Repeat("ş", 2048)+"a")), []string{"content too_long"}},
		{"push content is not limited like sms", valid(push, content(strings.Repeat("a", 919))), nil},

		// scheduled_at: in the future, at most 30 days ahead.
		{"scheduled a moment ahead", valid(sms, scheduledAt(now.Add(time.Nanosecond))), nil},
		{"scheduled exactly 30 days ahead", valid(sms, scheduledAt(now.Add(30*24*time.Hour))), nil},
		{"scheduled in another time zone", valid(sms, scheduledAt(now.Add(time.Hour).In(time.FixedZone("TRT", 3*60*60)))), nil},
		{"scheduled now", valid(sms, scheduledAt(now)), []string{"scheduled_at not_in_future"}},
		{"scheduled in the past", valid(sms, scheduledAt(now.Add(-time.Second))), []string{"scheduled_at not_in_future"}},
		{"scheduled past 30 days", valid(sms, scheduledAt(now.Add(30*24*time.Hour+time.Nanosecond))), []string{"scheduled_at too_far_in_future"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := invalidFields(t, tt.req.Validate(now)); fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("invalid fields = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateSMSTooLongMessage(t *testing.T) {
	tests := []struct {
		content string
		want    string
	}{
		{strings.Repeat("a", 919), "needs 7 SMS segments (GSM-7), the limit is 6"},
		{strings.Repeat("ş", 403), "needs 7 SMS segments (UCS-2), the limit is 6"},
	}
	for _, tt := range tests {
		err := valid(domain.ChannelSMS, content(tt.content)).Validate(now)
		var invalid *domain.ValidationError
		if !errors.As(err, &invalid) || len(invalid.Fields) != 1 || invalid.Fields[0].Message != tt.want {
			t.Errorf("Validate error = %v, want one content error %q", err, tt.want)
		}
	}
}

// invalidFields returns the fields err reports as "field code", failing the test if err is neither nil
// nor a *domain.ValidationError that lists fields with messages.
func invalidFields(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	var invalid *domain.ValidationError
	if !errors.As(err, &invalid) || len(invalid.Fields) == 0 {
		t.Fatalf("error = %v, want a *domain.ValidationError with fields", err)
	}
	var fields []string
	for _, f := range invalid.Fields {
		if f.Message == "" {
			t.Errorf("field %s has no message", f.Field)
		}
		fields = append(fields, f.Field+" "+string(f.Code))
	}
	return fields
}
