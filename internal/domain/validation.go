package domain

import (
	"fmt"
	"net/mail"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// NotificationRequest is a notification as a client asked for it, before validation.
type NotificationRequest struct {
	Channel   Channel
	Recipient string
	Content   string
	// Priority is "" when the client did not choose one.
	Priority Priority
	// ScheduledAt is nil when the notification is to be sent as soon as possible.
	ScheduledAt *time.Time
}

// Limits of the API contract (api/openapi.yaml). They are fixed rather than configurable, so the
// contract states them exactly.
const (
	maxRecipientChars    = 4096
	maxPushTokenBytes    = 4096
	maxSMSSegments       = 6
	maxEmailContentBytes = 100 * 1024
	maxPushContentBytes  = 4 * 1024
	maxScheduleAhead     = 30 * 24 * time.Hour
)

// Validate checks r against the rules of its channel. It returns a *ValidationError listing every
// invalid field, in the order channel, recipient, content, priority, scheduled_at, or nil if r is valid.
// now is the current time: scheduled_at must be after it and at most 30 days later.
//
// The recipient and the content are checked against the rules of the channel only when the channel is
// valid.
func (r NotificationRequest) Validate(now time.Time) error {
	var errs []FieldError
	add := func(field string, code FieldErrorCode, message string) {
		errs = append(errs, FieldError{Field: field, Code: code, Message: message})
	}

	switch r.Channel {
	case ChannelSMS, ChannelEmail, ChannelPush:
	case "":
		add("channel", CodeRequired, "is required")
	default:
		add("channel", CodeInvalidValue, "must be one of sms, email, push")
	}

	if code, msg := checkRecipient(r.Channel, r.Recipient); code != "" {
		add("recipient", code, msg)
	}
	if code, msg := checkContent(r.Channel, r.Content); code != "" {
		add("content", code, msg)
	}

	switch r.Priority {
	case "", PriorityHigh, PriorityNormal, PriorityLow:
	default:
		add("priority", CodeInvalidValue, "must be one of high, normal, low")
	}

	if at := r.ScheduledAt; at != nil {
		switch {
		case !at.After(now):
			add("scheduled_at", CodeNotInFuture, "must be in the future")
		case at.After(now.Add(maxScheduleAhead)):
			add("scheduled_at", CodeTooFarInFuture, "must be at most 30 days ahead")
		}
	}

	if len(errs) > 0 {
		return &ValidationError{Fields: errs}
	}
	return nil
}

// checkRecipient returns why recipient is invalid for channel, or an empty code if it is valid.
func checkRecipient(channel Channel, recipient string) (FieldErrorCode, string) {
	switch {
	case recipient == "":
		return CodeRequired, "is required"
	case utf8.RuneCountInString(recipient) > maxRecipientChars:
		return CodeTooLong, fmt.Sprintf("must be at most %d characters", maxRecipientChars)
	}
	switch channel {
	case ChannelSMS:
		if !isE164(recipient) {
			return CodeInvalidFormat, "must be an E.164 phone number, e.g. +905551234567"
		}
	case ChannelEmail:
		if !isEmailAddress(recipient) {
			return CodeInvalidFormat, "must be a single email address without a display name, e.g. jane@example.com"
		}
	case ChannelPush:
		switch {
		case len(recipient) > maxPushTokenBytes:
			return CodeTooLong, fmt.Sprintf("must be at most %d bytes", maxPushTokenBytes)
		case strings.ContainsFunc(recipient, unicode.IsSpace):
			return CodeInvalidFormat, "must be a device token without whitespace"
		}
	}
	return "", ""
}

// checkContent returns why content is invalid for channel, or an empty code if it is valid.
func checkContent(channel Channel, content string) (FieldErrorCode, string) {
	if content == "" {
		return CodeRequired, "is required"
	}
	switch channel {
	case ChannelSMS:
		if size := MeasureSMS(content); size.Segments > maxSMSSegments {
			return CodeTooLong, fmt.Sprintf("needs %d SMS segments (%s), the limit is %d",
				size.Segments, encodingName(size.Encoding), maxSMSSegments)
		}
	case ChannelEmail:
		if len(content) > maxEmailContentBytes {
			return CodeTooLong, fmt.Sprintf("must be at most %d bytes in UTF-8", maxEmailContentBytes)
		}
	case ChannelPush:
		if len(content) > maxPushContentBytes {
			return CodeTooLong, fmt.Sprintf("must be at most %d bytes in UTF-8", maxPushContentBytes)
		}
	}
	return "", ""
}

// isE164 reports whether s is a phone number in E.164 format: a plus sign and 2 to 15 digits, the first
// of which is not 0. It checks the format only, as the regular expression ^\+[1-9][0-9]{1,14}$ would,
// not whether the number exists.
func isE164(s string) bool {
	if len(s) < 3 || len(s) > 16 || s[0] != '+' || s[1] < '1' || s[1] > '9' {
		return false
	}
	for i := 2; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isEmailAddress reports whether s is exactly one RFC 5322 address, such as jane@example.com, without
// a display name, angle brackets, comments or surrounding spaces.
func isEmailAddress(s string) bool {
	addr, err := mail.ParseAddress(s)
	return err == nil && addr.Name == "" && addr.Address == s
}

func encodingName(e SMSEncoding) string {
	if e == SMSEncodingGSM7 {
		return "GSM-7"
	}
	return "UCS-2"
}
