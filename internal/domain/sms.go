package domain

import "strings"

// SMSEncoding is the character encoding an SMS text is sent with.
type SMSEncoding string

// SMS encodings.
const (
	// SMSEncodingGSM7 is the GSM 7-bit default alphabet with its extension table (3GPP TS 23.038).
	SMSEncodingGSM7 SMSEncoding = "gsm7"
	// SMSEncodingUCS2 is used as soon as one character is outside GSM-7. Characters outside the Basic
	// Multilingual Plane, such as most emoji, are sent as UTF-16 surrogate pairs, as providers do.
	SMSEncodingUCS2 SMSEncoding = "ucs2"
)

// Capacity in encoding units of a single SMS and of each part of a concatenated SMS, whose header takes
// the rest.
const (
	gsm7SingleUnits = 160
	gsm7PartUnits   = 153
	ucs2SingleUnits = 70
	ucs2PartUnits   = 67
)

// The GSM 7-bit default alphabet and its extension table, as Unicode. A character of the extension
// table takes two septets: an escape and the character itself. The escape is not a character of its
// own. 0x09 is the capital Ç; the small ç is not in the alphabet.
const (
	gsm7Basic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
		"¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
	gsm7Extension = "\f^{}\\[~]|€"
)

// SMSSize describes how a text is encoded and split when it is sent as an SMS.
type SMSSize struct {
	Encoding SMSEncoding
	// Units is the length of the text in units of Encoding: septets for GSM-7, where a character of the
	// extension table takes two, and UTF-16 code units for UCS-2, where a character outside the Basic
	// Multilingual Plane takes two.
	Units int
	// Segments is the number of SMS the text is sent as, 0 for an empty text.
	Segments int
}

// MeasureSMS returns how text is sent as an SMS. The text is GSM-7 if every character is in the GSM
// 7-bit alphabet or its extension table, and UCS-2 otherwise.
//
// A text of at most 160 GSM-7 or 70 UCS-2 units is one segment; a longer one is split into segments of
// at most 153 or 67 units. A character is never split across two segments, so a segment closes one unit
// early when the next character takes two units and only one is left.
func MeasureSMS(text string) SMSSize {
	enc, single, part, width := SMSEncodingGSM7, gsm7SingleUnits, gsm7PartUnits, gsm7Width
	if !isGSM7(text) {
		enc, single, part, width = SMSEncodingUCS2, ucs2SingleUnits, ucs2PartUnits, ucs2Width
	}

	units := 0
	for _, r := range text {
		units += width(r)
	}
	switch {
	case units == 0:
		return SMSSize{Encoding: enc}
	case units <= single:
		return SMSSize{Encoding: enc, Units: units, Segments: 1}
	}

	segments, used := 1, 0
	for _, r := range text {
		w := width(r)
		if used+w > part {
			segments++
			used = 0
		}
		used += w
	}
	return SMSSize{Encoding: enc, Units: units, Segments: segments}
}

func isGSM7(text string) bool {
	for _, r := range text {
		if !strings.ContainsRune(gsm7Basic, r) && !strings.ContainsRune(gsm7Extension, r) {
			return false
		}
	}
	return true
}

// gsm7Width returns the septets that r takes; r must be a GSM-7 character.
func gsm7Width(r rune) int {
	if strings.ContainsRune(gsm7Extension, r) {
		return 2
	}
	return 1
}

// ucs2Width returns the UTF-16 code units that r takes. Ranging over a string yields only valid runes
// (invalid UTF-8 becomes U+FFFD), so r is never a lone surrogate.
func ucs2Width(r rune) int {
	if r > 0xFFFF {
		return 2
	}
	return 1
}
