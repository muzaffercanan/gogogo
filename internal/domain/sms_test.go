package domain_test

import (
	"strings"
	"testing"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

func TestMeasureSMS(t *testing.T) {
	const (
		gsm7 = domain.SMSEncodingGSM7
		ucs2 = domain.SMSEncodingUCS2
	)
	a := func(n int) string { return strings.Repeat("a", n) } // one GSM-7 septet
	s := func(n int) string { return strings.Repeat("ş", n) } // one UCS-2 unit
	e := func(n int) string { return strings.Repeat("€", n) } // two GSM-7 septets
	u := func(n int) string { return strings.Repeat("👍", n) } // two UCS-2 units

	tests := []struct {
		name string
		text string
		want domain.SMSSize
	}{
		{"empty", "", domain.SMSSize{Encoding: gsm7}},
		{"ascii", "Hello", domain.SMSSize{Encoding: gsm7, Units: 5, Segments: 1}},
		{"Turkish letters in the GSM alphabet", "ÇÖÜöü", domain.SMSSize{Encoding: gsm7, Units: 5, Segments: 1}},
		{"GSM symbols", "@£$¥¤§¡¿_\n\r", domain.SMSSize{Encoding: gsm7, Units: 11, Segments: 1}},
		{"Turkish OTP", "Doğrulama kodunuz: 482913", domain.SMSSize{Encoding: ucs2, Units: 25, Segments: 1}},

		// Characters outside the GSM alphabet switch the whole text to UCS-2.
		{"small c cedilla", "ç", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"dotless i", "ı", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"capital dotted I", "İ", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"backtick", "`", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"tab", "\t", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"escape is not a character", "\x1b", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"invalid UTF-8 becomes U+FFFD", "\xff", domain.SMSSize{Encoding: ucs2, Units: 1, Segments: 1}},
		{"one UCS-2 character at the end", a(160) + "ş", domain.SMSSize{Encoding: ucs2, Units: 161, Segments: 3}},

		// GSM-7: 160 septets in one SMS, 153 per segment.
		{"GSM-7 single full", a(160), domain.SMSSize{Encoding: gsm7, Units: 160, Segments: 1}},
		{"GSM-7 one over single", a(161), domain.SMSSize{Encoding: gsm7, Units: 161, Segments: 2}},
		{"GSM-7 two full segments", a(306), domain.SMSSize{Encoding: gsm7, Units: 306, Segments: 2}},
		{"GSM-7 one over two segments", a(307), domain.SMSSize{Encoding: gsm7, Units: 307, Segments: 3}},
		{"GSM-7 six full segments", a(918), domain.SMSSize{Encoding: gsm7, Units: 918, Segments: 6}},
		{"GSM-7 one over six segments", a(919), domain.SMSSize{Encoding: gsm7, Units: 919, Segments: 7}},

		// The extension table takes two septets each, and an escape sequence is never split.
		{"all extension characters", "\f^{}\\[~]|€", domain.SMSSize{Encoding: gsm7, Units: 20, Segments: 1}},
		{"extension fills a single SMS", e(80), domain.SMSSize{Encoding: gsm7, Units: 160, Segments: 1}},
		{"extension pushes past a single SMS", a(159) + "€", domain.SMSSize{Encoding: gsm7, Units: 161, Segments: 2}},
		{"extension not split across segments", a(152) + "€" + a(152), domain.SMSSize{Encoding: gsm7, Units: 306, Segments: 3}},
		{"extension ending a segment exactly", a(151) + "€" + a(153), domain.SMSSize{Encoding: gsm7, Units: 306, Segments: 2}},

		// UCS-2: 70 units in one SMS, 67 per segment.
		{"UCS-2 single full", s(70), domain.SMSSize{Encoding: ucs2, Units: 70, Segments: 1}},
		{"UCS-2 one over single", s(71), domain.SMSSize{Encoding: ucs2, Units: 71, Segments: 2}},
		{"UCS-2 two full segments", s(134), domain.SMSSize{Encoding: ucs2, Units: 134, Segments: 2}},
		{"UCS-2 one over two segments", s(135), domain.SMSSize{Encoding: ucs2, Units: 135, Segments: 3}},
		{"UCS-2 six full segments", s(402), domain.SMSSize{Encoding: ucs2, Units: 402, Segments: 6}},
		{"UCS-2 one over six segments", s(403), domain.SMSSize{Encoding: ucs2, Units: 403, Segments: 7}},

		// Outside the Basic Multilingual Plane: two UTF-16 units, and a surrogate pair is never split.
		{"emoji in GSM text", "Kodunuz 4829 👍", domain.SMSSize{Encoding: ucs2, Units: 15, Segments: 1}},
		{"emoji fill a single SMS", u(35), domain.SMSSize{Encoding: ucs2, Units: 70, Segments: 1}},
		{"emoji pushes past a single SMS", s(69) + "👍", domain.SMSSize{Encoding: ucs2, Units: 71, Segments: 2}},
		{"surrogate pair not split across segments", s(66) + "👍" + s(66), domain.SMSSize{Encoding: ucs2, Units: 134, Segments: 3}},
		{"surrogate pair ending a segment exactly", s(65) + "👍" + s(67), domain.SMSSize{Encoding: ucs2, Units: 134, Segments: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := domain.MeasureSMS(tt.text); got != tt.want {
				t.Errorf("MeasureSMS(%.20q) = %+v, want %+v", tt.text, got, tt.want)
			}
		})
	}
}
