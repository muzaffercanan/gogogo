package repository_test

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/muzaffercanan/gogogo/internal/repository"
)

func TestCursorRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		at   time.Time
	}{
		{"recent", time.Date(2026, 11, 23, 18, 4, 5, 123456000, time.UTC)},
		{"other time zone", time.Date(2026, 11, 23, 21, 4, 5, 123456000, time.FixedZone("TRT", 3*60*60))},
		{"before 1970", time.Date(1969, 7, 20, 20, 17, 0, 1000, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := repository.Cursor{CreatedAt: tt.at, ID: uuid.Must(uuid.NewV7())}

			out, err := repository.DecodeCursor(in.Encode())
			if err != nil {
				t.Fatalf("DecodeCursor: %v", err)
			}
			if !out.CreatedAt.Equal(in.CreatedAt) || out.ID != in.ID {
				t.Errorf("round trip = %+v, want %+v", out, in)
			}
		})
	}
}

func TestDecodeCursorRejectsForeignInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"not base64", "not a cursor!"},
		{"too short", base64.RawURLEncoding.EncodeToString(make([]byte, 23))},
		{"too long", base64.RawURLEncoding.EncodeToString(make([]byte, 25))},
		{"padded base64", base64.URLEncoding.EncodeToString(make([]byte, 24))[:30] + "=="},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := repository.DecodeCursor(tt.input); !errors.Is(err, repository.ErrInvalidCursor) {
				t.Errorf("DecodeCursor(%q) error = %v, want ErrInvalidCursor", tt.input, err)
			}
		})
	}
}
