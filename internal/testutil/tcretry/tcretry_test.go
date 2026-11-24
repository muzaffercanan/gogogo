package tcretry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// errRace has the shape of the error testcontainers-go v0.45.0 returns when it loses the reaper race.
var errRace = errors.New(`run postgres: generic container: create container: reaper: from container "1ca40182": ` +
	`wait for reaper 1ca40182: unexpected container status "created"`)

func TestStartWith(t *testing.T) {
	otherErr := errors.New("pull access denied")

	tests := []struct {
		name      string
		errs      []error // returned by successive calls; calls past the end succeed
		wantCalls int
		wantErr   error
	}{
		{name: "succeeds at once", wantCalls: 1},
		{name: "retries the reaper race", errs: []error{errRace, errRace}, wantCalls: 3},
		{name: "gives up after three races", errs: []error{errRace, errRace, errRace}, wantCalls: 3, wantErr: errRace},
		{name: "does not retry other errors", errs: []error{otherErr}, wantCalls: 1, wantErr: otherErr},
		{name: "stops at another error after a race", errs: []error{errRace, otherErr}, wantCalls: 2, wantErr: otherErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			start := func(context.Context) (int, error) {
				calls++
				if calls <= len(tt.errs) {
					return 0, tt.errs[calls-1]
				}
				return 42, nil
			}

			got, err := startWith(t.Context(), start, 3, 0)

			if calls != tt.wantCalls {
				t.Errorf("start called %d times, want %d", calls, tt.wantCalls)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got != 42 {
				t.Errorf("value = %d, want 42", got)
			}
		})
	}
}

func TestStartWithStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	start := func(context.Context) (int, error) {
		calls++
		cancel()
		return 0, errRace
	}

	_, err := startWith(ctx, start, 3, time.Hour)

	if calls != 1 || !errors.Is(err, context.Canceled) || !errors.Is(err, errRace) {
		t.Errorf("calls = %d, error = %v; want 1 call and an error wrapping both the race and context.Canceled", calls, err)
	}
}
