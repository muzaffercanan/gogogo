// Package tcretry works around a startup race in testcontainers-go that shows up when `go test` runs
// several integration test binaries at once.
package tcretry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Start calls start until it succeeds, fails for a reason other than the reaper race, or has failed three
// times. start must clean up after itself when it fails.
//
// testcontainers-go (v0.45.0) shares one Ryuk reaper container among the test binaries of a `go test`
// run. A binary that finds the reaper another binary has created but not yet started fails at once
// ("wait for reaper …: unexpected container status "created"") instead of waiting for it; the next
// attempt finds it running. Remove this once testcontainers-go waits for the reaper itself.
func Start[T any](ctx context.Context, start func(context.Context) (T, error)) (T, error) {
	return startWith(ctx, start, 3, time.Second)
}

func startWith[T any](ctx context.Context, start func(context.Context) (T, error), attempts int, delay time.Duration) (T, error) {
	for attempt := 1; ; attempt++ {
		v, err := start(ctx)
		if err == nil || attempt == attempts || !isReaperRace(err) {
			return v, err
		}
		fmt.Fprintf(os.Stderr, "tcretry: attempt %d of %d hit the reaper race, retrying: %v\n", attempt, attempts, err)
		select {
		case <-ctx.Done():
			var zero T
			return zero, errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
	}
}

// isReaperRace reports whether err came from waiting for a reaper container that another test binary
// started. testcontainers-go exports no sentinel for it, so the message is matched.
func isReaperRace(err error) bool {
	return strings.Contains(err.Error(), "wait for reaper")
}
