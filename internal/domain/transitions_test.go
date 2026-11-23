package domain_test

import (
	"slices"
	"testing"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

// edges is the state machine of ADR 0007 written out edge by edge, independently of the implementation.
var edges = []struct{ from, to domain.Status }{
	{domain.StatusPending, domain.StatusQueued},
	{domain.StatusRetryScheduled, domain.StatusQueued},
	{domain.StatusQueued, domain.StatusSending},
	{domain.StatusSending, domain.StatusSent},
	{domain.StatusSending, domain.StatusRetryScheduled},
	{domain.StatusSending, domain.StatusPermanentlyFailed},
	{domain.StatusPending, domain.StatusCancelled},
	{domain.StatusQueued, domain.StatusCancelled},
	{domain.StatusRetryScheduled, domain.StatusCancelled},
}

// statuses lists every status plus one the domain does not know.
var statuses = []domain.Status{
	domain.StatusPending,
	domain.StatusQueued,
	domain.StatusSending,
	domain.StatusSent,
	domain.StatusRetryScheduled,
	domain.StatusPermanentlyFailed,
	domain.StatusCancelled,
	"delivered",
}

func TestCanTransition(t *testing.T) {
	allowed := make(map[[2]domain.Status]bool, len(edges))
	for _, e := range edges {
		allowed[[2]domain.Status{e.from, e.to}] = true
	}

	for _, from := range statuses {
		for _, to := range statuses {
			t.Run(string(from)+" to "+string(to), func(t *testing.T) {
				want := allowed[[2]domain.Status{from, to}]
				if got := domain.CanTransition(from, to); got != want {
					t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
				}
			})
		}
	}
}

func TestAllowedFrom(t *testing.T) {
	for _, to := range statuses {
		t.Run(string(to), func(t *testing.T) {
			var want []domain.Status
			for _, e := range edges {
				if e.to == to {
					want = append(want, e.from)
				}
			}
			slices.Sort(want)

			got := domain.AllowedFrom(to)
			sorted := slices.Sorted(slices.Values(got))
			if !slices.Equal(sorted, want) {
				t.Fatalf("AllowedFrom(%s) = %v, want %v in any order", to, got, want)
			}

			// Callers may narrow the result, so changing it must not change the state machine.
			for i := range got {
				got[i] = domain.StatusSent
			}
			if again := slices.Sorted(slices.Values(domain.AllowedFrom(to))); !slices.Equal(again, want) {
				t.Errorf("AllowedFrom(%s) = %v after a caller changed an earlier result, want %v", to, again, want)
			}
		})
	}
}
