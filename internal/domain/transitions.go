package domain

import "slices"

// AllowedFrom returns the statuses from which a notification may move to status to. It is the state
// machine of ADR 0007, keyed by target status:
//
//	from                              to                  actor
//	pending, retry_scheduled          queued              relay
//	queued                            sending             worker
//	sending                           sent                worker
//	sending                           retry_scheduled     worker, reaper
//	sending                           permanently_failed  worker, reaper
//	pending, queued, retry_scheduled  cancelled           API
//
// A notification is created in pending and never returns to it, so AllowedFrom(StatusPending) is
// empty, as it is for an unknown status. sent, permanently_failed and cancelled are terminal.
//
// Every call returns a new slice, so a caller may narrow it to the statuses it expects.
func AllowedFrom(to Status) []Status {
	switch to {
	case StatusQueued:
		return []Status{StatusPending, StatusRetryScheduled}
	case StatusSending:
		return []Status{StatusQueued}
	case StatusSent, StatusRetryScheduled, StatusPermanentlyFailed:
		return []Status{StatusSending}
	case StatusCancelled:
		return []Status{StatusPending, StatusQueued, StatusRetryScheduled}
	default:
		return nil
	}
}

// CanTransition reports whether the state machine allows a notification in status from to move to
// status to.
func CanTransition(from, to Status) bool {
	return slices.Contains(AllowedFrom(to), from)
}
