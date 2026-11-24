package provider

import "time"

// SetClock replaces the clock the client reads Retry-After dates against.
func SetClock(c *Client, now func() time.Time) { c.now = now }
