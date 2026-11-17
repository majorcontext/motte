// Package clock aliases the quartz Clock and adds Real, Or and Wait. Code that takes a Clock waits on it,
// and a test passes a quartz Mock to make that wait take no wall time.
package clock

import (
	"context"
	"time"

	"github.com/coder/quartz"
)

// Clock reads time and starts timers and tickers.
type Clock = quartz.Clock

// Real returns the clock that follows wall time.
func Real() Clock { return quartz.NewReal() }

// Or returns c, or the real clock when c is nil.
func Or(c Clock) Clock {
	if c == nil {
		return Real()
	}
	return c
}

// Wait blocks for d on c. It returns the error of ctx when ctx ends first.
func Wait(ctx context.Context, c Clock, d time.Duration) error {
	t := c.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
