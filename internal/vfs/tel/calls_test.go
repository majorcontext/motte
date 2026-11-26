package tel_test

import (
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/majorcontext/motte/internal/vfs/tel"
)

// TestCallStatsTimesWithTheInjectedClock: a total or maximum that reads the wall clock makes every stats test flaky.
func TestCallStatsTimesWithTheInjectedClock(t *testing.T) {
	t.Parallel()
	clk := quartz.NewMock(t)
	c := tel.NewCallStats(clk)
	for _, d := range []time.Duration{time.Second, 3 * time.Second} {
		end := c.Begin("k")
		clk.Advance(d)
		end()
	}
	got := c.Take()
	if len(got) != 1 || got[0] != (tel.CallWindow{Kind: "k", Count: 2, Total: 4 * time.Second, Max: 3 * time.Second}) {
		t.Errorf("Take = %+v", got)
	}
	if again := c.Take(); len(again) != 0 {
		t.Errorf("second Take = %+v, want none", again)
	}
}
