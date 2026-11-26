package tel

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/majorcontext/motte/internal/clock"
)

// CallStats counts calls by kind between two reads. Each call is counted, never sampled.
type CallStats struct {
	clk clock.Clock
	mu  sync.Mutex
	by  map[string]*CallWindow
}

// CallWindow is what the calls of one kind did since the last Take.
type CallWindow struct {
	Kind       string
	Count      uint64
	Total, Max time.Duration
}

// NewCallStats returns a recorder that reads time from clk; nil is the real clock.
func NewCallStats(clk clock.Clock) *CallStats {
	return &CallStats{clk: clock.Or(clk), by: map[string]*CallWindow{}}
}

// Begin starts timing a call of kind. The returned function ends it. A nil receiver records nothing.
func (c *CallStats) Begin(kind string) (end func()) {
	if c == nil {
		return func() {}
	}
	start := c.clk.Now()
	return func() { c.record(kind, c.clk.Since(start)) }
}

func (c *CallStats) record(kind string, d time.Duration) {
	d = max(d, 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.by[kind]
	if w == nil {
		w = &CallWindow{Kind: kind}
		c.by[kind] = w
	}
	w.Count++
	w.Total += d
	w.Max = max(w.Max, d)
}

// Take returns the windows of the kinds that had calls, sorted by kind, and resets them.
func (c *CallStats) Take() []CallWindow {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]CallWindow, 0, len(c.by))
	for _, w := range c.by {
		out = append(out, *w)
	}
	clear(c.by)
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

type callsKey struct{}

// WithCalls returns ctx so that the store calls made under it count in c.
func WithCalls(ctx context.Context, c *CallStats) context.Context {
	return context.WithValue(ctx, callsKey{}, c)
}

// CallsFrom returns the recorder that WithCalls set, or nil.
func CallsFrom(ctx context.Context) *CallStats {
	c, _ := ctx.Value(callsKey{}).(*CallStats)
	return c
}
