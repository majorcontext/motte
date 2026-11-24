package bucket

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// HedgeConfig tunes Hedge.
type HedgeConfig struct {
	// Ratio is the share of reads that may have a duplicate, from 0 to 1. Zero turns hedging off.
	Ratio float64
	// Min and Max bound the time that a read waits before its duplicate starts. The wait is the 95th percentile of the
	// recent reads of the same size class, and Max until enough reads have finished.
	Min, Max time.Duration
}

// HedgeStats counts the duplicates of a Hedged store.
type HedgeStats struct {
	// Hedges is the duplicates that started, Wins those whose answer came first, and Skipped the reads that waited
	// longer than their delay but had no token or no budget for a duplicate.
	Hedges, Wins, Skipped int64
}

const (
	// hedgeBudget is the bytes that duplicate reads may hold at once, apart from the budgets of the callers. A
	// duplicate of n bytes holds hedgeFactor*n: the buffer, the chunks that a reader keeps while it grows the buffer,
	// and the unused capacity of both.
	hedgeBudget = 16 << 20
	hedgeFactor = 3
	// hedgeBurst is the duplicates that may start together after a quiet time.
	hedgeBurst = 10
	ringSize   = 256
	// warm is the reads of a size class that must finish before its percentile is trusted.
	warm = 64
)

// Hedged reads from a primary store and, when a read is slow, starts one duplicate on another store and returns the
// answer that comes first.
type Hedged struct {
	Store
	alt      Store
	cfg      HedgeConfig
	tokens   atomic.Int64
	budget   *semaphore.Weighted
	classes  [3]quantile
	hedges   atomic.Int64
	wins     atomic.Int64
	skipped  atomic.Int64
	earnEach int64
}

// Hedge returns a store whose range reads have a duplicate on alt, as "The Tail at Scale" describes. A duplicate of a
// read starts when the read outlasts the recent 95th percentile of its size class, within cfg, and only while the
// duplicates stay under cfg.Ratio of the reads (a token bucket) and under their memory budget. The loser is
// cancelled. alt may be the same store as primary, but a store on other connections gives the duplicate a path that a
// stalled connection does not block. The first success wins. A first answer that is an error other than ErrNotFound
// waits for the other read, so one bad connection does not fail the read. Other methods, a read of a whole object and
// a range too large for the duplicate budget use the primary only.
func Hedge(primary, alt Store, cfg HedgeConfig) *Hedged {
	h := &Hedged{Store: primary, alt: alt, cfg: cfg, budget: semaphore.NewWeighted(hedgeBudget), earnEach: int64(cfg.Ratio * 1000)}
	h.tokens.Store(hedgeBurst * 1000)
	return h
}

// HedgeStats returns the counters of the duplicates.
func (h *Hedged) HedgeStats() HedgeStats {
	return HedgeStats{h.hedges.Load(), h.wins.Load(), h.skipped.Load()}
}

type result struct {
	data []byte
	err  error
	dup  bool
}

func (h *Hedged) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if n < 0 || n > hedgeBudget/hedgeFactor || h.earnEach <= 0 {
		return h.Store.GetRange(ctx, key, off, n)
	}
	class := &h.classes[sizeClass(n)]
	h.earn()
	start := time.Now()
	out := make(chan result, 2)
	read := func(ctx context.Context, s Store, dup bool) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			b, err := s.GetRange(ctx, key, off, n)
			out <- result{b, err, dup}
		}()
		return done
	}
	pctx, stopPrimary := context.WithCancel(ctx)
	defer stopPrimary()
	primary := read(pctx, h.Store, false)
	live := []<-chan struct{}{primary}
	stopDup := func() {}
	defer func() { stopDup() }()
	abort := func() (result, error) {
		stopPrimary()
		stopDup()
		for _, done := range live {
			<-done
		}
		return result{}, ctx.Err()
	}
	recv := func() (result, error) {
		select {
		case r := <-out:
			return r, nil
		case <-ctx.Done():
			return abort()
		}
	}
	timer := time.NewTimer(h.delay(class))
	defer timer.Stop()
	var r result
	var err error
	select {
	case r = <-out:
	case <-ctx.Done():
		_, err = abort()
		return nil, err
	case <-timer.C:
		release, ok := h.start(hedgeFactor * n)
		if !ok {
			h.skipped.Add(1)
			if r, err = recv(); err != nil {
				return nil, err
			}
			break
		}
		dctx, stop := context.WithCancel(ctx)
		stopDup = stop
		dup := read(dctx, h.alt, true)
		live = append(live, dup)
		go func() { <-primary; <-dup; release() }()
		if r, err = recv(); err != nil {
			return nil, err
		}
		if r.err != nil && !errors.Is(r.err, ErrNotFound) {
			r2, err := recv()
			if err != nil {
				return nil, err
			}
			if r2.err == nil {
				r = r2
			}
		}
		if r.dup && r.err == nil {
			h.wins.Add(1)
		}
	}
	if r.err == nil {
		class.add(time.Since(start))
	}
	return r.data, r.err
}

func sizeClass(n int64) int {
	switch {
	case n <= 64<<10:
		return 0
	case n <= 1<<20:
		return 1
	}
	return 2
}

func (h *Hedged) delay(q *quantile) time.Duration {
	d := q.p95()
	if d == 0 {
		return h.cfg.Max
	}
	return min(max(d, h.cfg.Min), h.cfg.Max)
}

func (h *Hedged) earn() {
	for {
		t := h.tokens.Load()
		if h.tokens.CompareAndSwap(t, min(t+h.earnEach, hedgeBurst*1000)) {
			return
		}
	}
}

// start takes a token and the budget for a duplicate of cost bytes. The returned function gives the budget back.
func (h *Hedged) start(cost int64) (release func(), ok bool) {
	if !h.budget.TryAcquire(cost) {
		return nil, false
	}
	for {
		t := h.tokens.Load()
		if t < 1000 {
			h.budget.Release(cost)
			return nil, false
		}
		if h.tokens.CompareAndSwap(t, t-1000) {
			h.hedges.Add(1)
			return func() { h.budget.Release(cost) }, true
		}
	}
}

// quantile keeps the times of the last ringSize reads of a size class.
type quantile struct {
	ring [ringSize]atomic.Int64
	n    atomic.Uint64
	est  atomic.Int64
}

func (q *quantile) add(d time.Duration) {
	i := q.n.Add(1)
	q.ring[(i-1)%ringSize].Store(int64(d))
	if i >= warm && i%(warm/2) == 0 {
		s := make([]int64, min(i, ringSize))
		for j := range s {
			s[j] = q.ring[j].Load()
		}
		slices.Sort(s)
		q.est.Store(s[len(s)*95/100])
	}
}

func (q *quantile) p95() time.Duration { return time.Duration(q.est.Load()) }
