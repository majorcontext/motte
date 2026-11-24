package bucket_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/majorcontext/motte/bucket"
)

type slow struct {
	bucket.Store
	name  string
	delay time.Duration
	err   error
}

func (s slow) GetRange(ctx context.Context, _ string, _, _ int64) ([]byte, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		if s.err != nil {
			return nil, s.err
		}
		return []byte(s.name), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestHedgeAnswersFromTheDuplicateOfAStalledRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := bucket.Hedge(slow{name: "primary", delay: time.Hour}, slow{name: "dup", delay: time.Millisecond}, bucket.HedgeConfig{Ratio: 0.05, Min: time.Millisecond, Max: 100 * time.Millisecond})
		start := time.Now()
		got, err := h.GetRange(context.Background(), "k", 0, 4096)
		if string(got) != "dup" || err != nil || time.Since(start) != 101*time.Millisecond {
			t.Fatalf("read = %q, %v after %v; want dup after 101ms", got, err, time.Since(start))
		}
		if s := h.HedgeStats(); s.Hedges != 1 || s.Wins != 1 {
			t.Errorf("stats = %+v, want one hedge and one win", s)
		}
	})
}

func TestHedgeKeepsToItsShareOfTheReads(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := bucket.Hedge(slow{name: "primary", delay: time.Second}, slow{name: "dup", delay: time.Millisecond}, bucket.HedgeConfig{Ratio: 0.05, Min: time.Millisecond, Max: 10 * time.Millisecond})
		for range 100 {
			if _, err := h.GetRange(context.Background(), "k", 0, 4096); err != nil {
				t.Fatal(err)
			}
		}
		if s := h.HedgeStats(); s.Hedges < 14 || s.Hedges > 16 || s.Skipped < 80 {
			t.Errorf("stats = %+v over 100 stalled reads, want 10 burst plus 5 earned hedges and the rest skipped", s)
		}
		fast := bucket.Hedge(slow{name: "primary", delay: time.Millisecond}, slow{name: "dup", delay: time.Millisecond}, bucket.HedgeConfig{Ratio: 0.05, Max: time.Second})
		if _, err := fast.GetRange(context.Background(), "k", 0, 4096); err != nil || fast.HedgeStats() != (bucket.HedgeStats{}) {
			t.Errorf("a fast read hedged: %+v, %v", fast.HedgeStats(), err)
		}
	})
}

func TestHedgeTakesTheSuccessOverAnEarlierError(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cfg := bucket.HedgeConfig{Ratio: 0.05, Min: time.Millisecond, Max: 10 * time.Millisecond}
		h := bucket.Hedge(slow{name: "primary", delay: time.Second}, slow{delay: time.Millisecond, err: context.DeadlineExceeded}, cfg)
		if got, err := h.GetRange(context.Background(), "k", 0, 4096); string(got) != "primary" || err != nil {
			t.Errorf("read = %q, %v; want the primary's success", got, err)
		}
		if got, err := h.GetRange(context.Background(), "k", 0, 1<<62); string(got) != "primary" || err != nil || h.HedgeStats().Hedges != 1 {
			t.Errorf("a range too large to hedge = %q, %v, %+v; want one read and no new hedge", got, err, h.HedgeStats())
		}
	})
}

type watched struct {
	slow
	live *atomic.Int32
}

func (w watched) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	w.live.Add(1)
	defer w.live.Add(-1)
	return w.slow.GetRange(ctx, k, off, n)
}

func TestHedgeReturnsACancelledReadOnlyAfterItsReadsEnded(t *testing.T) {
	t.Parallel()
	for name, after := range map[string]time.Duration{"before the duplicate": 5 * time.Millisecond, "after the duplicate": 20 * time.Millisecond} {
		synctest.Test(t, func(t *testing.T) {
			var live atomic.Int32
			cfg := bucket.HedgeConfig{Ratio: 0.05, Min: time.Millisecond, Max: 10 * time.Millisecond}
			h := bucket.Hedge(watched{slow{delay: time.Hour}, &live}, watched{slow{delay: time.Hour}, &live}, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			go func() { time.Sleep(after); cancel() }()
			if _, err := h.GetRange(ctx, "k", 0, 4096); err != context.Canceled || live.Load() != 0 {
				t.Errorf("%s: err %v with %d reads still running, want Canceled and none", name, err, live.Load())
			}
		})
	}
}
