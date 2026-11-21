package store

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/majorcontext/motte/internal/store")

// Stats counts what one request read from the store. A handler puts one in the context with WithStats and reads it when
// the request ends. A nil *Stats counts nothing.
type Stats struct {
	Objects, Bytes, Layers, RangeReads, RangeBytes, MaxDepth, SlotWaitNanos atomic.Int64
}

type statsKey struct{}

// WithStats returns ctx with a Stats that the reads of the request fill.
func WithStats(ctx context.Context) (context.Context, *Stats) {
	st := &Stats{}
	return context.WithValue(ctx, statsKey{}, st), st
}

// StatsFrom returns the Stats of a request, or nil.
func StatsFrom(ctx context.Context) *Stats { return statsFrom(ctx) }

// AddSlotWait adds the time a request waited for a weighted slot.
func (s *Stats) AddSlotWait(d time.Duration) {
	if s != nil {
		s.SlotWaitNanos.Add(int64(d))
	}
}

func statsFrom(ctx context.Context) *Stats {
	st, _ := ctx.Value(statsKey{}).(*Stats)
	return st
}

func (s *Stats) layer() {
	if s != nil {
		s.Layers.Add(1)
	}
}

func (s *Stats) object(n, depth int) {
	if s == nil {
		return
	}
	s.Objects.Add(1)
	s.Bytes.Add(int64(n))
	for {
		cur := s.MaxDepth.Load()
		if int64(depth) <= cur || s.MaxDepth.CompareAndSwap(cur, int64(depth)) {
			return
		}
	}
}

func (s *Stats) rangeRead(n int) {
	if s != nil {
		s.RangeReads.Add(1)
		s.RangeBytes.Add(int64(n))
	}
}

func finishSpan(sp trace.Span, err error) {
	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, "error")
	}
	sp.End()
}
