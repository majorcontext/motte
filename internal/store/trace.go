package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/majorcontext/motte/internal/store")

// Stats counts what one request read from the store. A handler puts one in the context with WithStats and reads it when
// the request ends. A nil *Stats counts nothing.
type Stats struct {
	Objects, Bytes, Layers, RangeReads, RangeBytes, RangeHits, RangeMisses, MaxDepth, SlotWaitNanos atomic.Int64
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

func (s *Stats) rangeCache(hit bool) {
	switch {
	case s == nil:
	case hit:
		s.RangeHits.Add(1)
	default:
		s.RangeMisses.Add(1)
	}
}

// Attrs returns the counts of the request as span attributes.
func (s *Stats) Attrs() []attribute.KeyValue { return s.snap().attrs() }

type statSnap struct{ objects, bytes, layers, reads, readBytes, hits, misses int64 }

func (s *Stats) snap() statSnap {
	if s == nil {
		return statSnap{}
	}
	return statSnap{s.Objects.Load(), s.Bytes.Load(), s.Layers.Load(), s.RangeReads.Load(), s.RangeBytes.Load(), s.RangeHits.Load(), s.RangeMisses.Load()}
}

func (a statSnap) attrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.Int64("objects", a.objects), attribute.Int64("bytes", a.bytes), attribute.Int64("layers_probed", a.layers),
		attribute.Int64("range_reads", a.reads), attribute.Int64("range_read_bytes", a.readBytes),
		attribute.Int64("range_cache_hits", a.hits), attribute.Int64("range_cache_misses", a.misses)}
}

func (a statSnap) since(b statSnap) statSnap {
	return statSnap{a.objects - b.objects, a.bytes - b.bytes, a.layers - b.layers, a.reads - b.reads, a.readBytes - b.readBytes, a.hits - b.hits, a.misses - b.misses}
}

func noSpan(error, ...attribute.KeyValue) {}

// StartSpan starts a child span of ctx. The function that it returns ends the span with err and attrs, and adds what the
// request of ctx read from the store meanwhile. Under a parent that is not recording, it starts nothing.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(err error, attrs ...attribute.KeyValue)) {
	if parent := trace.SpanFromContext(ctx); parent.SpanContext().IsValid() && !parent.IsRecording() {
		return ctx, noSpan
	}
	return startSpan(ctx, name, trace.WithAttributes(attrs...))
}

// StartRootSpan is StartSpan for work that has no request, such as maintenance: the span is a root, so the sampling of
// a request that happens to run it does not drop it.
func StartRootSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(err error, attrs ...attribute.KeyValue)) {
	return startSpan(ctx, name, trace.WithNewRoot(), trace.WithAttributes(attrs...))
}

func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, func(err error, attrs ...attribute.KeyValue)) {
	ctx, sp := tracer.Start(ctx, name, opts...)
	if !sp.IsRecording() {
		return ctx, noSpan
	}
	st := statsFrom(ctx)
	before := st.snap()
	return ctx, func(err error, attrs ...attribute.KeyValue) {
		if st != nil {
			sp.SetAttributes(st.snap().since(before).attrs()...)
		}
		sp.SetAttributes(attrs...)
		finishSpan(sp, err)
	}
}

// Phases returns a function that ends a phase of the request of ctx: it records a child span named prefix and name
// that runs from the end of the last phase, with what the request read from the store meanwhile. It returns nil when
// the request is not traced.
func Phases(ctx context.Context, prefix string) func(name string) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return nil
	}
	st := statsFrom(ctx)
	last, before := time.Now(), st.snap()
	return func(name string) {
		now, cur := time.Now(), st.snap()
		_, sp := tracer.Start(ctx, prefix+name, trace.WithTimestamp(last))
		if st != nil {
			sp.SetAttributes(cur.since(before).attrs()...)
		}
		sp.End(trace.WithTimestamp(now))
		last, before = now, cur
	}
}

func finishSpan(sp trace.Span, err error) {
	if err != nil {
		sp.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", fmt.Sprintf("%T", err))))
		sp.SetStatus(codes.Error, "error")
	}
	sp.End()
}
