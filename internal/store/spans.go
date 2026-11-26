package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
)

const (
	// minRead is the least that one pack read asks for: the header of an object and, for a small object, all of it.
	minRead = 4 << 10
	// spanGap is the gap between two reads that Prefetch closes by reading it, so near objects cost one request.
	spanGap = 128 << 10
	// maxRun bounds one read of Prefetch.
	maxRun = 1 << 20
	// maxSpanBytes bounds the bytes that one pack keeps.
	maxSpanBytes = 4 << 20
	// aheadKeep bounds the bytes that the prefetches of one request keep.
	aheadKeep = 4 << 20
)

type span struct {
	off  int64
	data []byte
}

// packReader reads a pack from the object store in ranges of the size that the reader needs, and keeps the newest
// ranges up to maxSpanBytes.
type packReader struct {
	svc  *Service
	st   bucket.Store
	key  string
	size int64

	mu    sync.Mutex
	spans []span
	held  int
	// changed runs, with no lock held, after the kept ranges change.
	changed func()
}

func (b *packReader) cachedBytes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.held
}

func (b *packReader) Size() int64 { return b.size }

// reqReader reads through a packReader under the context of one request, and first from what the prefetches of the
// request kept in ahead, when it has one.
type reqReader struct {
	b     *packReader
	ctx   context.Context
	ahead *aheadBuf
}

func (r reqReader) Size() int64 { return r.b.size }

func (r reqReader) ReadAt(p []byte, off int64) (int, error) {
	return r.b.readAt(r.ctx, r.ahead, p, off)
}

// aheadBuf holds the ranges that the prefetches of one request read, at most aheadKeep bytes, oldest out first. A
// request keeps its own, so the prefetches of other requests cannot push out what it has read and not yet used.
type aheadBuf struct {
	mu    sync.Mutex
	spans []aheadSpan
	held  int
}

type aheadSpan struct {
	b *packReader
	span
}

func (a *aheadBuf) cover(b *packReader, start, end int64) []byte {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := len(a.spans) - 1; i >= 0; i-- {
		s := a.spans[i]
		if s.b == b && s.off <= start && end <= s.off+int64(len(s.data)) {
			return s.data[start-s.off:]
		}
	}
	return nil
}

// reset drops what the buffer holds.
func (a *aheadBuf) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	clear(a.spans)
	a.spans, a.held = nil, 0
}

func (a *aheadBuf) keep(b *packReader, off int64, d []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.held+len(d) > aheadKeep && len(a.spans) > 0 {
		a.held -= len(a.spans[0].data)
		a.spans[0] = aheadSpan{}
		a.spans = a.spans[1:]
	}
	a.spans = append(a.spans, aheadSpan{b, span{off, d}})
	a.held += len(d)
}

func (b *packReader) readAt(ctx context.Context, a *aheadBuf, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < b.size {
		d := a.cover(b, off, off+1)
		if d == nil {
			d = b.cover(off, off+1)
		}
		if d == nil {
			end := min(max(off+int64(len(p)-n), off+minRead), b.size)
			var err error
			if d, err = b.fetch(ctx, off, end); err != nil {
				return n, err
			}
			if len(d) == 0 {
				return n, io.ErrUnexpectedEOF
			}
		}
		c := copy(p[n:], d)
		n += c
		off += int64(c)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// cover returns the bytes of a kept range from start on, when one range holds [start, end).
func (b *packReader) cover(start, end int64) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := len(b.spans) - 1; i >= 0; i-- {
		s := b.spans[i]
		if s.off <= start && end <= s.off+int64(len(s.data)) {
			return s.data[start-s.off:]
		}
	}
	return nil
}

// read returns [off, end) of the pack, from the range cache when an earlier request read it. Requests that ask for one
// range together make one bucket read, which no single request cancels. That read outlives the request that began it,
// so it holds bytes of the service-owned read budget, not of a request, until it ends.
func (b *packReader) read(ctx context.Context, off, end int64) ([]byte, error) {
	k := rangeKey{b.key, off, end}
	d, hit := b.svc.rcache.get(k)
	statsFrom(ctx).rangeCache(hit)
	if hit {
		return d, nil
	}
	svc, st := b.svc, b.st
	ch := svc.loads.DoChan(fmt.Sprintf("range|%s|%d|%d", b.key, off, end), func() (any, error) { return svc.loadRange(ctx, st, k) })
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]byte), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// readBudgetFactor is how many times its length a read holds of the read budget: the final buffer, the chunks that the
// readers of the buckets keep while they grow it, and the unused capacity of both.
const readBudgetFactor = 3

// loadRange reads the range k of a pack from the bucket st, unless a flight that ended since the caller missed the
// cache has stored it. It holds bytes of the read budget while it reads. It runs after the request that began it may
// have gone, so it keeps only the service, the bucket and the key, not the reader of the pack.
func (s *Service) loadRange(ctx context.Context, st bucket.Store, k rangeKey) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blockTimeout)
	defer cancel()
	if d, ok := s.rcache.peek(k); ok {
		return d, nil
	}
	n := min(readBudgetFactor*(k.end-k.off), s.readBudget)
	if err := s.rflight.Acquire(ctx, n); err != nil {
		return nil, err
	}
	defer s.rflight.Release(n)
	v := s.rcache.version()
	d, err := s.readRange(ctx, st, k)
	if err == nil {
		s.rcache.put(k, d, v)
	}
	return d, err
}

// readRange reads the range k of a pack from the bucket st.
func (s *Service) readRange(ctx context.Context, st bucket.Store, k rangeKey) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, blockTimeout)
	defer cancel()
	start := s.clk.Now()
	d, err := st.GetRange(ctx, k.pack, k.off, k.end-k.off)
	statsFrom(ctx).rangeRead(len(d))
	s.rangeReads.Add(1)
	s.rangeBytes.Add(int64(len(d)))
	s.slowRead(ctx, k, start, len(d), err)
	return d, err
}

// slowReadAfter is the time that a bucket read takes to count as slow, and slowReadEvery is the least time between two
// spans of slow reads.
const (
	slowReadAfter = 500 * time.Millisecond
	slowReadEvery = time.Second
)

// slowRead records a span for a read that took slowReadAfter or longer, and at most one in each slowReadEvery. Other
// reads have no span of their own: their counts are on the span of the request.
func (s *Service) slowRead(ctx context.Context, k rangeKey, start time.Time, n int, err error) {
	end := s.clk.Now()
	if end.Sub(start) < s.slowAfter {
		return
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && !sc.IsSampled() {
		return
	}
	last := s.slowAt.Load()
	if end.UnixNano()-last < int64(slowReadEvery) || !s.slowAt.CompareAndSwap(last, end.UnixNano()) {
		return
	}
	_, sp := tracer.Start(ctx, "vcs.pack.read.slow", trace.WithTimestamp(start), trace.WithAttributes(attribute.Int64("offset", k.off), attribute.Int64("range_bytes", k.end-k.off), attribute.Int("bytes", n)))
	if err != nil {
		sp.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", fmt.Sprintf("%T", err))))
		sp.SetStatus(codes.Error, "error")
	}
	sp.End(trace.WithTimestamp(end))
}

// keep adds the range that starts at off, and drops the oldest ranges while the reader holds too much.
func (b *packReader) keep(off int64, d []byte) {
	b.mu.Lock()
	for b.held+len(d) > maxSpanBytes && len(b.spans) > 0 {
		b.held -= len(b.spans[0].data)
		b.spans[0] = span{}
		b.spans = b.spans[1:]
	}
	b.spans = append(b.spans, span{off, d})
	b.held += len(d)
	b.mu.Unlock()
	if b.changed != nil {
		b.changed()
	}
}

// fetch reads [off, end) of the pack, keeps it and returns it.
func (b *packReader) fetch(ctx context.Context, off, end int64) ([]byte, error) {
	d, err := b.read(ctx, off, end)
	if err != nil {
		return nil, err
	}
	b.keep(off, d)
	return d, nil
}

// prefetchRun is one read of Prefetch and the objects that it serves.
type prefetchRun struct {
	start, end int64
	offs       []int64
}

// maxEntry bounds the bytes that the read-ahead keeps for one object.
const maxEntry = 256 << 10

// fetchRun reads the run and keeps each object whole in a, or only its first bytes when whole is false, from its header to the bound that the header gives, and not
// the gaps that the read closed. release frees the bytes that the run holds in flight once its read ends, and the tail
// of an object that runs past the end of the run is read after that, from flight, and kept as a range of its own that
// follows the head.
func (b *packReader) fetchRun(ctx context.Context, a *aheadBuf, r prefetchRun, whole bool, release func(), flight *semaphore.Weighted) error {
	d, err := b.read(ctx, r.start, r.end)
	if err == nil && int64(len(d)) != r.end-r.start {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		release()
		return err
	}
	ends := make([]int64, len(r.offs))
	for i, o := range r.offs {
		need := int64(minRead)
		if bound, ok := gitpack.EntryBound(d[o-r.start:]); ok && whole {
			need = max(need, min(bound, maxEntry))
		}
		ends[i] = min(o+need, b.size)
	}
	var tailTo int64
	for i := 0; i < len(r.offs); {
		from, to := r.offs[i], ends[i]
		for i++; i < len(r.offs) && r.offs[i] <= to; i++ {
			to = max(to, ends[i])
		}
		switch {
		case to > r.end:
			tailTo = to
			a.keep(b, from, bytes.Clone(d[from-r.start:]))
		case from == r.start && to == r.end:
			a.keep(b, from, d)
		default:
			a.keep(b, from, bytes.Clone(d[from-r.start:to-r.start]))
		}
	}
	release()
	if tailTo == 0 {
		return nil
	}
	n := tailTo - r.end
	if err := flight.Acquire(ctx, n); err != nil {
		return err
	}
	defer flight.Release(n)
	x, err := b.read(ctx, r.end, tailTo)
	if err != nil {
		return err
	}
	a.keep(b, r.end, x)
	return nil
}

// plan joins the objects at offs into the ranges that Prefetch reads: it joins objects that lie within spanGap of each
// other, skips what the reader keeps, and stops after half of what the reader keeps.
func (b *packReader) plan(offs []int64, a *aheadBuf) []prefetchRun {
	slices.Sort(offs)
	offs = slices.DeleteFunc(offs, func(o int64) bool {
		end := min(o+minRead, b.size)
		return a.cover(b, o, end) != nil || b.cover(o, end) != nil
	})
	var runs []prefetchRun
	budget := int64(maxSpanBytes / 2)
	for i := 0; i < len(offs) && budget > 0; {
		i0, start := i, offs[i]
		end := min(start+minRead, b.size)
		j := i + 1
		for ; j < len(offs) && offs[j] <= end+spanGap && min(offs[j]+minRead, b.size)-start <= maxRun; j++ {
			end = max(end, min(offs[j]+minRead, b.size))
		}
		i = j
		if start >= end {
			continue
		}
		runs = append(runs, prefetchRun{start, end, offs[i0:i]})
		budget -= int64(i-i0) * minRead
	}
	return runs
}
