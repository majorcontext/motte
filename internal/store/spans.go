package store

import (
	"bytes"
	"context"
	"io"
	"slices"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/majorcontext/motte/bucket"
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

// reqReader reads through a packReader under the context of one request.
type reqReader struct {
	b   *packReader
	ctx context.Context
}

func (r reqReader) Size() int64 { return r.b.size }

func (r reqReader) ReadAt(p []byte, off int64) (int, error) { return r.b.readAt(r.ctx, p, off) }

func (b *packReader) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < b.size {
		d := b.cover(off, off+1)
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

// read reads [off, end) of the pack from the object store.
func (b *packReader) read(ctx context.Context, off, end int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, blockTimeout)
	defer cancel()
	ctx, sp := tracer.Start(ctx, "vcs.pack.read", trace.WithAttributes(attribute.Int64("offset", off), attribute.Int64("range_bytes", end-off)))
	d, err := b.st.GetRange(ctx, b.key, off, end-off)
	statsFrom(ctx).rangeRead(len(d))
	b.svc.rangeReads.Add(1)
	b.svc.rangeBytes.Add(int64(len(d)))
	sp.SetAttributes(attribute.Int("bytes", len(d)))
	finishSpan(sp, err)
	return d, err
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

// fetchRun reads the run and keeps each object whole, from its header to the bound that the header gives, and not the
// gaps that the read closed. release frees the bytes that the run holds in the read-ahead once its read ends, and the
// tail of an object that runs past the end of the run is read after that, from ahead.
func (b *packReader) fetchRun(ctx context.Context, r prefetchRun, release func(), ahead *semaphore.Weighted) error {
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
		if bound, ok := gitpack.EntryBound(d[o-r.start:]); ok {
			need = max(need, min(bound, maxEntry))
		}
		ends[i] = min(o+need, b.size)
	}
	type tail struct {
		from, to int64
		head     []byte
	}
	var tails []tail
	for i := 0; i < len(r.offs); {
		from, to := r.offs[i], ends[i]
		for i++; i < len(r.offs) && r.offs[i] <= to; i++ {
			to = max(to, ends[i])
		}
		switch {
		case to > r.end:
			tails = append(tails, tail{from, to, bytes.Clone(d[from-r.start:])})
		case from == r.start && to == r.end:
			b.keep(from, d)
		default:
			b.keep(from, bytes.Clone(d[from-r.start:to-r.start]))
		}
	}
	release()
	for _, t := range tails {
		n := t.to - r.end
		if err := ahead.Acquire(ctx, n); err != nil {
			return err
		}
		x, err := b.read(ctx, r.end, t.to)
		ahead.Release(n)
		if err != nil {
			return err
		}
		b.keep(t.from, append(t.head, x...))
	}
	return nil
}

// plan joins the objects at offs into the ranges that Prefetch reads: it joins objects that lie within spanGap of each
// other, skips what the reader keeps, and stops after half of what the reader keeps.
func (b *packReader) plan(offs []int64) []prefetchRun {
	slices.Sort(offs)
	offs = slices.DeleteFunc(offs, func(o int64) bool { return b.cover(o, min(o+minRead, b.size)) != nil })
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
