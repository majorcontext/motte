package store

import (
	"context"
	"io"
	"slices"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/majorcontext/motte/bucket"
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

// fetch reads [off, end) of the pack, keeps it and returns it.
func (b *packReader) fetch(ctx context.Context, off, end int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, blockTimeout)
	defer cancel()
	ctx, sp := tracer.Start(ctx, "vcs.pack.read", trace.WithAttributes(attribute.Int64("offset", off), attribute.Int64("range_bytes", end-off)))
	d, err := b.st.GetRange(ctx, b.key, off, end-off)
	statsFrom(ctx).rangeRead(len(d))
	b.svc.rangeReads.Add(1)
	b.svc.rangeBytes.Add(int64(len(d)))
	sp.SetAttributes(attribute.Int("bytes", len(d)))
	finishSpan(sp, err)
	if err != nil {
		return nil, err
	}
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
	return d, nil
}

// prefetch reads the bytes of the objects at offs in as few ranges as it can: it joins objects that lie within
// spanGap of each other, and it stops after it has read half of what the reader keeps.
func (b *packReader) prefetch(ctx context.Context, offs []int64) {
	slices.Sort(offs)
	budget := int64(maxSpanBytes / 2)
	for i := 0; i < len(offs) && budget > 0 && ctx.Err() == nil; {
		start := offs[i]
		end := min(start+minRead, b.size)
		j := i + 1
		for ; j < len(offs) && offs[j] <= end+spanGap && min(offs[j]+minRead, b.size)-start <= maxRun; j++ {
			end = max(end, min(offs[j]+minRead, b.size))
		}
		i = j
		if start >= end || b.cover(start, end) != nil {
			continue
		}
		if _, err := b.fetch(ctx, start, end); err != nil {
			return
		}
		budget -= end - start
	}
}
