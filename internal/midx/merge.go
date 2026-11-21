package midx

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"io"
)

// Iter yields records in order. It returns false at the end.
type Iter interface {
	Next() (Record, bool, error)
}

// ReadFunc reads n bytes at off of one layer file.
type ReadFunc func(ctx context.Context, off, n int64) ([]byte, error)

const iterChunk = 4096

type fileIter struct {
	ctx  context.Context
	read ReadFunc
	d    *Dir
	i    int
	base int
	buf  []byte
	hash interface {
		io.Writer
		Sum([]byte) []byte
	}
}

// NewIter reads the records of a layer in order, in chunks, and checks the checksum of the layer at the end.
func NewIter(ctx context.Context, d *Dir, read ReadFunc) Iter {
	return &fileIter{ctx: ctx, read: read, d: d, hash: sha256.New()}
}

func (it *fileIter) Next() (Record, bool, error) {
	if it.i >= it.d.N {
		return Record{}, false, nil
	}
	if it.i-it.base >= len(it.buf)/RecordSize {
		c := min(iterChunk, it.d.N-it.i)
		b, err := it.read(it.ctx, int64(it.i)*RecordSize, int64(c)*RecordSize)
		if err != nil {
			return Record{}, false, err
		}
		if len(b) != c*RecordSize {
			return Record{}, false, ErrCorrupt
		}
		it.base, it.buf = it.i, b
		_, _ = it.hash.Write(b)
	}
	r := getRecord(it.buf[(it.i-it.base)*RecordSize:])
	it.i++
	if it.i == it.d.N {
		_, _ = it.hash.Write(it.d.tail[:len(it.d.tail)-trailer])
		if !bytes.Equal(it.hash.Sum(nil), it.d.tail[len(it.d.tail)-trailer:]) {
			return Record{}, false, ErrCorrupt
		}
	}
	return r, true, nil
}

type source struct {
	it  Iter
	cur Record
	idx int
}

type sources []*source

func (h sources) Len() int { return len(h) }
func (h sources) Less(i, j int) bool {
	if c := h[i].cur.compare(h[j].cur); c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx
}
func (h sources) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sources) Push(x any)   { *h = append(*h, x.(*source)) }
func (h *sources) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// Merge writes one layer over packs from the records of ins, in order. remap turns a record of input in into its record
// in the new layer, and its result must keep each input in order. maxObjects bounds the records of all inputs. It
// returns the number of records that the inputs gave, before it drops repeats, so a caller can check that none was lost.
func Merge(ctx context.Context, ins []Iter, packs []Pack, maxObjects int, remap func(in int, r Record) (Record, error)) (Layer, int, error) {
	b := NewBuilder(packs, maxObjects)
	var h sources
	seen := 0
	pull := func(s *source) (bool, error) {
		r, ok, err := s.it.Next()
		if err != nil || !ok {
			return false, err
		}
		seen++
		if s.cur, err = remap(s.idx, r); err != nil {
			return false, err
		}
		return true, nil
	}
	for i, it := range ins {
		s := &source{it: it, idx: i}
		ok, err := pull(s)
		if err != nil {
			return Layer{}, 0, err
		}
		if ok {
			h = append(h, s)
		}
	}
	heap.Init(&h)
	for n := 0; len(h) > 0; n++ {
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return Layer{}, 0, err
			}
		}
		s := h[0]
		if err := b.Add(s.cur); err != nil {
			return Layer{}, 0, err
		}
		ok, err := pull(s)
		if err != nil {
			return Layer{}, 0, err
		}
		if ok {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	l, err := b.Finish()
	return l, seen, err
}
