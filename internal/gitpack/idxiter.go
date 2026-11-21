package gitpack

import (
	"context"
	"encoding/binary"
)

// IdxCount returns the number of objects of a v2 index from its first IdxHeadBytes bytes.
func IdxCount(head []byte) (int, error) {
	if len(head) < IdxHeadBytes || head[0] != 0xff || head[1] != 't' || head[2] != 'O' || head[3] != 'c' || binary.BigEndian.Uint32(head[4:8]) != 2 {
		return 0, ErrCorrupt
	}
	return int(binary.BigEndian.Uint32(head[8+4*255:])), nil
}

// IdxHeadBytes is the size of the header and the fanout table of a v2 index.
const IdxHeadBytes = idxHead

// IdxIter reads the names and offsets of a v2 index in name order with ranged reads, so a caller can merge indexes of
// any size in bounded memory.
type IdxIter struct {
	ctx   context.Context
	get   func(ctx context.Context, off, n int64) ([]byte, error)
	n     int
	i     int
	names []byte
	offs  []byte
	base  int
	big   []byte
}

const idxChunk = 8192

// NewIdxIter returns an iterator over the n entries of an index. get reads a range of the index, and the rest of it
// when n is negative.
func NewIdxIter(ctx context.Context, n int, get func(ctx context.Context, off, n int64) ([]byte, error)) *IdxIter {
	return &IdxIter{ctx: ctx, get: get, n: n}
}

// Next returns the next name and its offset in the pack, and false at the end.
func (it *IdxIter) Next() (SHA, int64, bool, error) {
	var s SHA
	if it.i >= it.n {
		return s, 0, false, nil
	}
	if it.i-it.base >= len(it.offs)/4 {
		it.base = it.i
		c := min(idxChunk, it.n-it.i)
		var err error
		if it.names, err = it.read(int64(idxHead+20*it.i), int64(20*c)); err != nil {
			return s, 0, false, err
		}
		if it.offs, err = it.read(int64(idxHead+24*it.n+4*it.i), int64(4*c)); err != nil {
			return s, 0, false, err
		}
	}
	j := it.i - it.base
	copy(s[:], it.names[20*j:])
	off := int64(binary.BigEndian.Uint32(it.offs[4*j:]))
	if off&0x80000000 != 0 {
		if it.big == nil {
			start := int64(idxHead + 28*it.n)
			b, err := it.get(it.ctx, start, -1)
			if err != nil || len(b) < 40 {
				return s, 0, false, ErrCorrupt
			}
			it.big = b[:len(b)-40]
		}
		k := int(off & 0x7fffffff)
		if (k+1)*8 > len(it.big) {
			return s, 0, false, ErrCorrupt
		}
		off = int64(binary.BigEndian.Uint64(it.big[8*k:]))
	}
	it.i++
	return s, off, true, nil
}

func (it *IdxIter) read(off, n int64) ([]byte, error) {
	b, err := it.get(it.ctx, off, n)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != n {
		return nil, ErrCorrupt
	}
	return b, nil
}
