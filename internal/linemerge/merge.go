// Package linemerge merges two edits of one text file, made from a common base, by lines. The result depends only on
// the three inputs.
package linemerge

import (
	"bytes"
	"errors"
	"slices"
)

var (
	// ErrBinary is an input that holds a NUL byte.
	ErrBinary = errors.New("linemerge: an input is binary")
	// ErrTooLarge is an input that passes MaxBytes or MaxLines.
	ErrTooLarge = errors.New("linemerge: an input is too large")
	// ErrOverlap is two sides that change the same lines or lines that touch.
	ErrOverlap = errors.New("linemerge: both sides change the same lines")
	// ErrTooManyEdits is a side that differs from the base by more than the bound of the diff.
	ErrTooManyEdits = errors.New("linemerge: a side differs from the base by too many lines")
)

const (
	// MaxBytes is the most bytes that Merge takes from each input.
	MaxBytes = 1 << 20
	// MaxLines is the most lines that Merge takes from each input.
	MaxLines = 20000
	// maxEdits is the most inserted and removed lines between the base and one side before Merge gives up.
	maxEdits = 1000
)

// Merge returns the base with the edits of both sides. It returns ErrTooLarge, ErrBinary, ErrOverlap or
// ErrTooManyEdits when it cannot merge them. Two sides that make the same change merge.
func Merge(base, ours, theirs []byte) ([]byte, error) {
	if lineCount(base) > MaxLines || lineCount(ours) > MaxLines || lineCount(theirs) > MaxLines {
		return nil, ErrTooLarge
	}
	switch {
	case len(base) > MaxBytes || len(ours) > MaxBytes || len(theirs) > MaxBytes:
		return nil, ErrTooLarge
	case bytes.IndexByte(base, 0) >= 0 || bytes.IndexByte(ours, 0) >= 0 || bytes.IndexByte(theirs, 0) >= 0:
		return nil, ErrBinary
	case bytes.Equal(ours, theirs) || bytes.Equal(base, theirs):
		return ours, nil
	case bytes.Equal(base, ours):
		return theirs, nil
	}
	in := interner{ids: map[string]int32{}}
	bl, bi := in.lines(base)
	ol, oi := in.lines(ours)
	tl, ti := in.lines(theirs)
	if len(bi) > MaxLines || len(oi) > MaxLines || len(ti) > MaxLines {
		return nil, ErrTooLarge
	}
	ha, ok := hunks(bi, oi)
	if !ok {
		return nil, ErrTooManyEdits
	}
	hb, ok := hunks(bi, ti)
	if !ok {
		return nil, ErrTooManyEdits
	}
	sides := [2]side{{hunks: ha, lines: ol, ids: oi}, {hunks: hb, lines: tl, ids: ti}}
	var out bytes.Buffer
	pos := 0
	for {
		first, second := &sides[0], &sides[1]
		if !first.more() || second.more() && second.next().bs < first.next().bs {
			first, second = second, first
		}
		if !first.more() {
			break
		}
		h := first.next()
		if second.more() && second.next().bs <= h.be {
			o := second.next()
			if o.bs != h.bs || o.be != h.be || !slices.Equal(second.ids[o.ns:o.ne], first.ids[h.ns:h.ne]) {
				return nil, ErrOverlap
			}
			second.i++
		}
		for _, l := range bl[pos:h.bs] {
			out.Write(l)
		}
		for _, l := range first.lines[h.ns:h.ne] {
			out.Write(l)
		}
		pos = h.be
		first.i++
	}
	for _, l := range bl[pos:] {
		out.Write(l)
	}
	return out.Bytes(), nil
}

type side struct {
	hunks []hunk
	i     int
	lines [][]byte
	ids   []int32
}

func (s *side) more() bool { return s.i < len(s.hunks) }
func (s *side) next() hunk { return s.hunks[s.i] }

type interner struct{ ids map[string]int32 }

func (in *interner) lines(data []byte) ([][]byte, []int32) {
	ls := bytes.SplitAfter(data, []byte("\n"))
	if len(ls) > 0 && len(ls[len(ls)-1]) == 0 {
		ls = ls[:len(ls)-1]
	}
	ids := make([]int32, len(ls))
	for i, l := range ls {
		id, ok := in.ids[string(l)]
		if !ok {
			id = int32(len(in.ids))
			in.ids[string(l)] = id
		}
		ids[i] = id
	}
	return ls, ids
}

// hunk replaces the base lines bs..be with the lines ns..ne of a side.
type hunk struct{ bs, be, ns, ne int }

func hunks(a, b []int32) ([]hunk, bool) {
	pairs, ok := matches(a, b)
	if !ok {
		return nil, false
	}
	var out []hunk
	i, j := 0, 0
	for _, p := range pairs {
		if p[0] > i || p[1] > j {
			out = append(out, hunk{i, p[0], j, p[1]})
		}
		i, j = p[0]+1, p[1]+1
	}
	if i < len(a) || j < len(b) {
		out = append(out, hunk{i, len(a), j, len(b)})
	}
	return out, true
}

// matches returns the pairs of equal lines of a shortest edit script of a into b, in order, and false when the script
// has more than maxEdits edits.
func matches(a, b []int32) ([][2]int, bool) {
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a)-pre && suf < len(b)-pre && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	out := make([][2]int, 0, pre+suf)
	for i := range pre {
		out = append(out, [2]int{i, i})
	}
	mid, ok := myers(a[pre:len(a)-suf], b[pre:len(b)-suf])
	if !ok {
		return nil, false
	}
	for _, p := range mid {
		out = append(out, [2]int{p[0] + pre, p[1] + pre})
	}
	for i := suf; i > 0; i-- {
		out = append(out, [2]int{len(a) - i, len(b) - i})
	}
	return out, true
}

func myers(a, b []int32) ([][2]int, bool) {
	n, m := len(a), len(b)
	if n == 0 || m == 0 {
		return nil, n+m <= maxEdits
	}
	off := maxEdits + 1
	v := make([]int32, 2*maxEdits+3)
	trace := make([][]int32, 0, 16)
	found := -1
	for d := 0; d <= maxEdits && found < 0; d++ {
		trace = append(trace, slices.Clone(v[off-d-1:off+d+2]))
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || k != d && v[off+k-1] < v[off+k+1] {
				x = int(v[off+k+1])
			} else {
				x = int(v[off+k-1]) + 1
			}
			y := x - k
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			v[off+k] = int32(x)
			if x >= n && y >= m {
				found = d
				break
			}
		}
	}
	if found < 0 {
		return nil, false
	}
	var rev [][2]int
	x, y := n, m
	for d := found; d > 0; d-- {
		t := trace[d]
		at := func(k int) int { return int(t[k+d+1]) }
		k := x - y
		pk := k - 1
		if k == -d || k != d && at(k-1) < at(k+1) {
			pk = k + 1
		}
		px := at(pk)
		py := px - pk
		mx, my := px+1, py
		if pk == k+1 {
			mx, my = px, py+1
		}
		for x > mx && y > my {
			x, y = x-1, y-1
			rev = append(rev, [2]int{x, y})
		}
		x, y = px, py
	}
	for x > 0 && y > 0 {
		x, y = x-1, y-1
		rev = append(rev, [2]int{x, y})
	}
	slices.Reverse(rev)
	return rev, true
}

func lineCount(data []byte) int {
	n := bytes.Count(data, []byte("\n"))
	if len(data) > 0 && data[len(data)-1] != '\n' {
		n++
	}
	return n
}
