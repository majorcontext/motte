package midx

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"testing"
)

func sha(i int) SHA { return sha1.Sum(binary.BigEndian.AppendUint32(nil, uint32(i))) }

func packs(n int) []Pack {
	out := make([]Pack, n)
	for i := range out {
		out[i] = Pack{ID: hex.EncodeToString(bytes.Repeat([]byte{byte(i + 1)}, 32)), Bytes: int64(100 * (i + 1))}
	}
	return out
}

type file struct{ l Layer }

func (f file) read(_ context.Context, off, n int64) ([]byte, error) {
	if n < 0 || off+n > int64(len(f.l.Data)) {
		n = int64(len(f.l.Data)) - off
	}
	return f.l.Data[off : off+n], nil
}

func open(t *testing.T, l Layer) (*Dir, Fetch) {
	t.Helper()
	d, err := Parse(l.Data[len(l.Data)-l.Dir:], int64(len(l.Data)))
	if err != nil {
		t.Fatal(err)
	}
	return d, func(ctx context.Context, i int) ([]byte, error) {
		off, n := d.BlockRange(i)
		return file{l}.read(ctx, off, n)
	}
}

func sorted(recs []Record) []Record {
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].compare(recs[j-1]) < 0; j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
	return recs
}

type sliceIter struct{ r []Record }

func (s *sliceIter) Next() (Record, bool, error) {
	if len(s.r) == 0 {
		return Record{}, false, nil
	}
	r := s.r[0]
	s.r = s.r[1:]
	return r, true, nil
}

func TestLookupFindsEveryRecordAndAnswersNoForOthers(t *testing.T) {
	t.Parallel()
	var recs []Record
	for i := range 1500 {
		recs = append(recs, Record{SHA: sha(i), Ord: uint32(i % 3), Off: uint32(12 + i)})
	}
	// One object held by all three packs, across a block border, must keep each copy.
	for ord := range 3 {
		recs = append(recs, Record{SHA: sha(-1), Ord: uint32(ord), Off: uint32(5000 + ord)})
	}
	b := NewBuilder(packs(3), len(recs))
	for _, r := range sorted(recs) {
		if err := b.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	l, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	d, fetch := open(t, l)
	for _, r := range recs {
		got, ok, err := d.Lookup(t.Context(), r.SHA, int(r.Ord), fetch)
		if err != nil || !ok || got != r {
			t.Fatalf("lookup of %x in pack %d = %+v, %v, %v; want %+v", r.SHA, r.Ord, got, ok, err, r)
		}
		if _, ok, _ := d.Lookup(t.Context(), r.SHA, -1, fetch); !ok {
			t.Fatalf("lookup of %x in any pack found nothing", r.SHA)
		}
	}
	for i := 2000; i < 2400; i++ {
		if _, ok, err := d.Lookup(t.Context(), sha(i), -1, fetch); ok || err != nil {
			t.Fatalf("lookup of absent %x = %v, %v", sha(i), ok, err)
		}
	}
	if _, ok, _ := d.Lookup(t.Context(), sha(1), 2, fetch); ok {
		t.Error("a lookup in a pack that does not hold the object found it")
	}
	var names []SHA
	for i := -1; i < 2400; i += 3 {
		names = append(names, sha(i))
	}
	slices.SortFunc(names, func(a, b SHA) int { return bytes.Compare(a[:], b[:]) })
	got, found, err := d.LookupMany(t.Context(), names, func(ctx context.Context, idx []int) ([][]byte, error) {
		var out [][]byte
		for _, i := range idx {
			blk, _ := fetch(ctx, i)
			out = append(out, blk)
		}
		return out, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		want, ok, _ := d.Lookup(t.Context(), n, -1, fetch)
		if found[i] != ok || got[i] != want {
			t.Fatalf("batched lookup of %x = %+v, %v; want %+v, %v", n, got[i], found[i], want, ok)
		}
	}
}

func TestMergeKeepsOrderAndCheckErrors(t *testing.T) {
	t.Parallel()
	mk := func(from, to, ord int) []Record {
		var r []Record
		for i := from; i < to; i++ {
			r = append(r, Record{SHA: sha(i), Ord: uint32(ord), Off: uint32(12 + i)})
		}
		return sorted(r)
	}
	a, b := mk(0, 600, 0), mk(300, 900, 0)
	l, seen, err := Merge(t.Context(), []Iter{&sliceIter{a}, &sliceIter{b}}, packs(2), 1200, func(in int, r Record) (Record, error) {
		r.Ord = uint32(in)
		return r, nil
	})
	if err != nil || seen != 1200 || l.Objects != 1200 {
		t.Fatalf("merge = %d objects, %d seen, %v; want 1200 records, 300 of them held by both packs", l.Objects, seen, err)
	}
	d, fetch := open(t, l)
	it := NewIter(t.Context(), d, file{l}.read)
	n := 0
	for {
		_, ok, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		n++
	}
	if n != 1200 {
		t.Errorf("iterated %d records, want 1200", n)
	}
	if _, ok, _ := d.Lookup(t.Context(), sha(450), 1, fetch); !ok {
		t.Error("the copy of a shared object in the second pack is lost")
	}
	if _, _, err := Merge(t.Context(), []Iter{&sliceIter{[]Record{a[1], a[0]}}}, packs(1), 2, func(_ int, r Record) (Record, error) { return r, nil }); err == nil {
		t.Error("an input out of order was merged")
	}
	l.Data[3] ^= 1
	it = NewIter(t.Context(), d, file{l}.read)
	var last error
	for ok := true; ok && last == nil; {
		_, ok, last = it.Next()
	}
	if last == nil {
		t.Error("a flipped byte passed the checksum of the layer")
	}
}

func TestPlansFollowTheGeometricRule(t *testing.T) {
	t.Parallel()
	e := func(objects int, packs int) Entry {
		return Entry{Key: "k", Objects: objects, Bytes: int64(objects) * 10, NPacks: packs}
	}
	for _, c := range []struct {
		name    string
		layers  []Entry
		merge   Plan
		compact Plan
	}{
		{"empty", nil, Plan{}, Plan{}},
		{"one pack", []Entry{e(100, 1)}, Plan{}, Plan{}},
		{"equal layers join", []Entry{e(100, 1), e(100, 1), e(100, 1)}, Plan{0, 3}, Plan{0, 3}},
		{"a layer over twice the newer ones stays", []Entry{e(1000, 1), e(100, 1), e(50, 1)}, Plan{1, 3}, Plan{1, 3}},
		{"sizes that double apart are left", []Entry{e(1000, 1), e(300, 1), e(100, 1)}, Plan{}, Plan{}},
		{"a layer of many packs compacts alone", []Entry{e(1000, 1), e(300, 5), e(100, 1)}, Plan{}, Plan{1, 2}},
		{"tiny layers under a larger newer one still join it", []Entry{e(5, 1), e(5, 1), e(5, 1), e(900, 1)}, Plan{0, 4}, Plan{0, 4}},
		{"small packs past the compaction cap are compacted before they merge", []Entry{{Key: "a", Objects: 400000, Bytes: 8 << 20, NPacks: 800}, {Key: "b", Objects: 400000, Bytes: 8 << 20, NPacks: 800}}, Plan{}, Plan{1, 2}},
		{"over the cap stays", []Entry{e(1<<20, 1), e(1<<20, 1)}, Plan{}, Plan{}},
	} {
		got, ok := PlanMerge(c.layers, 1<<20, 64, 10<<20)
		if (c.merge != Plan{}) != ok || (ok && got != c.merge) {
			t.Errorf("%s: merge = %+v, %v; want %+v", c.name, got, ok, c.merge)
		}
		got, ok = PlanCompact(c.layers, 10<<20, 1<<20, 64)
		if (c.compact != Plan{}) != ok || (ok && got != c.compact) {
			t.Errorf("%s: compact = %+v, %v; want %+v", c.name, got, ok, c.compact)
		}
	}
}

func TestASizeFileRefusesACorruptBlock(t *testing.T) {
	t.Parallel()
	vals := make([]uint32, 600)
	for i := range vals {
		vals[i] = EncodeSize(int64(i))
	}
	file := BuildSizes(vals)
	z, err := ParseSizes(file, len(vals))
	if err != nil || z.At(599) != vals[599] {
		t.Fatalf("parse: %v", err)
	}
	off, n := SizesBlockRange(1, len(vals))
	block := append([]byte{}, file[off:off+n]...)
	if _, err := BlockValues(1, block); err != nil {
		t.Fatalf("block 1: %v", err)
	}
	block[7] ^= 1
	if _, err := BlockValues(1, block); err == nil {
		t.Error("a block with a flipped bit passed its CRC")
	}
}
