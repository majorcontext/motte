package midx

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func benchCommits(n int) []Commit {
	cs := make([]Commit, n)
	for i := range cs {
		cs[i] = Commit{SHA: sha(i), Tree: sha(n + i), Time: int64(1700000000 + i), Gen: uint32(i + 1)}
		if i > 0 {
			cs[i].Parents = []SHA{sha(i - 1)}
		}
	}
	return cs
}

func BenchmarkGraphDecode(b *testing.B) {
	for _, n := range []int{10000, MaxGraphCommits} {
		file, err := BuildGraph(benchCommits(n))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(file)))
			b.ResetTimer()
			for range b.N {
				if _, err := ParseGraph(file); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGraphLookup(b *testing.B) {
	file, _ := BuildGraph(benchCommits(MaxGraphCommits))
	g, err := ParseGraph(file)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, ok := g.Lookup(sha(i % MaxGraphCommits)); !ok {
			b.Fatal("a commit is missing")
		}
	}
}

func BenchmarkLayerMerge(b *testing.B) {
	const n = 50000
	mk := func(from, ord int) []Record {
		r := make([]Record, 0, n)
		for i := from; i < from+n; i++ {
			var s SHA
			binary.BigEndian.PutUint32(s[:], uint32(i))
			r = append(r, Record{SHA: s, Ord: uint32(ord), Off: uint32(12 + i)})
		}
		return r
	}
	layers := [][]Record{mk(0, 0), mk(n/2, 1), mk(n, 2), mk(2*n, 3)}
	b.ReportAllocs()
	b.ReportMetric(float64(4*n), "records/op")
	b.ResetTimer()
	for range b.N {
		ins := make([]Iter, len(layers))
		for i, l := range layers {
			ins[i] = &sliceIter{l}
		}
		if _, _, err := Merge(b.Context(), ins, packs(4), 4*n, func(_ int, r Record) (Record, error) { return r, nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSizeFileDecode(b *testing.B) {
	for _, n := range []int{1 << 14, 1 << 20} {
		vals := make([]uint32, n)
		for i := range vals {
			vals[i] = EncodeSize(int64(i % 70000))
		}
		file := BuildSizes(vals)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(file)))
			b.ResetTimer()
			for range b.N {
				z, err := ParseSizes(file, n)
				if err != nil || z.At(n-1) != vals[n-1] {
					b.Fatalf("parse: %v", err)
				}
			}
		})
	}
}

func BenchmarkSizeBlockDecode(b *testing.B) {
	const n = 1 << 14
	vals := make([]uint32, n)
	for i := range vals {
		vals[i] = EncodeSize(int64(i))
	}
	file := BuildSizes(vals)
	off, l := SizesBlockRange(5, n)
	block := file[off : off+l]
	b.ReportAllocs()
	b.SetBytes(l)
	b.ResetTimer()
	for range b.N {
		v, err := BlockValues(5, block)
		if err != nil || ValueAt(v, 3) != vals[5*BlockRecords+3] {
			b.Fatalf("block: %v", err)
		}
	}
}
