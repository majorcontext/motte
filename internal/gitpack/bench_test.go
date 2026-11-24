package gitpack

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

func benchBlob(n int) []byte {
	rng := rand.New(rand.NewPCG(1, 2))
	b := make([]byte, n)
	for i := range b {
		b[i] = "abcdefghij klmnopqrstuvwxyz\n"[rng.IntN(28)]
	}
	return b
}

func BenchmarkHash(b *testing.B) {
	for _, n := range []int{4 << 10, 64 << 10} {
		data := benchBlob(n)
		b.Run(fmt.Sprintf("%dKiB", n>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			b.ResetTimer()
			for range b.N {
				Hash(Blob, data)
			}
		})
	}
}

func BenchmarkInflate(b *testing.B) {
	for _, n := range []int{4 << 10, 256 << 10} {
		data := benchBlob(n)
		pack, idx, names := Build([]Object{{Type: Blob, Data: data}})
		p, err := NewPack(idx, bytes.NewReader(pack))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%dKiB", n>>10), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			b.ResetTimer()
			for range b.N {
				if _, _, _, err := p.GetDepth(names[0]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDeltaApply(b *testing.B) {
	base := benchBlob(64 << 10)
	insert := []byte("an inserted line of text\n")
	delta := binaryUvarint(nil, uint64(len(base)))
	delta = binaryUvarint(delta, uint64(len(base)+len(insert)))
	delta = append(delta, 0x80|0x20, 0x80)
	delta = append(delta, byte(len(insert)))
	delta = append(delta, insert...)
	delta = append(delta, 0x80|0x02|0x20, 0x80, 0x80)
	if out, err := applyDelta(base, delta); err != nil || len(out) != len(base)+len(insert) {
		b.Fatalf("fixture: %d bytes, %v", len(out), err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(base)))
	b.ResetTimer()
	for range b.N {
		if _, err := applyDelta(base, delta); err != nil {
			b.Fatal(err)
		}
	}
}

func binaryUvarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}
