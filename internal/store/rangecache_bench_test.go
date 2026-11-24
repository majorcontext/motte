package store

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkRangeCacheHit(b *testing.B) {
	c := newRangeCache(64 << 20)
	keys := make([]rangeKey, 1024)
	for i := range keys {
		keys[i] = rangeKey{pack: "p", off: int64(i) * 4096, end: int64(i)*4096 + 4096}
		c.put(keys[i], make([]byte, 4096), c.version())
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := c.get(keys[i%len(keys)]); !ok {
				b.Error("a range of the cache is missing")
				return
			}
			i++
		}
	})
}

func BenchmarkRangeCacheMiss(b *testing.B) {
	c := newRangeCache(64 << 20)
	k := rangeKey{pack: "absent", off: 0, end: 10}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok := c.get(k); ok {
				b.Error("a range that was never stored is in the cache")
				return
			}
		}
	})
}

func BenchmarkRangeCacheInsert(b *testing.B) {
	data := make([]byte, 4096)
	for _, max := range []int64{256 << 10, 64 << 20} {
		b.Run(fmt.Sprintf("max=%dKiB", max>>10), func(b *testing.B) {
			c := newRangeCache(max)
			v := c.version()
			b.ReportAllocs()
			b.ResetTimer()
			var n atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := n.Add(1)
					c.put(rangeKey{pack: "p", off: i * 4096, end: i*4096 + 4096}, data, v)
				}
			})
		})
	}
}
