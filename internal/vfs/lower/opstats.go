package lower

import (
	"context"
	"fmt"
	"log/slog"
	"math/bits"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/majorcontext/motte/internal/vfs/tel"
)

// opKind names a FUSE operation in the per-mount statistics.
type opKind uint8

const (
	opLookup opKind = iota
	opGetattr
	opReaddir
	opReadlink
	opOpen
	opRead
	opWrite
	opCreate
	opMkdir
	opSymlink
	opUnlink
	opRmdir
	opRename
	opSetattr
	opFsync
	opRelease
	opFlush
	opStatfs
	opAllocate
	numOps
)

var opNames = [numOps]string{
	"lookup", "getattr", "readdir", "readlink", "open", "read", "write", "create", "mkdir", "symlink",
	"unlink", "rmdir", "rename", "setattr", "fsync", "release", "flush", "statfs", "allocate",
}

const (
	histSub     = 4            // buckets per octave
	histBuckets = 64 * histSub // nanoseconds up to 2^64
	errnoSlots  = 128          // errors beyond this share the last slot
)

// opStat counts one operation. Every field is atomic, so the hot path takes no lock.
type opStat struct {
	count atomic.Uint64
	bytes atomic.Uint64
	max   atomic.Uint64
	errs  [errnoSlots]atomic.Uint32
	hist  [histBuckets]atomic.Uint32
}

// bucketOf maps a latency in nanoseconds to a bucket: one per nanosecond below 4, then four per power of two.
func bucketOf(ns uint64) int {
	if ns < 4 {
		return int(ns)
	}
	oct := bits.Len64(ns) - 1
	return min(oct*histSub+int(ns>>(oct-2))&(histSub-1), histBuckets-1)
}

// bucketUpper is the largest latency, in nanoseconds, that falls into bucket b.
func bucketUpper(b int) uint64 {
	if b < 4 {
		return uint64(b)
	}
	oct, sub := b/histSub, b%histSub
	return (uint64(histSub+sub+1) << (oct - 2)) - 1
}

func (s *opStat) record(d time.Duration, errno syscall.Errno, n int64) {
	ns := uint64(max(d, 0))
	s.count.Add(1)
	s.hist[bucketOf(ns)].Add(1)
	if n > 0 {
		s.bytes.Add(uint64(n))
	}
	if errno != 0 {
		s.errs[min(int(errno), errnoSlots-1)].Add(1)
	}
	for {
		cur := s.max.Load()
		if ns <= cur || s.max.CompareAndSwap(cur, ns) {
			return
		}
	}
}

// opWindow is what one operation did since the last emit.
type opWindow struct {
	count, bytes, max uint64
	p50, p95, p99     uint64
	errs              map[syscall.Errno]uint32
}

// take reads the counters of an interval and resets them.
func (s *opStat) take() opWindow {
	w := opWindow{count: s.count.Swap(0), bytes: s.bytes.Swap(0), max: s.max.Swap(0)}
	if w.count == 0 {
		return w
	}
	var hist [histBuckets]uint32
	var total uint64
	for i := range hist {
		hist[i] = s.hist[i].Swap(0)
		total += uint64(hist[i])
	}
	w.p50, w.p95, w.p99 = percentile(&hist, total, 0.50), percentile(&hist, total, 0.95), percentile(&hist, total, 0.99)
	for i := range s.errs {
		if n := s.errs[i].Swap(0); n > 0 {
			if w.errs == nil {
				w.errs = map[syscall.Errno]uint32{}
			}
			w.errs[syscall.Errno(i)] = n
		}
	}
	return w
}

func percentile(h *[histBuckets]uint32, total uint64, q float64) uint64 {
	want := uint64(float64(total)*q + 0.999999)
	var seen uint64
	for i, n := range h {
		if seen += uint64(n); seen >= want && n > 0 {
			return bucketUpper(i)
		}
	}
	return 0
}

// mountStats is the in-memory record of one mount between two vfs.stats records.
type mountStats struct {
	ops        [numOps]opStat
	slowDrops  atomic.Uint64
	slowTokens atomic.Int64 // slow-operation spans left in this interval
}

const slowSpansPerInterval = 20

func (m *mountStats) record(op opKind, d time.Duration, errno syscall.Errno, n int64) {
	m.ops[op].record(d, errno, n)
}

// slowAllowed spends one slow-operation span of the interval, or counts the span as dropped.
func (m *mountStats) slowAllowed() bool {
	if m.slowTokens.Add(-1) >= 0 {
		return true
	}
	m.slowDrops.Add(1)
	return false
}

func errnoList(errs map[syscall.Errno]uint32) string {
	keys := make([]int, 0, len(errs))
	for e := range errs {
		keys = append(keys, int(e))
	}
	sort.Ints(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", errnoName(syscall.Errno(k)), errs[syscall.Errno(k)])
	}
	return strings.Join(parts, ",")
}

func errnoName(e syscall.Errno) string {
	if e == errnoSlots-1 {
		return "other"
	}
	return strings.ReplaceAll(strings.ToUpper(e.Error()), " ", "_")
}

// extraStats are the figures a mount adds to its vfs.stats record besides the operations.
type extraStats struct {
	cache cacheShare
}

// cacheShare is the number of tree and blob reads that each tier served in an interval.
type cacheShare struct{ treeMemory, treeStore, blobDisk, blobStore uint64 }

// emitStats writes one vfs.stats record, or nothing when the interval had no activity. It returns whether it wrote.
func emitStats(t tel.Telemetry, ctx context.Context, m *mountStats, x extraStats, final bool) bool {
	args := t.Corr.LogAttrs()
	var active bool
	for op := range opKind(numOps) {
		w := m.ops[op].take()
		if w.count == 0 {
			continue
		}
		active = true
		n := opNames[op]
		args = append(args,
			slog.Uint64(n+"_count", w.count), slog.Uint64(n+"_bytes", w.bytes),
			slog.Float64(n+"_p50_us", us(w.p50)), slog.Float64(n+"_p95_us", us(w.p95)),
			slog.Float64(n+"_p99_us", us(w.p99)), slog.Float64(n+"_max_us", us(w.max)))
		if len(w.errs) > 0 {
			args = append(args, slog.String(n+"_errors", errnoList(w.errs)))
		}
	}
	drops := m.slowDrops.Swap(0)
	m.slowTokens.Store(slowSpansPerInterval)
	if !active && !final {
		return false
	}
	args = append(args,
		slog.Uint64("slow_spans_dropped", drops),
		slog.Uint64("tree_memory_hits", x.cache.treeMemory), slog.Uint64("tree_store_reads", x.cache.treeStore),
		slog.Uint64("blob_disk_hits", x.cache.blobDisk), slog.Uint64("blob_store_reads", x.cache.blobStore),
		slog.Bool("final", final))
	t.Log().InfoContext(ctx, "vfs.stats", args...)
	return true
}

func us(ns uint64) float64 { return float64(ns) / 1e3 }
