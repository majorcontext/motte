//go:build linux

package lower

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/majorcontext/motte/internal/vfs/tel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// opTimer times one FUSE operation. The operation reports its result with done.
type opTimer struct {
	v     *VFS
	op    opKind
	start time.Time
	ot    *tel.OpTrace
	depth int
}

// begin starts timing a FUSE operation. The returned context lets the store calls of the operation link to its
// slow-operation span.
func (v *VFS) begin(ctx context.Context, op opKind) (context.Context, *opTimer) {
	ctx, ot := tel.NewOpTrace(ctx)
	return ctx, &opTimer{v: v, op: op, start: time.Now(), ot: ot, depth: -1}
}

// at records the depth of the path of the operation. A slow-operation span carries the depth, never the path.
func (t *opTimer) at(p string) { t.depth = pathDepth(p) }

// done records the result of the operation: its errno and the bytes it moved.
func (t *opTimer) done(errno syscall.Errno, n int64) {
	d := time.Since(t.start)
	t.v.ms.record(t.op, d, errno, n)
	if t.op == opRead && errno == 0 {
		t.v.noteFirstRead()
	}
	if d >= slowThreshold(t.v.tel) && t.v.ms.slowAllowed() {
		t.v.slowSpan(t, d, errno, n)
	}
}

func slowThreshold(t tel.Telemetry) time.Duration {
	if t.SlowOp > 0 {
		return t.SlowOp
	}
	return tel.DefaultSlowOp
}

// slowSpan emits the span of one slow operation, started at the start of the operation.
func (v *VFS) slowSpan(t *opTimer, d time.Duration, errno syscall.Errno, n int64) {
	wait, links := t.ot.Waited()
	attrs := append(v.tel.Corr.SpanAttrs(),
		attribute.String("op", opNames[t.op]), attribute.Int("path_depth", t.depth), attribute.Int64("size", n),
		attribute.Bool("waited_on_store", wait > 0), attribute.Int64("store_wait_us", wait.Microseconds()),
		attribute.Int64("duration_us", d.Microseconds()))
	if errno != 0 {
		attrs = append(attrs, attribute.String("errno", errnoName(errno)))
	}
	_, sp := tel.Tracer().Start(context.Background(), "vfs.slow_op", trace.WithTimestamp(t.start), trace.WithAttributes(attrs...), trace.WithLinks(links...))
	sp.End(trace.WithTimestamp(t.start.Add(d)))
}

// noteFirstRead logs how long the first read of the mount came after the volume was published.
func (v *VFS) noteFirstRead() {
	if !v.firstRead.CompareAndSwap(false, true) {
		return
	}
	v.tel.Log().Info("vfs.first_read", append(v.tel.Corr.LogAttrs(), slog.Int64("publish_to_first_read_ms", time.Since(v.published).Milliseconds()))...)
}

func readSize(r fuse.ReadResult) int64 {
	if r == nil {
		return 0
	}
	return int64(r.Size())
}

// startStats emits a vfs.stats record every interval until Close.
func (v *VFS) startStats(ctx context.Context) {
	every := v.tel.StatsEvery
	if every <= 0 {
		every = tel.DefaultStatsEvery
	}
	v.ms.slowTokens.Store(slowSpansPerInterval)
	v.clk.TickerFunc(ctx, every, func() error { v.emitStats(false); return nil })
}

// emitStats writes the vfs.stats record of the interval that ends now.
func (v *VFS) emitStats(final bool) {
	var x extraStats
	x.cache = v.cacheShare()
	emitStats(v.tel, context.Background(), &v.ms, x, final)
}

// cacheShare returns the tree and blob reads that each tier served since the last call.
func (v *VFS) cacheShare() cacheShare {
	v.shareMu.Lock()
	defer v.shareMu.Unlock()
	var now [4]uint64
	now[0], now[1] = uint64(v.c.TreeHits.Load()), uint64(v.c.TreeMisses.Load())
	now[2], now[3] = uint64(v.c.BlobHits.Load()), uint64(v.c.BlobMisses.Load())
	s := cacheShare{now[0] - v.lastShare[0], now[1] - v.lastShare[1], now[2] - v.lastShare[2], now[3] - v.lastShare[3]}
	v.lastShare = now
	return s
}

// statsState holds the fields of VFS that the observability code owns.
type statsState struct {
	ms        mountStats
	firstRead atomic.Bool
	published time.Time
	shareMu   sync.Mutex
	lastShare [4]uint64
}

// pathDepth is the number of components of a slash-separated path; the empty path is the root.
func pathDepth(p string) int {
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}
