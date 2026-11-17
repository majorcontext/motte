package vfs

import (
	"context"
	"log/slog"
	"math"
	"os"
	"runtime/metrics"
	"time"

	"github.com/majorcontext/motte/internal/clock"
)

const defaultHealthEvery = 60 * time.Second

// Health logs the vfs.health record of the daemon process: the signal to read when the Go runtime shows up in the hot
// path of a mount.
type Health struct {
	Logger *slog.Logger
	Node   string
	// Mounts returns the number of mounts the process serves.
	Mounts func() int
	Every  time.Duration
	Clock  clock.Clock

	prevPauses []uint64
}

var healthMetrics = []string{"/sched/goroutines:goroutines", "/memory/classes/heap/objects:bytes", "/sched/pauses/total/gc:seconds"}

// Run writes a record every Every until ctx ends.
func (h *Health) Run(ctx context.Context) {
	every := h.Every
	if every <= 0 {
		every = defaultHealthEvery
	}
	clock.Or(h.Clock).TickerFunc(ctx, every, func() error { h.emit(ctx); return nil })
}

func (h *Health) emit(ctx context.Context) {
	samples := make([]metrics.Sample, len(healthMetrics))
	for i, n := range healthMetrics {
		samples[i].Name = n
	}
	metrics.Read(samples)
	args := []any{slog.String("node", h.Node), slog.Int("mounts", h.Mounts()), slog.Int("open_fds", openFDs()),
		slog.Int64("tier2_bytes", Tier2Bytes()), slog.Int64("tier2_evictions", 0)}
	if v := samples[0].Value; v.Kind() == metrics.KindUint64 {
		args = append(args, slog.Uint64("goroutines", v.Uint64()))
	}
	if v := samples[1].Value; v.Kind() == metrics.KindUint64 {
		args = append(args, slog.Uint64("heap_bytes", v.Uint64()))
	}
	if v := samples[2].Value; v.Kind() == metrics.KindFloat64Histogram {
		args = append(args, slog.Float64("gc_pause_p99_us", h.pauseP99(v.Float64Histogram())*1e6))
	}
	h.logger().InfoContext(ctx, "vfs.health", args...)
}

func (h *Health) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// pauseP99 is the 99th percentile, in seconds, of the GC pauses since the last record.
func (h *Health) pauseP99(hist *metrics.Float64Histogram) float64 {
	delta := make([]uint64, len(hist.Counts))
	var total uint64
	for i, c := range hist.Counts {
		if i < len(h.prevPauses) {
			c -= h.prevPauses[i]
		}
		delta[i], total = c, total+c
	}
	h.prevPauses = append(h.prevPauses[:0], hist.Counts...)
	want := uint64(float64(total)*0.99 + 0.999999)
	var seen uint64
	for i, c := range delta {
		if seen += c; c > 0 && seen >= want {
			if b := hist.Buckets[i+1]; !math.IsInf(b, 1) {
				return b
			}
			return hist.Buckets[i]
		}
	}
	return 0
}

func openFDs() int {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(ents)
}
