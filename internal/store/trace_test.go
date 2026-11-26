package store

import (
	"context"
	"testing"
	"time"

	"github.com/coder/quartz"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSlowReadsGetASpanAtMostOnceASecond(t *testing.T) {
	t.Parallel()
	spans := tracetest.NewInMemoryExporter()
	clk := quartz.NewMock(t)
	s := &Service{clk: clk, slowAfter: slowReadAfter}
	WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)))(s)
	k := rangeKey{pack: "p", end: 10}
	start := clk.Now()
	for _, step := range []struct {
		took, wait time.Duration
		want       int
	}{{time.Millisecond, 0, 0}, {slowReadAfter + 100*time.Millisecond, 0, 1}, {slowReadAfter + 100*time.Millisecond, 0, 1}, {slowReadAfter + 100*time.Millisecond, 2 * slowReadEvery, 2}} {
		clk.Advance(step.wait)
		start = clk.Now()
		clk.Advance(step.took)
		s.slowRead(context.Background(), k, start, 10, nil)
		if got := len(spans.GetSpans()); got != step.want {
			t.Errorf("after a read of %v, %d spans, want %d", step.took, got, step.want)
		}
	}
}
