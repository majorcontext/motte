// Package tel carries the correlation attributes, spans and log records of the node daemon.
package tel

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("github.com/majorcontext/motte/internal/vfs/tel")

// Correlation names the workspace and the mount in every span and log record of the daemon.
type Correlation struct {
	WorkspaceID, Repo, MountID, Mode, Runtime, Node string
	Epoch                                           uint64
}

func (c Correlation) SpanAttrs() []attribute.KeyValue {
	var out []attribute.KeyValue
	for _, kv := range [][2]string{{"workspace_id", c.WorkspaceID}, {"repo", c.Repo}, {"mount_id", c.MountID}, {"mode", c.Mode}, {"runtime", c.Runtime}, {"node", c.Node}} {
		if kv[1] != "" {
			out = append(out, attribute.String(kv[0], kv[1]))
		}
	}
	if c.Epoch > 0 {
		out = append(out, attribute.Int64("claim_epoch", int64(c.Epoch)))
	}
	return out
}

func (c Correlation) LogAttrs() []any {
	var out []any
	for _, kv := range [][2]string{{"workspace_id", c.WorkspaceID}, {"repo", c.Repo}, {"mount_id", c.MountID}, {"mode", c.Mode}, {"runtime", c.Runtime}, {"node", c.Node}} {
		if kv[1] != "" {
			out = append(out, slog.String(kv[0], kv[1]))
		}
	}
	if c.Epoch > 0 {
		out = append(out, slog.Uint64("claim_epoch", c.Epoch))
	}
	return out
}

// Telemetry configures the spans and log records of one mount. The zero value logs nothing and sends spans to the
// global provider, which exports none unless the process installed a tracer provider.
type Telemetry struct {
	Logger *slog.Logger
	Corr   Correlation
	// SlowOp is the latency above which a FUSE operation emits a span; zero is 50 ms.
	SlowOp time.Duration
	// StatsEvery is the interval of the vfs.stats record; zero is 30 s.
	StatsEvery time.Duration
	// Tracer starts the spans; nil is the package tracer.
	Tracer trace.Tracer
}

const (
	DefaultSlowOp     = 50 * time.Millisecond
	DefaultStatsEvery = 30 * time.Second
)

func (t Telemetry) Log() *slog.Logger {
	if t.Logger != nil {
		return t.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// start begins a span that carries the correlation attributes.
func (t Telemetry) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tr := t.Tracer
	if tr == nil {
		tr = tracer
	}
	return tr.Start(ctx, name, trace.WithAttributes(append(t.Corr.SpanAttrs(), attrs...)...))
}

// finish ends a span and marks it failed when err is not nil.
func Finish(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "")
	}
	span.End()
}

// logError logs one failure with the correlation attributes. It never logs a path, a grant or file content.
func (t Telemetry) LogError(ctx context.Context, msg string, err error, attrs ...any) {
	args := append(t.Corr.LogAttrs(), attrs...)
	t.Log().ErrorContext(ctx, msg, append(args, slog.String("error", err.Error()))...)
}

// WithTraceparent returns ctx with the remote span context of a W3C traceparent header value. A value that does not
// parse leaves ctx unchanged.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// OpTrace collects the store calls that one FUSE operation waits on, so that a slow-operation span can link to them.
type OpTrace struct {
	mu    sync.Mutex
	links []trace.Link
	wait  time.Duration
}

type opTraceKey struct{}

const maxOpLinks = 4

// NewOpTrace returns ctx with a new OpTrace that the store calls under ctx report to.
func NewOpTrace(ctx context.Context) (context.Context, *OpTrace) {
	ot := &OpTrace{}
	return context.WithValue(ctx, opTraceKey{}, ot), ot
}

// OpTraceFrom returns the OpTrace of ctx, or nil.
func OpTraceFrom(ctx context.Context) *OpTrace {
	t, _ := ctx.Value(opTraceKey{}).(*OpTrace)
	return t
}

// AddStore records one store call of the operation.
func (o *OpTrace) AddStore(sc trace.SpanContext, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.wait += d
	if len(o.links) < maxOpLinks && sc.IsValid() {
		o.links = append(o.links, trace.Link{SpanContext: sc})
	}
}

// Waited returns the time that the operation spent on the store and the spans of its first calls.
func (o *OpTrace) Waited() (time.Duration, []trace.Link) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.wait, append([]trace.Link(nil), o.links...)
}

// Tracer is the tracer of the node daemon.
func Tracer() trace.Tracer { return tracer }
