package server_test

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	spans     = tracetest.NewInMemoryExporter()
	traceOnce sync.Once
)

func traceInMemory() {
	traceOnce.Do(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))) })
}

// tracedClone is a clone whose requests all carry a trace of its own, g.trace, so that cases on other backends that run
// at the same time do not share the spans.
func (g *gitEnv) tracedClone(token string) string {
	traceInMemory()
	var id [16]byte
	_, _ = rand.Read(id[:])
	g.trace = hex.EncodeToString(id[:])
	return g.clone(token, "-c", "http.extraHeader=traceparent: 00-"+g.trace+"-00f067aa0ba902b7-01")
}

// checkFetchSpans fails the test unless the fetches of the trace traceID recorded each phase with counts.
func checkFetchSpans(t *testing.T, traceID string) {
	t.Helper()
	serves := map[string]bool{}
	phases := map[string]string{}
	var objects, misses int64
	for _, s := range spans.GetSpans() {
		if s.SpanContext.TraceID().String() != traceID {
			continue
		}
		if strings.HasPrefix(s.Name, "vcs.serve.") {
			serves[s.SpanContext.SpanID().String()] = true
		}
		phases[s.Name] = s.Parent.SpanID().String()
		for _, kv := range s.Attributes {
			switch kv.Key {
			case "objects":
				objects += kv.Value.AsInt64()
			case "range_cache_misses":
				misses += kv.Value.AsInt64()
			}
		}
	}
	for _, phase := range []string{"has_wants", "has_haves", "commit_walk", "edge_trees", "tree_walk", "pack_write"} {
		if parent, ok := phases["vcs.fetch."+phase]; !ok || !serves[parent] {
			t.Errorf("phase %s: recorded %v, parent %s, want a child of a serve span", phase, ok, parent)
		}
	}
	if objects == 0 || misses == 0 {
		t.Errorf("the spans of a clone count %d objects and %d range cache misses, want both above 0", objects, misses)
	}
}

// checkLandSpans fails the test unless the lands so far recorded their queue, merge, write and swap spans.
func checkLandSpans(t *testing.T) {
	t.Helper()
	{
		got := map[string]bool{}
		counted := false
		for _, s := range spans.GetSpans() {
			got[s.Name] = true
			for _, kv := range s.Attributes {
				counted = counted || s.Name == "vcs.land.merge" && kv.Key == "range_cache_misses"
			}
		}
		if !counted {
			t.Error("no merge span carries the counts of its reads")
		}
		for _, want := range []string{"vcs.land.enqueue", "vcs.land.wait", "vcs.land.batch", "vcs.land.merge", "vcs.land.write", "vcs.land.swap"} {
			if !got[want] {
				t.Errorf("no span %s", want)
			}
		}
	}
}
