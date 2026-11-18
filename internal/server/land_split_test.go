package server

import (
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
)

func TestSplitObjectsKeepsOrderAndBothLimits(t *testing.T) {
	t.Parallel()
	objs := make([]gitpack.Object, 7)
	for i := range objs {
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: make([]byte, 10)}
	}
	for _, c := range []struct{ count, bytes, parts int }{{3, 1000, 3}, {10, 25, 4}, {10, 1000, 1}} {
		parts := splitObjects(objs, c.count, c.bytes)
		total := 0
		for _, p := range parts {
			size := 0
			for _, o := range p {
				size += len(o.Data)
			}
			if len(p) > c.count || size > c.bytes || len(p) == 0 {
				t.Errorf("%+v: part of %d objects and %d bytes", c, len(p), size)
			}
			total += len(p)
		}
		if len(parts) != c.parts || total != len(objs) {
			t.Errorf("%+v: %d parts of %d objects, want %d parts of %d", c, len(parts), total, c.parts, len(objs))
		}
	}
}
