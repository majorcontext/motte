package store_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta/sqlite"
)

type benchWorld struct {
	ctx  context.Context
	meta *sqlite.Store
	mem  *memBucket
	spy  *spy
	svc  *store.Service
}

func newBenchWorld(b *testing.B) *benchWorld {
	b.Helper()
	m, err := sqlite.Open(b.Context(), ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = m.Close() })
	w := &benchWorld{ctx: b.Context(), meta: m, mem: &memBucket{m: map[string][]byte{}}}
	w.spy = &spy{Store: w.mem}
	w.svc = store.New(w.spy, m, store.WithGrace(grace))
	return w
}

// cold is a service with empty caches over the same bucket, which counts its reads in the spy.
func (w *benchWorld) cold() *store.Service { return store.New(w.spy, w.meta, store.WithGrace(grace)) }

func (w *benchWorld) put(b *testing.B, objs []gitpack.Object) []gitpack.SHA {
	b.Helper()
	names, err := w.svc.PutObjects(w.ctx, repo, objs)
	if err != nil {
		b.Fatal(err)
	}
	return names
}

// layered pushes batches of 4^k objects, largest first, so that the tail merge keeps each as a layer of its own. It
// returns the name of an object of the oldest layer.
func (w *benchWorld) layered(b *testing.B, layers int) gitpack.SHA {
	b.Helper()
	var oldest gitpack.SHA
	for l := range layers {
		n := 1 << (2 * (layers - 1 - l))
		objs := make([]gitpack.Object, n)
		for i := range objs {
			objs[i] = gitpack.Object{Type: gitpack.Blob, Data: fmt.Appendf(nil, "layer %d object %d with some padding", l, i)}
		}
		names := w.put(b, objs)
		if l == 0 {
			oldest = names[0]
		}
	}
	return oldest
}

func BenchmarkObjectLookupAcrossLayers(b *testing.B) {
	for _, layers := range []int{1, 4, 7} {
		w := newBenchWorld(b)
		oldest := w.layered(b, layers)
		missing := gitpack.Hash(gitpack.Blob, []byte("not there"))
		for name, sha := range map[string]gitpack.SHA{"oldest": oldest, "missing": missing} {
			b.Run(fmt.Sprintf("layers=%d/%s", layers, name), func(b *testing.B) {
				if _, err := w.svc.Has(w.ctx, repo, []gitpack.SHA{sha}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := w.svc.Has(w.ctx, repo, []gitpack.SHA{sha}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkObjectLookupColdLayers(b *testing.B) {
	w := newBenchWorld(b)
	oldest := w.layered(b, 7)
	b.ReportAllocs()
	before := w.spy.reads.Load()
	b.ResetTimer()
	for range b.N {
		if _, err := w.cold().Has(w.ctx, repo, []gitpack.SHA{oldest}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(w.spy.reads.Load()-before)/float64(b.N), "bucket-reads/op")
}

// bulkTree builds dirs directories of files blobs of 16 KiB, and returns the name of the root tree.
func (w *benchWorld) bulkTree(b *testing.B, dirs, files int) gitpack.SHA {
	b.Helper()
	rng := rand.New(rand.NewPCG(3, 4))
	var objs []gitpack.Object
	var root []byte
	for d := range dirs {
		var dir []byte
		for f := range files {
			data := make([]byte, 16<<10)
			for i := range data {
				data[i] = byte(rng.Uint32())
			}
			objs = append(objs, gitpack.Object{Type: gitpack.Blob, Data: data})
			sha := gitpack.Hash(gitpack.Blob, data)
			dir = append(append(dir, fmt.Sprintf("100644 f%d\x00", f)...), sha[:]...)
		}
		objs = append(objs, gitpack.Object{Type: gitpack.Tree, Data: dir})
		sha := gitpack.Hash(gitpack.Tree, dir)
		root = append(append(root, fmt.Sprintf("40000 d%d\x00", d)...), sha[:]...)
	}
	objs = append(objs, gitpack.Object{Type: gitpack.Tree, Data: root})
	w.put(b, objs)
	return gitpack.Hash(gitpack.Tree, root)
}

func (w *benchWorld) stripSizes(b *testing.B) {
	b.Helper()
	row, err := w.meta.Manifest(w.ctx, repo.String())
	if err != nil {
		b.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		b.Fatal(err)
	}
	for i := range m.Layers {
		m.Layers[i].Sizes = ""
	}
	body, _ := m.Encode()
	if _, err := w.meta.SwapManifest(w.ctx, repo.String(), row.Generation, body); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTreeBulk(b *testing.B) {
	for _, mode := range []string{"sizes", "headers"} {
		b.Run(mode, func(b *testing.B) {
			w := newBenchWorld(b)
			root := w.bulkTree(b, 20, 30)
			if mode == "headers" {
				w.stripSizes(b)
			}
			b.ReportAllocs()
			before := w.spy.reads.Load()
			b.ResetTimer()
			for range b.N {
				trees, err := w.cold().TreeBulk(w.ctx, repo, root, 100000, func(int64) error { return nil })
				if err != nil || len(trees) != 21 {
					b.Fatalf("trees = %d, %v", len(trees), err)
				}
			}
			b.ReportMetric(float64(w.spy.reads.Load()-before)/float64(b.N), "bucket-reads/op")
		})
	}
}
