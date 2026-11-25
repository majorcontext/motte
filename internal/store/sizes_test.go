package store_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
	"github.com/majorcontext/motte/internal/store"
)

// stripSizes drops the size file of each layer that keep rejects, as a layer of an older release has none.
func (w *world) stripSizes(keep func(i int) bool) {
	w.t.Helper()
	row, err := w.meta.Manifest(w.t.Context(), repo.String())
	if err != nil {
		w.t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		w.t.Fatal(err)
	}
	for i := range m.Layers {
		if !keep(i) {
			m.Layers[i].Sizes = ""
		}
	}
	body, _ := m.Encode()
	if _, err := w.meta.SwapManifest(w.t.Context(), repo.String(), row.Generation, body); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) sizedLayers() (sized, total int) {
	w.t.Helper()
	row, _ := w.meta.Manifest(w.t.Context(), repo.String())
	m, _ := midx.DecodeManifest(row.Body)
	for _, l := range m.Layers {
		if l.Sizes != "" {
			sized++
		}
	}
	return sized, len(m.Layers)
}

func TestBlobSizesComeFromTheIndexAndMatchTheHeaders(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(9, 10))
	var pushes [3][]gitpack.Object
	var dirs []byte
	for d := range pushes {
		var dir []byte
		for f := range 40 {
			data := make([]byte, 100<<10+rng.IntN(1000))
			if f == 0 {
				data = nil
			}
			fillRandom(rng, data)
			pushes[d] = append(pushes[d], gitpack.Object{Type: gitpack.Blob, Data: data})
			sha := gitpack.Hash(gitpack.Blob, data)
			dir = append(append(dir, fmt.Sprintf("100644 f%d\x00", f)...), sha[:]...)
		}
		pushes[d] = append(pushes[d], gitpack.Object{Type: gitpack.Tree, Data: dir})
		sha := gitpack.Hash(gitpack.Tree, dir)
		dirs = append(append(dirs, fmt.Sprintf("40000 d%d\x00", d)...), sha[:]...)
	}
	bulk := func(w *world, strip ...int) ([]store.Tree, int64) {
		puts := 0
		put := func(objs []gitpack.Object) {
			if _, err := w.svc.PutObjects(t.Context(), repo, objs); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(strip, puts) {
				w.stripSizes(func(int) bool { return false })
			}
			puts++
		}
		for _, objs := range pushes {
			put(objs)
		}
		put([]gitpack.Object{{Type: gitpack.Tree, Data: dirs}})
		svc := w.cold()
		trees, err := svc.TreeBulk(t.Context(), repo, gitpack.Hash(gitpack.Tree, dirs), 100000, func(int64) error { return nil })
		if err != nil || len(trees) != 4 {
			t.Fatalf("trees = %d, %v", len(trees), err)
		}
		return trees, svc.CacheStats().RangeBytes
	}
	full, mixed, old := newWorld(t), newWorld(t), newWorld(t)
	got, indexBytes := bulk(full)
	want, headerBytes := bulk(old, 0, 1, 2, 3)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sizes from the index differ from the sizes of the headers")
	}
	if indexBytes*10 > headerBytes {
		t.Errorf("a bulk read with sizes in the index read %d range bytes, against %d with headers: it must read no blob header", indexBytes, headerBytes)
	}
	if mix, _ := bulk(mixed, 0); !reflect.DeepEqual(mix, want) {
		t.Errorf("a layer with sizes next to a layer without gives other sizes than the headers")
	}
	if err := mixed.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	trees, err := mixed.cold().TreeBulk(t.Context(), repo, want[0].SHA, 100000, func(int64) error { return nil })
	if err != nil || !reflect.DeepEqual(trees, want) {
		t.Errorf("sizes after maintenance differ: %v", err)
	}
}

func TestAMergeReservesTheSizeFilesItReadsOrReadsNone(t *testing.T) {
	t.Parallel()
	const n = 1000
	plain := []midx.Entry{{Objects: n}, {Objects: n}}
	sized := []midx.Entry{store.SizedLayer(n), store.SizedLayer(n)}
	base, in := store.MergeNeed(plain, 2*n)
	got, inSized := store.MergeNeed(sized, 2*n)
	if got != base+4*2*n || !in || !inSized {
		t.Errorf("a merge of 2000 records with sized inputs reserves %d, want %d plus 8000 for the files that it reads", got, base)
	}
	huge := []midx.Entry{store.SizedLayer(8 << 20)}
	if _, readsInputs := store.MergeNeed(huge, 8<<20); readsInputs {
		t.Errorf("a merge that would pass the memory budget with its input size files must read none")
	}
}

// keyBucket counts the reads of size files, and holds the stores of the first push until its pack, index and size file
// have all arrived.
type keyBucket struct {
	bucket.Store
	ctx      context.Context
	sizeGets atomic.Int64
	arrived  atomic.Int64
	armed    atomic.Bool
	all      chan struct{}
}

func (b *keyBucket) read(key string) {
	if strings.HasSuffix(key, ".sz") {
		b.sizeGets.Add(1)
	}
}

func (b *keyBucket) Get(ctx context.Context, key string) ([]byte, error) {
	b.read(key)
	return b.Store.Get(ctx, key)
}

func (b *keyBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.read(key)
	return b.Store.GetRange(ctx, key, off, n)
}

func (b *keyBucket) Create(ctx context.Context, key string, data []byte) (bool, error) {
	if b.armed.Load() && (strings.HasSuffix(key, ".pack") || strings.HasSuffix(key, ".idx") || strings.HasSuffix(key, ".sz")) {
		if b.arrived.Add(1) == 3 {
			close(b.all)
		}
		select {
		case <-b.all:
		case <-b.ctx.Done():
			return false, errors.New("the stores of a push were not concurrent")
		}
	}
	return b.Store.Create(ctx, key, data)
}

func TestAPushStoresItsPackIndexAndSizesTogetherAndItsTailMergeReadsNoSizeFile(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	kb := &keyBucket{Store: w.mem, ctx: ctx, all: make(chan struct{})}
	svc := store.New(kb, w.meta, store.WithClock(w.clock), store.WithGrace(grace))
	push := func(i int) {
		if _, err := svc.PutObjects(ctx, repo, []gitpack.Object{{Type: gitpack.Blob, Data: fmt.Appendf(nil, "blob %d", i)}}); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	kb.armed.Store(true)
	push(0)
	kb.armed.Store(false)
	for i := 1; i < 6; i++ {
		push(i)
	}
	if w.layers() > 3 {
		t.Fatalf("%d layers: the tail merge did not run", w.layers())
	}
	if n := kb.sizeGets.Load(); n != 0 {
		t.Errorf("the tail merges of 5 pushes read a size file %d times; the size file that a push wrote is in the layer cache", n)
	}
}
