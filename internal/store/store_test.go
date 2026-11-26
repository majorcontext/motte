package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/quartz"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta/sqlite"
)

var repo = store.Repo{Prefix: "native", Path: "repo_x"}

const grace = time.Hour

// spy counts the writes and reads of a bucket, and fails the failAt-th write: before it happens, or after it landed.
type spy struct {
	bucket.Store
	writes, reads    atomic.Int64
	failAt           int64
	onDelete         func(key string)
	afterLanding     bool
	unconditional    bool
	mu               sync.Mutex
	files, deletions int
}

func (b *spy) write(do func() error) error {
	n := b.writes.Add(1)
	if b.failAt == n && !b.afterLanding {
		return errors.New("crash")
	}
	if err := do(); err != nil {
		return err
	}
	if b.failAt == n {
		return errors.New("crash after the write")
	}
	return nil
}

func (b *spy) Create(ctx context.Context, key string, data []byte) (created bool, err error) {
	err = b.write(func() (err error) { created, err = b.Store.Create(ctx, key, data); return })
	return created, err
}

func (b *spy) Delete(ctx context.Context, key, version string) error {
	if b.onDelete != nil {
		b.onDelete(key)
	}
	if b.unconditional {
		v, err := b.Store.Version(ctx, key)
		if errors.Is(err, bucket.ErrNotFound) {
			return nil
		}
		version = v
	}
	return b.write(func() error { b.mu.Lock(); b.deletions++; b.mu.Unlock(); return b.Store.Delete(ctx, key, version) })
}

func (b *spy) Get(ctx context.Context, key string) ([]byte, error) {
	b.reads.Add(1)
	return b.Store.Get(ctx, key)
}

func (b *spy) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.reads.Add(1)
	return b.Store.GetRange(ctx, key, off, n)
}

type world struct {
	t     *testing.T
	meta  *sqlite.Store
	mem   *memBucket
	spy   *spy
	clock *quartz.Mock
	svc   *store.Service
	have  []gitpack.SHA
	sent  [][]gitpack.Object
	n     int
}

func newWorld(t *testing.T) *world {
	t.Helper()
	m, err := sqlite.Open(t.Context(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	w := &world{t: t, meta: m, mem: &memBucket{m: map[string][]byte{}}, clock: quartz.NewMock(t)}
	w.spy = &spy{Store: w.mem}
	w.svc = store.New(w.spy, m, store.WithClock(w.clock), store.WithGrace(grace))
	return w
}

func (w *world) push(objects int) {
	w.t.Helper()
	objs := make([]gitpack.Object, objects)
	for i := range objs {
		w.n++
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: fmt.Appendf(nil, "blob %d with some padding to give it weight", w.n)}
	}
	names, err := w.svc.PutObjects(w.t.Context(), repo, objs)
	if err != nil {
		w.t.Fatal(err)
	}
	w.have = append(w.have, names...)
	w.sent = append(w.sent, objs)
}

func (w *world) layers() int {
	w.t.Helper()
	row, err := w.meta.Manifest(w.t.Context(), repo.String())
	if err != nil {
		w.t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		w.t.Fatal(err)
	}
	return len(m.Layers)
}

func (w *world) pastGrace() { w.clock.Advance(grace + time.Minute).MustWait(w.t.Context()) }

// cold is a service with empty caches on the same state.
func (w *world) cold() *store.Service {
	return store.New(w.mem, w.meta, store.WithClock(w.clock), store.WithGrace(grace))
}

func (w *world) reachable(svc *store.Service) {
	w.t.Helper()
	ctx := w.t.Context()
	got, err := svc.Has(ctx, repo, w.have)
	if err != nil {
		w.t.Fatal(err)
	}
	for i, ok := range got {
		if !ok {
			w.t.Fatalf("object %s is not reachable after %d layers", w.have[i], w.layers())
		}
	}
	for _, sha := range []gitpack.SHA{w.have[0], w.have[len(w.have)/2], w.have[len(w.have)-1]} {
		if _, _, err := svc.Get(ctx, repo, sha); err != nil {
			w.t.Fatalf("read %s: %v", sha, err)
		}
	}
}

type bytesAt []byte

func (b bytesAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (b bytesAt) Size() int64 { return int64(len(b)) }

func TestEveryObjectStaysReachableThroughPushesAndMaintenance(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for round := range 4 {
		for range 9 {
			w.push(3)
			if layers := w.layers(); layers > 8 {
				t.Fatalf("%d layers after %d pushes: the tail merge must keep them logarithmic", layers, len(w.have)/3)
			}
		}
		w.reachable(w.cold())
		if err := w.svc.Maintain(t.Context(), repo); err != nil {
			t.Fatal(err)
		}
		w.reachable(w.cold())
		if round == 1 {
			w.pastGrace()
			if err := w.svc.Maintain(t.Context(), repo); err != nil {
				t.Fatal(err)
			}
			w.reachable(w.cold())
		}
	}
	st := w.svc.MaintStats()
	if st.CompactRuns == 0 || st.MergeRuns == 0 || st.Deleted == 0 {
		t.Errorf("maintenance did nothing to be proud of: %+v", st)
	}
	packs, err := w.meta.Packs(t.Context(), repo.String())
	if err != nil || len(packs) >= 36 {
		t.Errorf("%d pack rows after compaction and deletion, %v; want fewer than the 36 pushed", len(packs), err)
	}
}

func TestReadingSmallObjectsOfABigPackCostsFewSmallRanges(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(1, 2))
	objs := make([]gitpack.Object, 2000)
	for i := range objs {
		data := make([]byte, 400)
		fillRandom(rng, data)
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: data}
	}
	names, err := w.svc.PutObjects(t.Context(), repo, objs)
	if err != nil {
		t.Fatal(err)
	}
	read := func(shas []gitpack.SHA, prefetch bool) store.CacheStats {
		svc := w.cold()
		rd, err := svc.Reader(t.Context(), repo)
		if err != nil {
			t.Fatal(err)
		}
		if prefetch {
			rd.Prefetch(t.Context(), shas)
		}
		for _, sha := range shas {
			if _, _, err := rd.Get(t.Context(), sha); err != nil {
				t.Fatal(err)
			}
		}
		return svc.CacheStats()
	}
	var spread []gitpack.SHA
	for i := 0; i < len(names); i += 100 {
		spread = append(spread, names[i])
	}
	near := names[:200]
	if st := read(spread, false); st.RangeReads > int64(len(spread)) || st.RangeBytes > int64(len(spread))*16<<10 {
		t.Fatalf("%d objects cost %d range reads and %d bytes: a read must fetch the bytes of its object, not a block", len(spread), st.RangeReads, st.RangeBytes)
	}
	if st := read(near, true); st.RangeReads > 2 || st.RangeBytes > 200*500 {
		t.Fatalf("%d near objects cost %d range reads and %d bytes after a prefetch: near objects must share ranges", len(near), st.RangeReads, st.RangeBytes)
	}
}

type slowBucket struct {
	bucket.Store
	wait func()
}

func (b slowBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.wait()
	return b.Store.GetRange(ctx, key, off, n)
}

func TestPrefetchReadsFarApartRangesAtTheSameTime(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(3, 4))
	objs := make([]gitpack.Object, 200)
	for i := range objs {
		data := make([]byte, 20<<10)
		fillRandom(rng, data)
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: data}
	}
	names, err := w.svc.PutObjects(t.Context(), repo, objs)
	if err != nil {
		t.Fatal(err)
	}
	var far []gitpack.SHA
	for i := 0; i < len(names); i += 20 {
		far = append(far, names[i])
	}
	synctest.Test(t, func(t *testing.T) {
		svc := store.New(slowBucket{w.mem, func() { time.Sleep(20 * time.Millisecond) }}, w.meta, store.WithGrace(grace))
		rd, err := svc.Reader(t.Context(), repo)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		rd.Prefetch(t.Context(), far)
		if d := time.Since(start); d > 100*time.Millisecond {
			t.Fatalf("a prefetch of %d far apart ranges took %v of 20 ms reads: it must not read them one after another", len(far), d)
		}
		before := svc.CacheStats().RangeReads
		for _, sha := range far {
			if _, _, err := rd.Get(t.Context(), sha); err != nil {
				t.Fatal(err)
			}
		}
		if got := svc.CacheStats().RangeReads - before; got != 0 {
			t.Fatalf("reading %d prefetched objects of 20 KiB made %d range reads; the prefetch must keep each object whole", len(far), got)
		}
		rd.Prefetch(t.Context(), far)
		if again := svc.CacheStats().RangeReads - before; again != 0 {
			t.Fatalf("a second prefetch of the same objects made %d range reads; it must read none", again)
		}
	})
}

func TestABulkTreeReadReadsTheBlobHeadersOfManyTreesTogether(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(5, 6))
	var objs []gitpack.Object
	add := func(o gitpack.Object) gitpack.SHA {
		objs = append(objs, o)
		return gitpack.Hash(o.Type, o.Data)
	}
	filler := func() {
		data := make([]byte, 140<<10)
		fillRandom(rng, data)
		objs = append(objs, gitpack.Object{Type: gitpack.Blob, Data: data})
	}
	var root []byte
	for d := range 60 {
		var dir []byte
		for f := range 3 {
			filler()
			sha := add(gitpack.Object{Type: gitpack.Blob, Data: fmt.Appendf(nil, "file %d of dir %d", f, d)})
			dir = append(append(dir, fmt.Sprintf("100644 f%d\x00", f)...), sha[:]...)
		}
		sha := add(gitpack.Object{Type: gitpack.Tree, Data: dir})
		root = append(append(root, fmt.Sprintf("40000 d%02d\x00", d)...), sha[:]...)
	}
	rootSHA := add(gitpack.Object{Type: gitpack.Tree, Data: root})
	if _, err := w.svc.PutObjects(t.Context(), repo, objs); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		svc := store.New(slowBucket{w.mem, func() { time.Sleep(20 * time.Millisecond) }}, w.meta, store.WithGrace(grace))
		start := time.Now()
		trees, err := svc.TreeBulk(t.Context(), repo, rootSHA, 100000, func(int64) error { return nil })
		if err != nil || len(trees) != 61 {
			t.Fatalf("trees = %d, %v", len(trees), err)
		}
		for _, e := range trees[1].Entries {
			if e.Size == 0 {
				t.Errorf("blob %s has no size", e.Name)
			}
		}
		if d := time.Since(start); d > 400*time.Millisecond {
			t.Errorf("a bulk read of 61 trees took %v of 20 ms reads: it must read their blob headers together, not one tree after another", d)
		}
	})
}

func TestRequestsThatAskForOneRangeTogetherMakeOneBucketRead(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.push(20)
	synctest.Test(t, func(t *testing.T) {
		svc := store.New(slowBucket{w.mem, func() { time.Sleep(20 * time.Millisecond) }}, w.meta, store.WithGrace(grace))
		if _, err := svc.Has(t.Context(), repo, w.have[:1]); err != nil {
			t.Fatal(err)
		}
		before := svc.CacheStats().RangeReads
		var wg sync.WaitGroup
		for range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, _, err := svc.Get(t.Context(), repo, w.have[0]); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if got := svc.CacheStats().RangeReads - before; got > 3 {
			t.Errorf("32 requests for one object made %d bucket reads of its pack, want at most 3", got)
		}
	})
}

func TestSharedBucketReadsHoldAtMostTheReadBudget(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(9, 10))
	objs := make([]gitpack.Object, 24)
	for i := range objs {
		data := make([]byte, 240<<10)
		fillRandom(rng, data)
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: data}
	}
	names, err := w.svc.PutObjects(t.Context(), repo, objs)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		var inFlight, peak atomic.Int64
		bk := slowBucket{w.mem, func() { time.Sleep(20 * time.Millisecond) }}
		svc := store.New(countingBucket{bk, &inFlight, &peak}, w.meta, store.WithGrace(grace), store.WithReadBudget(1))
		var wg sync.WaitGroup
		for _, name := range names {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rd, err := svc.Reader(t.Context(), repo)
				if err != nil {
					t.Error(err)
					return
				}
				rd.Prefetch(t.Context(), []gitpack.SHA{name})
			}()
		}
		wg.Wait()
		if got := peak.Load(); got > store.MinReadBudget/3 || got == 0 {
			t.Errorf("shared bucket reads held %d bytes at once, want some and at most a third of the least budget, %d, because a read reserves three times its length", got, store.MinReadBudget/3)
		}
	})
}

// countingBucket tracks the bytes of the pack reads that are in flight.
type countingBucket struct {
	slowBucket
	inFlight, peak *atomic.Int64
}

func (b countingBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	now := b.inFlight.Add(n)
	for p := b.peak.Load(); now > p && !b.peak.CompareAndSwap(p, now); p = b.peak.Load() {
	}
	defer b.inFlight.Add(-n)
	return b.slowBucket.GetRange(ctx, key, off, n)
}

func TestTheRangeCacheStaysWithinItsBudget(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(7, 8))
	objs := make([]gitpack.Object, 200)
	for i := range objs {
		data := make([]byte, 20<<10)
		fillRandom(rng, data)
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: data}
	}
	names, err := w.svc.PutObjects(t.Context(), repo, objs)
	if err != nil {
		t.Fatal(err)
	}
	const budget = 512 << 10
	svc := store.New(w.mem, w.meta, store.WithGrace(grace), store.WithRangeCache(budget))
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(names); i += 20 {
		rd.Prefetch(t.Context(), names[i:i+20])
	}
	st := svc.CacheStats()
	if st.RangeCacheBytes > budget || st.RangeCacheBytes == 0 || st.RangeCacheEvictions == 0 {
		t.Errorf("range cache holds %d bytes with %d evictions for a budget of %d, want some bytes, some evictions and no more than the budget", st.RangeCacheBytes, st.RangeCacheEvictions, budget)
	}
}

func TestADeletedPacksRangesLeaveTheRangeCache(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for range 10 {
		w.push(2)
	}
	ctx := t.Context()
	rd, err := w.svc.Reader(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.svc.Maintain(ctx, repo); err != nil {
		t.Fatal(err)
	}
	objs, victim := w.retiredPack()
	if _, _, err := rd.Get(ctx, gitpack.Hash(objs[0].Type, objs[0].Data)); err != nil {
		t.Fatal(err)
	}
	if w.svc.CacheStats().RangeCacheBytes == 0 {
		t.Fatal("the read of the retired pack left nothing in the range cache")
	}
	w.pastGrace()
	if err := w.svc.Maintain(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := w.mem.Get(ctx, "repos/native/"+b64("repo_x")+"/"+victim); err == nil {
		t.Fatal("maintenance did not delete the retired pack")
	}
	if n := w.svc.CacheStats().RangeCacheBytes; n != 0 {
		t.Errorf("the range cache holds %d bytes of ranges of a pack that maintenance deleted", n)
	}
}

// pushHistory stores a random history of n commits, with merges, parents first, in batches of 7 commits, and returns
// the commits in push order with their parents.
func (w *world) pushHistory(n int) (names []gitpack.SHA, parents map[gitpack.SHA][]gitpack.SHA, times map[gitpack.SHA]int64) {
	w.t.Helper()
	rng := rand.New(rand.NewPCG(11, 12))
	tree := gitpack.Hash(gitpack.Tree, nil)
	parents, times = map[gitpack.SHA][]gitpack.SHA{}, map[gitpack.SHA]int64{}
	batch := []gitpack.Object{{Type: gitpack.Tree, Data: nil}}
	flush := func() {
		pack, idx, _ := gitpack.Build(batch)
		sum := sha256sum(pack)
		if err := w.svc.ImportPack(w.t.Context(), repo, fmt.Sprintf("%x", sum), idx, pack); err != nil {
			w.t.Fatal(err)
		}
		batch = nil
	}
	for i := range n {
		var ps []gitpack.SHA
		for range min(i, 1+rng.IntN(3)) {
			if p := names[rng.IntN(len(names))]; !slices.Contains(ps, p) {
				ps = append(ps, p)
			}
		}
		text := "tree " + tree.String() + "\n"
		for _, p := range ps {
			text += "parent " + p.String() + "\n"
		}
		when := int64(1700000000 + i)
		text += fmt.Sprintf("author a <a@b> %d +0000\ncommitter a <a@b> %d +0000\n\ncommit %d\n", when, when, i)
		sha := gitpack.Hash(gitpack.Commit, []byte(text))
		names, parents[sha], times[sha] = append(names, sha), ps, when
		batch = append(batch, gitpack.Object{Type: gitpack.Commit, Data: []byte(text)})
		if len(batch) == 7 {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
	return names, parents, times
}

func (w *world) checkGraph(svc *store.Service, names []gitpack.SHA, parents map[gitpack.SHA][]gitpack.SHA, times map[gitpack.SHA]int64) {
	w.t.Helper()
	rd, err := svc.Reader(w.t.Context(), repo)
	if err != nil {
		w.t.Fatal(err)
	}
	gen := map[gitpack.SHA]uint32{}
	before := svc.CacheStats().RangeReads
	for _, sha := range names {
		_, ps, when, ok, err := rd.GraphCommit(w.t.Context(), sha)
		if err != nil || !ok || when != times[sha] || !slices.Equal(ps, parents[sha]) {
			w.t.Fatalf("graph of %s = %v, %d, %v, %v; want parents %v at %d", sha, ps, when, ok, err, parents[sha], times[sha])
		}
		gen[sha] = 1
		for _, p := range ps {
			gen[sha] = max(gen[sha], gen[p]+1)
		}
	}
	if got := svc.CacheStats().RangeReads - before; got != 0 {
		w.t.Errorf("reading the commits from the graph made %d pack reads, want 0", got)
	}
}

// checkGraphSizes checks that the manifest records the length of each graph file, with the overflow list of a commit
// with more than two parents.
func (w *world) checkGraphSizes() {
	w.t.Helper()
	row, err := w.meta.Manifest(w.t.Context(), repo.String())
	if err != nil {
		w.t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, e := range m.Layers {
		if e.Graph == "" {
			continue
		}
		data, err := w.mem.Get(w.t.Context(), "repos/native/"+b64("repo_x")+"/"+e.Graph)
		if err != nil || int64(len(data)) != e.GraphSize {
			w.t.Errorf("graph %s holds %d bytes, manifest says %d (%v)", e.Graph, len(data), e.GraphSize, err)
		}
	}
}

func TestGraphLookupsOfOneRequestTakeTheLayerCacheOncePerGraph(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	names, _, _ := w.pushHistory(200)
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	svc := w.cold()
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	before := svc.CacheStats()
	for _, sha := range names {
		if _, _, _, ok, err := rd.GraphCommit(t.Context(), sha); !ok || err != nil {
			t.Fatalf("GraphCommit = %v, %v", ok, err)
		}
	}
	after := svc.CacheStats()
	if n := after.IndexHits + after.IndexMisses - before.IndexHits - before.IndexMisses; n > 8 {
		t.Errorf("200 lookups took the layer cache %d times, want once for each graph of the request", n)
	}
}

func TestAMissingGraphIsLoadedOncePerRequestNotOncePerCommit(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	names, _, _ := w.pushHistory(60)
	row, err := w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range m.Layers {
		if e.Graph != "" {
			key := "repos/native/" + b64("repo_x") + "/" + e.Graph
			v, err := w.mem.Version(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.mem.Delete(t.Context(), key, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	svc := store.New(w.spy, w.meta, store.WithGrace(grace))
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	before := w.spy.reads.Load()
	for _, sha := range names {
		if _, _, _, ok, err := rd.GraphCommit(t.Context(), sha); ok || err != nil {
			t.Fatalf("GraphCommit = %v, %v; want no graph and no error", ok, err)
		}
	}
	if got := w.spy.reads.Load() - before; got > int64(len(m.Layers)) {
		t.Errorf("60 lookups of a missing graph made %d bucket reads, want at most %d, one for each layer", got, len(m.Layers))
	}
}

func TestAGraphFileLongerThanItsManifestLengthIsReadOnlyToThatLength(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	names, _, _ := w.pushHistory(6)
	row, err := w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil || len(m.Layers) != 1 || m.Layers[0].Graph == "" {
		t.Fatalf("manifest = %+v, %v; want one layer with a graph", m.Layers, err)
	}
	e := m.Layers[0]
	key := "repos/native/" + b64("repo_x") + "/" + e.Graph
	v, err := w.mem.Version(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	old, err := w.mem.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.mem.Delete(t.Context(), key, v); err != nil {
		t.Fatal(err)
	}
	if _, err := w.mem.Create(t.Context(), key, append(old, make([]byte, 19*e.GraphSize)...)); err != nil {
		t.Fatal(err)
	}
	svc := w.cold()
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, err := rd.GraphCommit(t.Context(), names[0]); ok || err != nil {
		t.Fatalf("GraphCommit = %v, %v; want no graph and no error", ok, err)
	}
	if got := svc.CacheStats().IndexBytes; got > e.GraphSize+1 {
		t.Errorf("a graph file of 20 times its length cost %d bytes of reads, want at most %d", got, e.GraphSize+1)
	}
}

func TestTheGraphReadsACommitAsTheWalkOfTheObjectsDoes(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	tree := gitpack.Hash(gitpack.Tree, nil)
	tab := []byte("tree " + tree.String() + "\ncommitter a <a@b> 100\t+0000\n\nm\n")
	zero := []byte("tree " + gitpack.SHA{}.String() + "\ncommitter a <a@b> 1 +0000\n\nm\n")
	pack, idx, _ := gitpack.Build([]gitpack.Object{{Type: gitpack.Tree}, {Type: gitpack.Commit, Data: tab}, {Type: gitpack.Commit, Data: zero}})
	if err := w.svc.ImportPack(t.Context(), repo, fmt.Sprintf("%x", sha256sum(pack)), idx, pack); err != nil {
		t.Fatal(err)
	}
	rd, err := w.cold().Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, when, ok, err := rd.GraphCommit(t.Context(), gitpack.Hash(gitpack.Commit, tab)); !ok || err != nil || when != 100 {
		t.Errorf("GraphCommit of a commit time after a tab = %d, %v, %v; want time 100", when, ok, err)
	}
	if _, _, _, ok, err := rd.GraphCommit(t.Context(), gitpack.Hash(gitpack.Commit, zero)); ok || err != nil {
		t.Errorf("GraphCommit of a commit with an all-zero tree = %v, %v; want a miss, as the objects walk refuses it", ok, err)
	}
}

func TestAPushStoresNoGraphAndItsCommitsAreReadFromObjects(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	tree := gitpack.Hash(gitpack.Tree, nil)
	commit := gitpack.Object{Type: gitpack.Commit, Data: []byte("tree " + tree.String() + "\ncommitter a <a@b> 1 +0000\n\nm\n")}
	if _, err := w.svc.PutObjects(t.Context(), repo, []gitpack.Object{{Type: gitpack.Tree}, commit}); err != nil {
		t.Fatal(err)
	}
	row, err := w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil || len(m.Layers) != 1 || m.Layers[0].Graph != "" || m.Layers[0].Commits != 0 {
		t.Fatalf("manifest = %+v, %v; want one layer without a graph", m.Layers, err)
	}
	rd, err := w.cold().Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, err := rd.GraphCommit(t.Context(), gitpack.Hash(gitpack.Commit, commit.Data)); ok || err != nil {
		t.Fatalf("GraphCommit = %v, %v; want a miss", ok, err)
	}
}

func TestATailMergeStopsAtTheNewestLayerThatHasAGraph(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.pushHistory(6)
	row, err := w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil || len(m.Layers) != 1 || m.Layers[0].Graph == "" {
		t.Fatalf("manifest = %+v, %v; want the one layer of the import with a graph", m.Layers, err)
	}
	imported := m.Layers[0]
	for range 4 {
		w.push(2)
	}
	row, err = w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	if m, err = midx.DecodeManifest(row.Body); err != nil || m.Layers[0].Key != imported.Key || m.Layers[0].Graph != imported.Graph {
		t.Fatalf("manifest = %+v, %v; want the layer of the import kept as it was", m.Layers, err)
	}
}

func TestACommitGraphAnswersWithoutReadingCommitObjectsAndSurvivesMaintenance(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	names, parents, times := w.pushHistory(60)
	w.checkGraphSizes()
	w.checkGraph(w.cold(), names, parents, times)
	w.pastGrace()
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	w.checkGraph(w.cold(), names, parents, times)
}

func TestACrashDuringMaintenanceLosesNothing(t *testing.T) {
	t.Parallel()
	for _, after := range []bool{false, true} {
		for failAt := int64(1); ; failAt++ {
			w := newWorld(t)
			for range 10 {
				w.push(2)
			}
			w.spy.writes.Store(0)
			w.spy.failAt, w.spy.afterLanding = failAt, after
			err := w.svc.Maintain(t.Context(), repo)
			w.reachable(w.cold())
			w.spy.failAt = 0
			w.pastGrace()
			if err2 := w.svc.Maintain(t.Context(), repo); err2 != nil {
				t.Fatalf("maintenance after a crash at write %d: %v", failAt, err2)
			}
			w.pastGrace()
			if err2 := w.svc.Maintain(t.Context(), repo); err2 != nil {
				t.Fatal(err2)
			}
			w.reachable(w.cold())
			if err == nil {
				break
			}
			if failAt > 40 {
				t.Fatal("maintenance never finished without a crash")
			}
		}
	}
}

func TestAReaderKeepsOneConsistentSetWhileMaintenanceReplacesIt(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for range 10 {
		w.push(2)
	}
	old, err := w.svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	w.clock.Advance(grace / 2).MustWait(t.Context())
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	cold := w.cold()
	for _, sha := range w.have {
		if _, _, err := old.Get(t.Context(), sha); err != nil {
			t.Fatalf("a reader that began before compaction lost %s within the grace: %v", sha, err)
		}
	}
	w.push(2)
	w.reachable(cold)
	w.pastGrace()
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	w.reachable(w.cold())
}

func TestAMissingObjectCostsAtMostOneProbePerLayer(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for range 13 {
		w.push(4)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	layers := w.layers()
	cold := w.cold()
	ctx, stats := store.WithStats(t.Context())
	var missing gitpack.SHA
	missing[0], missing[19] = 0x7f, 1
	before := w.spy.reads.Load()
	got, err := cold.Has(ctx, repo, []gitpack.SHA{missing})
	if err != nil || got[0] {
		t.Fatalf("has = %v, %v; want false", got, err)
	}
	if n := stats.Layers.Load(); n != int64(layers) {
		t.Errorf("probed %d layers, want one for each of the %d layers", n, layers)
	}
	if reads := w.spy.reads.Load() - before; reads > 2*int64(layers) {
		t.Errorf("a missing object cost %d bucket reads over %d layers, want at most one directory and one block for each", reads, layers)
	}
	before = w.spy.reads.Load()
	if _, err := cold.Has(t.Context(), repo, []gitpack.SHA{missing}); err != nil {
		t.Fatal(err)
	}
	if reads := w.spy.reads.Load() - before; reads != 0 {
		t.Errorf("a repeat of the lookup read the bucket %d times, want 0 from the layer cache", reads)
	}
}

func TestManyProbesOfOneRequestTakeTheLayerCacheOncePerLayer(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for range 13 {
		w.push(4)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	layers := int64(w.layers())
	cold := w.cold()
	missing := make([]gitpack.SHA, 500)
	for i := range missing {
		missing[i][0], missing[i][19], missing[i][18] = 0x7f, byte(i), byte(i>>8)
	}
	before := cold.CacheStats()
	if _, err := cold.Has(t.Context(), repo, missing); err != nil {
		t.Fatal(err)
	}
	after := cold.CacheStats()
	if n := after.IndexHits + after.IndexMisses - before.IndexHits - before.IndexMisses; n > 4*layers {
		t.Errorf("500 probes of %d layers took the layer cache %d times, want once for each layer", layers, n)
	}
}

// retiredPack returns the objects of a pack that a push stored and a compaction retired, and its file name.
func (w *world) retiredPack() ([]gitpack.Object, string) {
	w.t.Helper()
	retired, err := w.meta.RetiredFiles(w.t.Context(), repo.String())
	if err != nil {
		w.t.Fatal(err)
	}
	for _, r := range retired {
		for _, sent := range w.sent {
			pack, _, _ := gitpack.Build(sent)
			if id := fmt.Sprintf("%x", sha256sum(pack)); strings.HasPrefix(r.Key, id) && strings.HasSuffix(r.Key, ".pack") {
				return sent, r.Key
			}
		}
	}
	w.t.Fatal("compaction retired no pack that a push stored")
	return nil, ""
}

func TestACollectorThatStallsPastTheLeaseLeavesAFileStoredAgainReadable(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.spy.unconditional = true
	for range 10 {
		w.push(2)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	objs, victim := w.retiredPack()
	path := "repos/native/" + b64("repo_x") + "/" + victim
	stalled, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	w.spy.onDelete = func(key string) {
		if key == path && calls.Add(1) == 1 {
			close(stalled)
			<-resume
		}
	}
	w.pastGrace()
	slow := make(chan error, 1)
	go func() { slow <- w.svc.Maintain(t.Context(), repo) }()
	<-stalled
	w.clock.Advance(11 * time.Minute).MustWait(t.Context())
	other := w.cold()
	if err := other.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	if _, err := other.PutObjects(t.Context(), repo, objs); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}
	if _, err := w.mem.Get(t.Context(), path); err == nil {
		t.Errorf("the collector that resumed left the old key %s", victim)
	}
	row, err := w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range m.Layers {
		if l.IsIndex() && l.Packs[0].ID == victim[:64] {
			found = true
			if _, err := w.mem.Get(t.Context(), "repos/native/"+b64("repo_x")+"/"+l.Packs[0].FileKey()); err != nil {
				t.Errorf("the pack stored again is gone: %v", err)
			}
		}
	}
	if !found {
		t.Error("the manifest lacks the pack stored again")
	}
	w.reachable(w.cold())
}

func TestMaintenanceDeletesRetiredFilesOnABucketThatDeletesUnconditionally(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.spy.unconditional = true
	for range 10 {
		w.push(2)
	}
	w.pastGrace()
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	w.pastGrace()
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	if st := w.svc.MaintStats(); st.MergeRuns+st.CompactRuns == 0 || st.Deleted == 0 {
		t.Errorf("stats = %+v, want work done and retired files deleted", st)
	}
	w.reachable(w.cold())
}

func TestAWriteThatHoldsTheWholeMemoryBudgetStillFinishes(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	release, err := w.svc.Reserve(t.Context(), store.MemoryBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for i := range 6 {
		if _, err := w.svc.PutObjects(ctx, repo, []gitpack.Object{{Type: gitpack.Blob, Data: []byte{byte(i)}}}); err != nil {
			t.Fatalf("push %d while the budget is held: %v", i, err)
		}
	}
	if n := w.layers(); n > 3 {
		t.Fatalf("%d layers after 6 pushes while the budget is held; want the tail merged", n)
	}
}

func TestAPackRowLostAfterCompactionIsRestoredFromTheManifest(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for range 10 {
		w.push(2)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	w.pastGrace()
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	rows, _ := w.meta.Packs(t.Context(), repo.String())
	for _, r := range rows {
		if err := w.meta.RemovePack(t.Context(), repo.String(), r.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	if got, _ := w.meta.Packs(t.Context(), repo.String()); len(got) != len(rows) {
		t.Errorf("%d pack rows after maintenance, want the %d that the manifest holds", len(got), len(rows))
	}
	w.reachable(w.cold())
}

func (w *world) listOlderPack(text string) {
	w.t.Helper()
	pack, idx, names := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: []byte(text)}})
	id := fmt.Sprintf("%x", sha256sum(pack))
	prefix := "repos/native/" + b64("repo_x")
	for k, v := range map[string][]byte{prefix + "/" + id + ".idx": idx, prefix + "/" + id + ".pack": pack} {
		if _, err := w.mem.Create(w.t.Context(), k, v); err != nil {
			w.t.Fatal(err)
		}
	}
	if err := w.meta.AddPack(w.t.Context(), repo.String(), id, int64(len(pack))); err != nil {
		w.t.Fatal(err)
	}
	w.have = append(w.have, names...)
}

func TestAPackThatAnOlderReleaseListedIsAddedToTheManifest(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.push(3)
	w.listOlderPack("from an older release")
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	w.reachable(w.cold())
}

func TestAPackRowWhoseFilesAreMissingDoesNotFailMaintenance(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.push(2)
	if err := w.meta.AddPack(t.Context(), repo.String(), strings.Repeat("ab", 32), 100); err != nil {
		t.Fatal(err)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	rows, _ := w.meta.Packs(t.Context(), repo.String())
	for _, p := range rows {
		if p.ID == strings.Repeat("ab", 32) {
			t.Fatal("the row without files is still listed")
		}
	}
	w.reachable(w.cold())
}

func TestOnePassOfMaintenanceClearsABacklogOfLayers(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for i := range 700 {
		w.listOlderPack(fmt.Sprintf("a pack of the burst %d", i))
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	if n := w.layers(); n > 12 {
		t.Fatalf("%d layers after one pass over a backlog of 700; want at most 12", n)
	}
	w.reachable(w.cold())
}

// fillRandom fills data with bytes that do not compress, eight for each call of rng.
func fillRandom(rng *rand.Rand, data []byte) {
	i := 0
	for ; i+8 <= len(data); i += 8 {
		binary.LittleEndian.PutUint64(data[i:], rng.Uint64())
	}
	if i < len(data) {
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], rng.Uint64())
		copy(data[i:], w[:])
	}
}

func sha256sum(b []byte) [32]byte { return sha256.Sum256(b) }

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

type memBucket struct {
	mu   sync.Mutex
	m    map[string][]byte
	vers map[string]int
	n    int
}

func (b *memBucket) Create(_ context.Context, key string, data []byte) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.m[key]; ok {
		return false, nil
	}
	b.m[key] = append([]byte(nil), data...)
	if b.vers == nil {
		b.vers = map[string]int{}
	}
	b.n++
	b.vers[key] = b.n
	return true, nil
}

func (b *memBucket) Version(_ context.Context, key string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.m[key]; !ok {
		return "", bucket.ErrNotFound
	}
	return strconv.Itoa(b.vers[key]), nil
}

func (b *memBucket) Delete(_ context.Context, key, version string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.m[key]; ok && strconv.Itoa(b.vers[key]) != version {
		return bucket.ErrChanged
	}
	delete(b.m, key)
	return nil
}

func (b *memBucket) Get(ctx context.Context, key string) ([]byte, error) {
	return b.GetRange(ctx, key, 0, -1)
}

func (b *memBucket) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.m[key]
	if !ok {
		return nil, bucket.ErrNotFound
	}
	if off >= int64(len(d)) {
		return nil, nil
	}
	if n < 0 || off+n > int64(len(d)) {
		n = int64(len(d)) - off
	}
	return d[off : off+n], nil
}

func TestAnotherRequestCannotPushOutWhatAPrefetchKept(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(5, 6))
	objs := make([]gitpack.Object, 500)
	for i := range objs {
		data := make([]byte, 20<<10)
		fillRandom(rng, data)
		objs[i] = gitpack.Object{Type: gitpack.Blob, Data: data}
	}
	names, err := w.svc.PutObjects(t.Context(), repo, objs)
	if err != nil {
		t.Fatal(err)
	}
	svc := w.cold()
	mine, other := names[:20], names[100:]
	a, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	a.Prefetch(t.Context(), mine)
	b, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	for _, sha := range other {
		if _, _, err := b.Get(t.Context(), sha); err != nil {
			t.Fatal(err)
		}
	}
	before := svc.CacheStats().RangeReads
	for _, sha := range mine {
		if _, _, err := a.Get(t.Context(), sha); err != nil {
			t.Fatal(err)
		}
	}
	if got := svc.CacheStats().RangeReads - before; got != 0 {
		t.Errorf("reading %d prefetched objects after another request read 8 MiB made %d range reads; it must make none", len(mine), got)
	}
}

func TestAWrittenCommitAndTreeNeedNoRangeRead(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	tree := append([]byte("100644 a\x00"), make([]byte, 20)...)
	names, err := w.svc.PutObjects(t.Context(), repo, []gitpack.Object{{Type: gitpack.Commit, Data: []byte("tree x\n\nm\n")}, {Type: gitpack.Tree, Data: tree}})
	if err != nil {
		t.Fatal(err)
	}
	before := w.svc.CacheStats().RangeReads
	for _, name := range names {
		if _, _, err := w.svc.Get(t.Context(), repo, name); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.svc.CacheStats().RangeReads - before; got != 0 {
		t.Errorf("reading the commit and tree that a put just wrote made %d range reads; it must make none", got)
	}
}

func TestAReaderDoesNotSeeACommitWrittenAfterItsView(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	if _, err := w.svc.PutObjects(t.Context(), repo, []gitpack.Object{{Type: gitpack.Blob, Data: []byte("first")}}); err != nil {
		t.Fatal(err)
	}
	rd, err := w.svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	names, err := w.svc.PutObjects(t.Context(), repo, []gitpack.Object{{Type: gitpack.Commit, Data: []byte("tree x\n\nm\n")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rd.Get(t.Context(), names[0]); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a reader made before a put reads the commit of the put: err = %v, want ErrNotFound", err)
	}
}

func TestATreeReadFetchesOnlyTheHeadersOfItsBlobs(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(7, 8))
	var objs []gitpack.Object
	var tree []byte
	for i := range 100 {
		data := make([]byte, 140<<10)
		fillRandom(rng, data)
		sha := gitpack.Hash(gitpack.Blob, data)
		objs = append(objs, gitpack.Object{Type: gitpack.Blob, Data: data})
		tree = append(tree, fmt.Sprintf("100644 f%03d\x00", i)...)
		tree = append(tree, sha[:]...)
	}
	objs = append(objs, gitpack.Object{Type: gitpack.Tree, Data: tree})
	names, err := w.svc.PutObjects(t.Context(), repo, objs)
	if err != nil {
		t.Fatal(err)
	}
	svc := w.cold()
	if _, err := svc.TreeBulk(t.Context(), repo, names[len(names)-1], 1000, func(int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := svc.CacheStats().RangeBytes; got > 2<<20 {
		t.Errorf("a tree of 100 blobs of 140 KiB cost %d bytes of range reads; it needs the headers only", got)
	}
}

func (s *spy) Caps() bucket.Caps {
	return bucket.Caps{ConditionalDelete: !s.unconditional && bucket.DeletesConditionally(s.Store)}
}

func (s *slowBucket) Unwrap() bucket.Store { return s.Store }

func (b *memBucket) Caps() bucket.Caps { return bucket.Caps{ConditionalDelete: true} }

func TestAPackIsReadFromTheKeyThatTheManifestRecords(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	w.push(3)
	row, err := w.meta.Manifest(t.Context(), repo.String())
	if err != nil {
		t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "repos/native/" + b64("repo_x") + "/"
	p := &m.Layers[0].Packs[0]
	data, err := w.mem.Get(t.Context(), prefix+p.FileKey())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.mem.Create(t.Context(), prefix+"moved.pack", data); err != nil {
		t.Fatal(err)
	}
	ver, _ := w.mem.Version(t.Context(), prefix+p.FileKey())
	if err := w.mem.Delete(t.Context(), prefix+p.FileKey(), ver); err != nil {
		t.Fatal(err)
	}
	p.Key = "moved.pack"
	body, _ := m.Encode()
	if _, err := w.meta.SwapManifest(t.Context(), repo.String(), row.Generation, body); err != nil {
		t.Fatal(err)
	}
	got, err := w.cold().Has(t.Context(), repo, w.have)
	if err != nil || !slices.Equal(got, []bool{true, true, true}) {
		t.Fatalf("has = %v, %v; want every object read from the recorded key", got, err)
	}
	for _, sha := range w.have {
		if _, _, err := w.cold().Get(t.Context(), repo, sha); err != nil {
			t.Errorf("get %s: %v", sha, err)
		}
	}
}
