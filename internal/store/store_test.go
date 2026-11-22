package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/quartz"

	"github.com/majorcontext/motte/bucket"
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
	m, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"))
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
	w.legacyReadable()
	for _, sha := range []gitpack.SHA{w.have[0], w.have[len(w.have)/2], w.have[len(w.have)-1]} {
		if _, _, err := svc.Get(ctx, repo, sha); err != nil {
			w.t.Fatalf("read %s: %v", sha, err)
		}
	}
}

// legacyReadable reads every object the way the release before the layered index does: from the pack rows, each pack
// with its own .idx file.
func (w *world) legacyReadable() {
	w.t.Helper()
	ctx := w.t.Context()
	rows, err := w.meta.Packs(ctx, repo.String())
	if err != nil {
		w.t.Fatal(err)
	}
	prefix := "repos/native/" + b64("repo_x") + "/"
	var packs []*gitpack.Pack
	for _, r := range rows {
		idx, ierr := w.mem.Get(ctx, prefix+r.ID+".idx")
		data, derr := w.mem.Get(ctx, prefix+r.ID+".pack")
		if ierr != nil || derr != nil {
			w.t.Fatalf("pack %s of the pack list lacks a file: idx %v, pack %v", r.ID, ierr, derr)
		}
		p, err := gitpack.NewPack(idx, bytesAt(data))
		if err != nil {
			w.t.Fatalf("pack %s: %v", r.ID, err)
		}
		packs = append(packs, p)
	}
	for _, sha := range w.have {
		found := false
		for _, p := range packs {
			if p.Has(sha) {
				if _, _, err := p.Get(sha); err != nil {
					w.t.Fatalf("legacy read of %s: %v", sha, err)
				}
				found = true
				break
			}
		}
		if !found {
			w.t.Fatalf("object %s is not readable by the previous release", sha)
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
		for j := range data {
			data[j] = byte(rng.Uint32())
		}
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
		for j := range data {
			data[j] = byte(rng.Uint32())
		}
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
			if id := fmt.Sprintf("%x", sha256sum(pack)); r.Key == id+".pack" {
				return sent, r.Key
			}
		}
	}
	w.t.Fatal("compaction retired no pack that a push stored")
	return nil, ""
}

func TestACollectorThatStallsPastTheLeaseKeepsAFileStoredAgain(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
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
	if _, err := w.mem.Get(t.Context(), path); err != nil {
		t.Errorf("the collector that resumed deleted the file stored again: %v", err)
	}
	w.reachable(w.cold())
}

func TestAPackStoredAgainWhileItIsBeingDeletedStaysReadable(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for range 10 {
		w.push(2)
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	objs, victim := w.retiredPack()
	done := make(chan struct{})
	var once sync.Once
	var parked *quartz.Call
	trap := w.clock.Trap().NewTimer()
	w.spy.onDelete = func(key string) {
		if key == "repos/native/"+b64("repo_x")+"/"+victim {
			once.Do(func() {
				go func() {
					defer close(done)
					if _, err := w.svc.PutObjects(t.Context(), repo, objs); err != nil {
						t.Error(err)
					}
				}()
				parked = trap.MustWait(t.Context())
			})
		}
	}
	w.pastGrace()
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	parked.MustRelease(t.Context())
	trap.Close()
	for waiting := true; waiting; {
		select {
		case <-done:
			waiting = false
		default:
			w.clock.Advance(100 * time.Millisecond).MustWait(t.Context())
			runtime.Gosched()
		}
	}
	w.reachable(w.cold())
	if _, err := w.mem.Get(t.Context(), "repos/native/"+b64("repo_x")+"/"+victim); err != nil {
		t.Errorf("the pack stored again is gone: %v", err)
	}
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

func TestOnePassOfMaintenanceClearsABacklogOfLayers(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for i := range 1100 {
		w.listOlderPack(fmt.Sprintf("a pack of the burst %d", i))
	}
	if err := w.svc.Maintain(t.Context(), repo); err != nil {
		t.Fatal(err)
	}
	if n := w.layers(); n > 12 {
		t.Fatalf("%d layers after one pass over a backlog of 1100; want at most 12", n)
	}
	w.reachable(w.cold())
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
		for j := range data {
			data[j] = byte(rng.Uint32())
		}
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
