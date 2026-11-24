package store_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/gitproto"
	"github.com/majorcontext/motte/internal/pktline"
	"github.com/majorcontext/motte/internal/store"
)

type plainObjects struct{ *store.Reader }

type copyObjects struct{ plainObjects }

func (o copyObjects) Copy(ctx context.Context, shas []gitpack.SHA, fn func(gitpack.SHA, *gitpack.Raw) error) error {
	return o.Reader.Copy(ctx, shas, store.ReadAheadBytes, nil, fn)
}

type phaseWriter struct {
	bytes.Buffer
	reads  func() int64
	atPack int64
}

func (p *phaseWriter) Write(b []byte) (int, error) {
	if p.atPack == 0 && bytes.Contains(b, []byte("packfile")) {
		p.atPack = p.reads()
	}
	return p.Buffer.Write(b)
}

// copyFixture stores 2000 trees of 2 KiB in one pack in an order unrelated to the walk, and returns the commit that
// reaches them and the objects of its clone.
func copyFixture(t *testing.T, w *world) (gitpack.SHA, map[gitpack.SHA]gitpack.Object) {
	t.Helper()
	rng := rand.New(rand.NewPCG(5, 6))
	var trees []gitpack.Object
	for range 2000 {
		var b []byte
		for i := range 60 {
			var sha gitpack.SHA
			for j := range sha {
				sha[j] = byte(rng.Uint32())
			}
			b = append(append(b, fmt.Sprintf("100644 f%02d\x00", i)...), sha[:]...)
		}
		trees = append(trees, gitpack.Object{Type: gitpack.Tree, Data: b})
	}
	names, err := w.svc.PutObjects(t.Context(), repo, trees)
	if err != nil {
		t.Fatal(err)
	}
	mixed := importMixed(w, 1)
	tops := append(slices.Clone(names), gitpack.Hash(gitpack.Tree, mixed[2].data), gitpack.Hash(gitpack.Tree, mixed[4].data))
	var root []byte
	for i, j := range rng.Perm(len(tops)) {
		root = append(append(root, fmt.Sprintf("40000 d%04d\x00", i)...), tops[j][:]...)
	}
	rootSHA := gitpack.Hash(gitpack.Tree, root)
	commit := fmt.Appendf(nil, "tree %s\nauthor a <a> 1 +0000\ncommitter a <a> 1 +0000\n\nm\n", rootSHA)
	want := map[gitpack.SHA]gitpack.Object{}
	for _, o := range append(trees, gitpack.Object{Type: gitpack.Tree, Data: root}, gitpack.Object{Type: gitpack.Commit, Data: commit},
		gitpack.Object{Type: gitpack.Tree, Data: mixed[2].data}, gitpack.Object{Type: gitpack.Tree, Data: mixed[4].data}) {
		want[gitpack.Hash(o.Type, o.Data)] = o
	}
	if _, err := w.svc.PutObjects(t.Context(), repo, []gitpack.Object{{Type: gitpack.Tree, Data: root}, {Type: gitpack.Commit, Data: commit}}); err != nil {
		t.Fatal(err)
	}

	return gitpack.Hash(gitpack.Commit, commit), want
}

func TestAPackWriteReadsTheTreesInFewLargeRuns(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	tip, want := copyFixture(t, w)
	svc := store.New(w.mem, w.meta, store.WithClock(w.clock), store.WithGrace(grace), store.WithRangeCache(1<<20))
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	objs := copyObjects{plainObjects{rd}}
	var req bytes.Buffer
	pw := pktline.NewWriter(&req)
	pw.Linef("command=fetch\n")
	pw.Delim()
	pw.Linef("want %s\n", tip)
	pw.Linef("filter blob:none\n")
	pw.Linef("done\n")
	pw.Flush()
	resp := &phaseWriter{reads: func() int64 { return svc.CacheStats().RangeReads }}
	if err := (&gitproto.Upload{Objects: objs}).Serve(t.Context(), resp, &req); err != nil {
		t.Fatal(err)
	}
	writeReads := svc.CacheStats().RangeReads - resp.atPack

	var pack []byte
	pr := pktline.NewReader(&resp.Buffer)
	for {
		line, kind, err := pr.Next()
		if err != nil || kind == pktline.Flush {
			break
		}
		if kind == pktline.Data && line[0] == 1 {
			pack = append(pack, line[1:]...)
		}
	}
	got, err := gitpack.Unpack(t.Context(), pack, nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[gitpack.SHA]bool{}
	for _, g := range got {
		sha := gitpack.Hash(g.Type, g.Data)
		if o, ok := want[sha]; !ok || seen[sha] || o.Type != g.Type || !bytes.Equal(o.Data, g.Data) {
			t.Fatalf("pack object %s is not an object of the repository, or repeats one", sha)
		}
		seen[sha] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("pack has %d objects, want %d", len(seen), len(want))
	}
	t.Logf("%d range reads while the pack is written", writeReads)
	if writeReads > 8 {
		t.Errorf("the pack write made %d range reads for 4 MiB of trees, want at most 8: it must read each pack in large runs", writeReads)
	}
}

type gatedStore struct {
	bucket.Store
	armed   atomic.Bool
	now     atomic.Int64
	peak    atomic.Int64
	arrived chan struct{}
	release chan struct{}
}

func (g *gatedStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if g.armed.Load() && strings.HasSuffix(key, ".pack") {
		defer g.now.Add(-1)
		p := g.now.Add(1)
		for q := g.peak.Load(); p > q && !g.peak.CompareAndSwap(q, p); q = g.peak.Load() {
		}
		g.arrived <- struct{}{}
		<-g.release
	}
	return g.Store.GetRange(ctx, key, off, n)
}

func TestACopyReadsTheRunsItIsAllowedAhead(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	_, want := copyFixture(t, w)
	var shas []gitpack.SHA
	for sha := range want {
		shas = append(shas, sha)
	}
	g := &gatedStore{Store: w.mem, arrived: make(chan struct{}, 64), release: make(chan struct{})}
	svc := store.New(g, w.meta, store.WithClock(w.clock), store.WithGrace(grace), store.WithRangeCache(1<<20))
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	release := sync.OnceFunc(func() { close(g.release) })
	defer release()
	done := make(chan error, 1)
	go func() {
		done <- rd.Copy(t.Context(), shas, 4<<20, nil, func(gitpack.SHA, *gitpack.Raw) error { return nil })
	}()
	for range 3 {
		select {
		case <-g.arrived:
		case err := <-done:
			t.Fatalf("Copy ended with %v before it had 3 runs in flight", err)
		case <-time.After(5 * time.Second): //lint:allow wallclock a read held at the gate is outside any bubble
			t.Fatal("Copy did not have 3 runs in flight")
		}
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g.peak.Load() < 3 {
		t.Errorf("at most %d runs were in flight, want 3 or more", g.peak.Load())
	}
}
