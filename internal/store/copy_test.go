package store_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/gitproto"
	"github.com/majorcontext/motte/internal/pktline"
	"github.com/majorcontext/motte/internal/store"
)

type plainObjects struct{ *store.Reader }

type copyObjects struct{ plainObjects }

func (o copyObjects) Copy(ctx context.Context, shas []gitpack.SHA, fn func(gitpack.SHA, *gitpack.Raw) error) error {
	return o.Reader.Copy(ctx, shas, nil, fn)
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

func TestAPackWriteReadsTheTreesInFewLargeRuns(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
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
	pw.Linef("want %s\n", gitpack.Hash(gitpack.Commit, commit))
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
