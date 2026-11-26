package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
)

func TestGenerationsAreOneMoreThanTheLargestParentAndZeroBelowAnUnknownParent(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(21, 22))
	known := gitpack.SHA{1}
	unknown := gitpack.SHA{2}
	var commits []midx.Commit
	for i := range 300 {
		c := midx.Commit{SHA: gitpack.SHA{byte(i), byte(i >> 8), 9}}
		for range min(i, rng.IntN(4)) {
			c.Parents = append(c.Parents, commits[rng.IntN(len(commits))].SHA)
		}
		switch {
		case i == 0:
			c.Parents = []midx.SHA{known}
		case i == 40:
			c.Parents = append(c.Parents, unknown)
		}
		commits = append(commits, c)
	}
	commits = append(commits, commits[3])
	rng.Shuffle(len(commits), func(i, j int) { commits[i], commits[j] = commits[j], commits[i] })
	setGenerations(commits, func(sha gitpack.SHA) uint32 {
		if sha == known {
			return 5
		}
		return 0
	})
	gen := map[gitpack.SHA]uint32{known: 5}
	for _, c := range commits {
		gen[c.SHA] = c.Gen
	}
	for _, c := range commits {
		want := uint32(1)
		for _, p := range c.Parents {
			if gen[p] == 0 {
				want = 0
				break
			}
			want = max(want, gen[p]+1)
		}
		if c.Gen != want {
			t.Fatalf("commit %x has generation %d, want %d", c.SHA, c.Gen, want)
		}
	}
}

func TestACommitOrAPackBeyondTheGraphLimitsIsLeftOut(t *testing.T) {
	t.Parallel()
	text := "tree " + gitpack.SHA{1}.String() + "\n"
	for i := 0; i <= midx.MaxGraphParents; i++ {
		text += "parent " + gitpack.SHA{byte(i), 2}.String() + "\n"
	}
	if _, ok := commitRecord(gitpack.SHA{9}, []byte(text+"committer a <a@b> 1 +0000\n\nx\n")); ok {
		t.Error("a commit with more parents than the graph holds was recorded")
	}
	svc := New(nil, nil)
	svc.graphCap = 100
	objs := make([]gitpack.Object, svc.graphCap+1)
	for i := range objs {
		objs[i] = gitpack.Object{Type: gitpack.Commit, Data: fmt.Appendf(nil, "tree %s\ncommitter a <a@b> %d +0000\n\n", gitpack.SHA{1}, i)}
	}
	pack, idx, _ := gitpack.Build(objs)
	p, err := gitpack.NewPack(idx, bytesReader(pack))
	if err != nil {
		t.Fatal(err)
	}
	if got := svc.graphOfPack(t.Context(), Repo{}, p); got != nil {
		t.Errorf("a pack of %d commits gave a graph of %d, want none and no lookup of generations", len(objs), len(got))
	}
}

func TestALookupProbesOnlyTheNewestGraphsThatFitTheWorkingSet(t *testing.T) {
	t.Parallel()
	layers := []midx.Entry{{Graph: "old", GraphSize: 30 << 20}, {}, {Graph: "mid", GraphSize: 30 << 20}, {Graph: "new", GraphSize: 30 << 20}}
	if got := graphLayers(layers); len(got) != 2 || got[0] != 3 || got[1] != 2 {
		t.Errorf("graphLayers = %v, want the newest two graph layers [3 2]: three of 30 MiB pass the working set of %d", got, maxGraphWorkingBytes)
	}
}

type graphFiles struct {
	bucket.Store
	files          map[string][]byte
	wait           func()
	inFlight, peak atomic.Int64
}

func (g *graphFiles) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	b := g.files[key]
	b = append(make([]byte, 0, 2*len(b)), b[off:min(off+n, int64(len(b)))]...)
	now := g.inFlight.Add(int64(len(b)))
	for p := g.peak.Load(); now > p && !g.peak.CompareAndSwap(p, now); p = g.peak.Load() {
	}
	defer g.inFlight.Add(-int64(len(b)))
	g.wait()
	return b, nil
}

func TestGraphLoadsOfDifferentFilesHoldAtMostTheReadBudget(t *testing.T) {
	t.Parallel()
	g := &graphFiles{files: map[string][]byte{}}
	var entries []midx.Entry
	for i := range 8 {
		cs := make([]midx.Commit, 40000)
		for j := range cs {
			cs[j] = midx.Commit{SHA: midx.SHA{byte(j), byte(j >> 8), byte(i), 1}}
		}
		file, err := midx.BuildGraph(cs)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("g%d.cg", i)
		g.files["p/"+key] = file
		entries = append(entries, midx.Entry{Graph: key, GraphSize: int64(len(file))})
	}
	s := New(g, nil, WithReadBudget(12<<20))
	synctest.Test(t, func(t *testing.T) {
		g.wait = func() { time.Sleep(20 * time.Millisecond) }
		var wg sync.WaitGroup
		for _, e := range entries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.graph(t.Context(), "p", e); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	})
	if peak, one := g.peak.Load(), int64(len(g.files["p/g0.cg"])); peak > 2*one {
		t.Errorf("graph loads held %d bytes at once for files of %d bytes, want at most the budget", peak, one)
	}
}

func TestAGraphThatNeedsMoreThanTheReadBudgetIsNotLoadedAndAMergeOfTooManyCommitsReadsNoGraph(t *testing.T) {
	t.Parallel()
	g := &graphFiles{files: map[string][]byte{"p/big.cg": make([]byte, 1)}, wait: func() {}}
	s := New(g, nil)
	big := midx.Entry{Graph: "big.cg", GraphSize: 20 << 20}
	if _, err := s.graph(t.Context(), "p", big); !errors.Is(err, ErrNotFound) || g.peak.Load() != 0 {
		t.Errorf("graph of 20 MiB under the budget of %d: err = %v, bytes read %d; want ErrNotFound and no read", s.readBudget, err, g.peak.Load())
	}
	layers := []midx.Entry{{Graph: "a.cg", Commits: midx.MaxGraphCommits, GraphSize: 100}, {Graph: "b.cg", Commits: 1, GraphSize: 100}}
	if n := graphBytes(layers); n != 0 {
		t.Errorf("a merge of too many commits reserves %d bytes for graphs it does not read", n)
	}
	e, err := s.mergedGraph(t.Context(), Repo{}, "p", midx.Entry{}, layers)
	if err != nil || e.Graph != "" || g.peak.Load() != 0 {
		t.Errorf("merge of %d commits = %+v, %v, bytes read %d; want no graph and no read", midx.MaxGraphCommits+1, e, err, g.peak.Load())
	}
}

func TestTheLayerCacheChargesAGraphByTheCapacityOfItsBuffer(t *testing.T) {
	t.Parallel()
	cs := []midx.Commit{{SHA: midx.SHA{1}}, {SHA: midx.SHA{2}}}
	file, err := midx.BuildGraph(cs)
	if err != nil {
		t.Fatal(err)
	}
	g := &graphFiles{files: map[string][]byte{"p/a.cg": file}, wait: func() {}}
	s := New(g, nil)
	if _, err := s.graph(t.Context(), "p", midx.Entry{Graph: "a.cg", GraphSize: int64(len(file))}); err != nil {
		t.Fatal(err)
	}
	if used, _ := s.icache.stats(); used < 2*len(file) {
		t.Errorf("the layer cache charged %d bytes for a graph held in a buffer of %d, want at least that", used, 2*len(file))
	}
}

func (g *graphFiles) Unwrap() bucket.Store { return g.Store }
