package gitproto

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
)

type fakeRepo struct {
	objs     map[gitpack.SHA]gitpack.Object
	graph    map[gitpack.SHA]fakeNode
	commitGs int
}

type fakeNode struct {
	tree    gitpack.SHA
	parents []gitpack.SHA
	when    int64
}

func (f *fakeRepo) Get(_ context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	o, ok := f.objs[sha]
	if !ok {
		return 0, nil, ErrNotFound
	}
	if o.Type == gitpack.Commit {
		f.commitGs++
	}
	return o.Type, o.Data, nil
}

func (f *fakeRepo) Has(_ context.Context, shas []gitpack.SHA) ([]bool, error) {
	out := make([]bool, len(shas))
	for i, s := range shas {
		_, out[i] = f.objs[s]
	}
	return out, nil
}

type graphRepo struct{ *fakeRepo }

func (g graphRepo) CommitInfo(_ context.Context, sha gitpack.SHA) (gitpack.SHA, []gitpack.SHA, int64, bool, error) {
	n, ok := g.graph[sha]
	return n.tree, n.parents, n.when, ok, nil
}

func randomHistory(rng *rand.Rand, n int) (*fakeRepo, []gitpack.SHA) {
	f := &fakeRepo{objs: map[gitpack.SHA]gitpack.Object{}, graph: map[gitpack.SHA]fakeNode{}}
	var commits []gitpack.SHA
	for i := range n {
		blob := gitpack.Object{Type: gitpack.Blob, Data: fmt.Appendf(nil, "blob %d", rng.IntN(n/2+1))}
		bsha := gitpack.Hash(gitpack.Blob, blob.Data)
		tree := gitpack.Object{Type: gitpack.Tree, Data: append([]byte("100644 f\x00"), bsha[:]...)}
		tsha := gitpack.Hash(gitpack.Tree, tree.Data)
		var ps []gitpack.SHA
		for range min(i, rng.IntN(4)) {
			if p := commits[rng.IntN(len(commits))]; !slices.Contains(ps, p) {
				ps = append(ps, p)
			}
		}
		var b strings.Builder
		fmt.Fprintf(&b, "tree %s\n", tsha)
		for _, p := range ps {
			fmt.Fprintf(&b, "parent %s\n", p)
		}
		when := int64(1700000000 + rng.IntN(i+1)*60)
		fmt.Fprintf(&b, "author a <a@b> %d +0000\ncommitter a <a@b> %d +0000\n\nc%d\n", when, when, i)
		c := gitpack.Object{Type: gitpack.Commit, Data: []byte(b.String())}
		csha := gitpack.Hash(gitpack.Commit, c.Data)
		f.objs[bsha], f.objs[tsha], f.objs[csha] = blob, tree, c
		f.graph[csha] = fakeNode{tsha, ps, when}
		commits = append(commits, csha)
	}
	return f, commits
}

func TestAWalkGivesTheSameObjectsWithNoGraphAFullGraphAndAHalfGraph(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewPCG(seed, 99))
		f, commits := randomHistory(rng, 80)
		wants := []gitpack.SHA{commits[len(commits)-1], commits[rng.IntN(len(commits))]}
		haves := []gitpack.SHA{commits[rng.IntN(len(commits)/2)]}
		run := func(o Objects) []gitpack.SHA {
			c := &collector{ctx: t.Context(), objs: o}
			out, err := c.collect(wants, haves)
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			slices.SortFunc(out, func(a, b gitpack.SHA) int { return strings.Compare(a.String(), b.String()) })
			return out
		}
		plain := run(f)
		f.commitGs = 0
		full := run(graphRepo{f})
		if reads := f.commitGs; reads != 0 {
			t.Fatalf("seed %d: a walk over graph-covered history read %d commit objects, want 0", seed, reads)
		}
		half := &fakeRepo{objs: f.objs, graph: map[gitpack.SHA]fakeNode{}}
		for sha, n := range f.graph {
			if rng.IntN(2) == 0 {
				half.graph[sha] = n
			}
		}
		if !slices.Equal(plain, full) || !slices.Equal(plain, run(graphRepo{half})) {
			t.Fatalf("seed %d: the walk with a graph sent other objects than the walk of the objects", seed)
		}
	}
}
