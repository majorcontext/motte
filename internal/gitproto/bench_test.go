package gitproto

import (
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
)

func BenchmarkCommitWalk(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 99))
	f, commits := randomHistory(rng, 3000)
	wants := []gitpack.SHA{commits[len(commits)-1]}
	haves := []gitpack.SHA{commits[len(commits)/2]}
	for name, o := range map[string]Objects{"objects": f, "graph": graphRepo{f}} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			f.commitGs = 0
			b.ResetTimer()
			for range b.N {
				c := &collector{ctx: b.Context(), objs: o}
				if _, err := c.collect(wants, haves); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(f.commitGs)/float64(b.N), "commit-reads/op")
		})
	}
}

// treeRepo is a tree of dirs directories of files blobs each, with the root tree at the returned name.
func treeRepo(dirs, files int) (*fakeRepo, gitpack.SHA) {
	f := &fakeRepo{objs: map[gitpack.SHA]gitpack.Object{}, graph: map[gitpack.SHA]fakeNode{}}
	var root []byte
	for d := range dirs {
		var dir []byte
		for i := range files {
			data := fmt.Appendf(nil, "file %d of dir %d", i, d)
			sha := gitpack.Hash(gitpack.Blob, data)
			f.objs[sha] = gitpack.Object{Type: gitpack.Blob, Data: data}
			dir = append(append(dir, fmt.Sprintf("100644 f%d\x00", i)...), sha[:]...)
		}
		sha := gitpack.Hash(gitpack.Tree, dir)
		f.objs[sha] = gitpack.Object{Type: gitpack.Tree, Data: dir}
		root = append(append(root, fmt.Sprintf("40000 d%d\x00", d)...), sha[:]...)
	}
	sha := gitpack.Hash(gitpack.Tree, root)
	f.objs[sha] = gitpack.Object{Type: gitpack.Tree, Data: root}
	return f, sha
}

func BenchmarkTreeWalk(b *testing.B) {
	f, root := treeRepo(200, 40)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c := &collector{ctx: b.Context(), objs: f, sent: map[gitpack.SHA]struct{}{}, skip: map[gitpack.SHA]struct{}{}}
		if err := c.walkTrees([]gitpack.SHA{root}, c.sent, c.add); err != nil {
			b.Fatal(err)
		}
		if len(c.out) != 200*41+1 {
			b.Fatalf("the walk sent %d objects", len(c.out))
		}
	}
}

func BenchmarkWritePack(b *testing.B) {
	f, _ := treeRepo(200, 40)
	var shas []gitpack.SHA
	var size int64
	for sha, o := range f.objs {
		shas = append(shas, sha)
		size += int64(len(o.Data))
	}
	b.ReportAllocs()
	b.SetBytes(size)
	b.ResetTimer()
	for range b.N {
		if err := writePack(b.Context(), io.Discard, f, shas); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(shas)), "objects/op")
}
