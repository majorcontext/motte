package store

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/majorcontext/motte/internal/gitpack"
)

// orderMeta puts the commits first, newest first, then the tags, then the trees in breadth-first order from the
// root trees of the commits, so a walk of a recent tree reads one dense range. Objects that nothing in objs reaches
// keep their order, last.
func orderMeta(objs []gitpack.Object) []gitpack.Object {
	type commit struct {
		obj  gitpack.Object
		when int64
		tree gitpack.SHA
	}
	var commits []commit
	var tags []gitpack.Object
	trees := map[gitpack.SHA]gitpack.Object{}
	var treeOrder []gitpack.SHA
	for _, o := range objs {
		switch o.Type {
		case gitpack.Commit:
			when, tree := parseCommit(o.Data)
			commits = append(commits, commit{o, when, tree})
		case gitpack.Tree:
			name := gitpack.Hash(gitpack.Tree, o.Data)
			if _, dup := trees[name]; !dup {
				treeOrder = append(treeOrder, name)
			}
			trees[name] = o
		default:
			tags = append(tags, o)
		}
	}
	sort.SliceStable(commits, func(i, j int) bool { return commits[i].when > commits[j].when })
	out := make([]gitpack.Object, 0, len(objs))
	for _, c := range commits {
		out = append(out, c.obj)
	}
	out = append(out, tags...)
	var queue []gitpack.SHA
	for _, c := range commits {
		queue = append(queue, c.tree)
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		o, ok := trees[name]
		if !ok {
			continue
		}
		delete(trees, name)
		out = append(out, o)
		queue = append(queue, childTrees(o.Data)...)
	}
	for _, name := range treeOrder {
		if o, ok := trees[name]; ok {
			out = append(out, o)
		}
	}
	return out
}

func parseCommit(data []byte) (when int64, tree gitpack.SHA) {
	head, _, _ := bytes.Cut(data, []byte("\n\n"))
	for _, l := range bytes.Split(head, []byte("\n")) {
		switch {
		case bytes.HasPrefix(l, []byte("tree ")):
			tree, _ = gitpack.ParseSHA(string(l[5:]))
		case bytes.HasPrefix(l, []byte("committer ")):
			f := bytes.Fields(l)
			if len(f) >= 3 {
				when, _ = strconv.ParseInt(string(f[len(f)-2]), 10, 64)
			}
		}
	}
	return when, tree
}

func childTrees(data []byte) []gitpack.SHA {
	var out []gitpack.SHA
	for len(data) > 0 {
		nul := bytes.IndexByte(data, 0)
		if nul < 0 || len(data) < nul+21 {
			return out
		}
		if bytes.HasPrefix(data, []byte("40000 ")) {
			out = append(out, gitpack.SHA(data[nul+1:nul+21]))
		}
		data = data[nul+21:]
	}
	return out
}
