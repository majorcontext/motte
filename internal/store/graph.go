package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"unicode"
	"weak"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
)

// commitRecord reads the tree, the parents and the commit time of a commit object, line by line. Gen is left at 0. A
// commit with more parents than a graph holds is not recorded.
func commitRecord(sha gitpack.SHA, data []byte) (midx.Commit, bool) {
	c := midx.Commit{SHA: sha}
	var haveTree bool
	for len(data) > 0 {
		line, rest, _ := bytes.Cut(data, []byte("\n"))
		data = rest
		switch {
		case len(line) == 0:
			return c, haveTree && c.Tree != (gitpack.SHA{})
		case bytes.HasPrefix(line, []byte("tree ")):
			t, err := gitpack.ParseSHA(string(line[5:]))
			if err != nil {
				return c, false
			}
			c.Tree, haveTree = t, true
		case bytes.HasPrefix(line, []byte("parent ")):
			p, err := gitpack.ParseSHA(string(line[7:]))
			if err != nil || len(c.Parents) == midx.MaxGraphParents {
				return c, false
			}
			c.Parents = append(c.Parents, p)
		case bytes.HasPrefix(line, []byte("committer ")):
			if i := bytes.LastIndexByte(line, '>'); i >= 0 {
				f := bytes.TrimLeftFunc(line[i+1:], unicode.IsSpace)
				if j := bytes.IndexFunc(f, unicode.IsSpace); j >= 0 {
					f = f[:j]
				}
				c.Time, _ = strconv.ParseInt(string(f), 10, 64)
			}
		}
	}
	return c, haveTree && c.Tree != (gitpack.SHA{})
}

// graphOf returns the graph records of the commits of objs, named by names, with generations.
func (s *Service) graphOf(ctx context.Context, repo Repo, objs []gitpack.Object, names []gitpack.SHA) []midx.Commit {
	var commits []midx.Commit
	for i, o := range objs {
		if o.Type != gitpack.Commit {
			continue
		}
		if c, ok := commitRecord(names[i], o.Data); ok {
			commits = append(commits, c)
		}
	}
	return s.withGenerations(ctx, repo, commits)
}

// withGenerations sets the generations of commits. A parent that commits lacks gets its generation from the graphs of
// repo.
func (s *Service) withGenerations(ctx context.Context, repo Repo, commits []midx.Commit) []midx.Commit {
	if len(commits) == 0 {
		return nil
	}
	v, _ := s.view(ctx, repo)
	setGenerations(commits, func(sha gitpack.SHA) uint32 {
		if v == nil {
			return 0
		}
		if c, ok, err := v.graphCommit(ctx, sha); err == nil && ok {
			return c.Gen
		}
		return 0
	})
	return commits
}

// setGenerations sets Gen of each commit to 1 more than the largest Gen of its parents, and to 1 for a root. A parent
// that is not in commits takes its Gen from outside; 0 means that it is not known, and then the Gen of the commit is 0.
func setGenerations(commits []midx.Commit, outside func(gitpack.SHA) uint32) {
	at := make(map[gitpack.SHA]int, len(commits))
	for i, c := range commits {
		at[c.SHA] = i
	}
	done := make(map[gitpack.SHA]bool, len(commits))
	var gen func(sha gitpack.SHA) uint32
	gen = func(sha gitpack.SHA) uint32 {
		i, in := at[sha]
		if !in {
			return outside(sha)
		}
		if done[sha] {
			return commits[i].Gen
		}
		done[sha] = true
		g := uint32(1)
		for _, p := range commits[i].Parents {
			pg := gen(p)
			if pg == 0 {
				g = 0
				break
			}
			g = max(g, pg+1)
		}
		commits[i].Gen = g
		return g
	}
	for _, c := range commits {
		gen(c.SHA)
	}
	for i, c := range commits {
		commits[i].Gen = commits[at[c.SHA]].Gen
	}
}

// graphObject stores the graph file of commits under the prefix of the repo and returns its key and the number of commits.
// The key is name+".cg" when name is not empty, and midx/<SHA-256 of the file>.cg otherwise.
func (s *Service) graphObject(ctx context.Context, repo Repo, prefix, name string, file []byte) (string, error) {
	key := name + ".cg"
	if name == "" {
		sum := sha256.Sum256(file)
		key = "midx/" + hex.EncodeToString(sum[:]) + ".cg"
	}
	if err := s.claim(ctx, repo, key); err != nil {
		return "", err
	}
	if _, err := s.st.Create(ctx, prefix+"/"+key, file); err != nil {
		return "", err
	}
	return key, nil
}

// withGraph stores the graph of commits and returns e with the key and the count of commits of the graph.
func (s *Service) withGraph(ctx context.Context, repo Repo, prefix, name string, e midx.Entry, commits []midx.Commit) (out midx.Entry, err error) {
	if len(commits) == 0 {
		return e, nil
	}
	ctx, end := StartRootSpan(ctx, "vcs.maintain.graph_build", attribute.Int("commits", len(commits)))
	defer func() { end(err, attribute.Int64("graph_bytes", out.GraphSize)) }()
	file, err := midx.BuildGraph(commits)
	if errors.Is(err, midx.ErrGraphTooLarge) {
		return e, nil
	}
	if err != nil {
		return e, err
	}
	g, err := midx.ParseGraph(file)
	if err != nil {
		return e, err
	}
	key, err := s.graphObject(ctx, repo, prefix, name, file)
	if err != nil {
		return e, err
	}
	e.Graph, e.Commits, e.GraphSize = key, g.Len(), int64(len(file))
	return e, nil
}

// graph loads the graph of layer e through the layer cache.
func (s *Service) graph(ctx context.Context, prefix string, e midx.Entry) (*midx.Graph, error) {
	key := prefix + "/" + e.Graph
	ck := "cg|" + key
	if g, ok := s.icache.get(ck); ok {
		s.ihits.Add(1)
		return g.(*midx.Graph), nil
	}
	ch := s.loads.DoChan(ck, func() (any, error) {
		s.imisses.Add(1)
		load, cancel := context.WithTimeout(context.WithoutCancel(ctx), layerLoadTimeout)
		defer cancel()
		if e.GraphSize <= 0 {
			return nil, midx.ErrCorruptGraph
		}
		n := readBudgetFactor * (e.GraphSize + 1)
		if n > s.readBudget {
			return nil, fmt.Errorf("%w: the graph needs more than the read budget", ErrNotFound)
		}
		if err := s.rflight.Acquire(load, n); err != nil {
			return nil, err
		}
		defer s.rflight.Release(n)
		data, err := s.st.GetRange(load, key, 0, e.GraphSize+1)
		if err != nil {
			return nil, mapNotFound(err)
		}
		if int64(len(data)) != e.GraphSize {
			return nil, midx.ErrCorruptGraph
		}
		s.idxBytes.Add(int64(len(data)))
		g, err := midx.ParseGraph(data)
		if err != nil {
			return nil, err
		}
		s.icache.put(ck, g, cap(data))
		return g, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*midx.Graph), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// graphCommit returns the graph record of sha from the newest layer whose graph holds it. A layer with no graph, or a
// graph that cannot be read, does not hold it, so the caller reads the object.
func (v *view) graphCommit(ctx context.Context, sha gitpack.SHA) (midx.Commit, bool, error) {
	for _, i := range graphLayers(v.layers) {
		if _, bad := v.noGraph.Load(v.layers[i].Graph); bad {
			continue
		}
		g, err := v.graphAt(ctx, i)
		if errors.Is(err, ErrNotFound) || errors.Is(err, midx.ErrCorruptGraph) {
			v.noGraph.Store(v.layers[i].Graph, true)
			continue
		}
		if err != nil {
			return midx.Commit{}, false, err
		}
		if c, ok := g.Lookup(sha); ok {
			return c, true, nil
		}
	}
	return midx.Commit{}, false, nil
}

// graphAt returns the graph of layer i of the view. The first probe of a request goes through the layer cache; the next
// ones read a weak pointer, so they take no lock and a graph that the cache evicts is not kept alive outside its budget.
func (v *view) graphAt(ctx context.Context, i int) (*midx.Graph, error) {
	if w := v.graphs[i].Load(); w != nil {
		if g := w.Value(); g != nil {
			return g, nil
		}
	}
	g, err := v.s.graph(ctx, v.prefix, v.layers[i])
	if err != nil {
		return nil, err
	}
	w := weak.Make(g)
	v.graphs[i].Store(&w)
	return g, nil
}

// GraphCommit returns the tree, the parents and the commit time of a commit from the commit graph, with false when no
// graph of the repo holds it.
func (r *Reader) GraphCommit(ctx context.Context, sha gitpack.SHA) (tree gitpack.SHA, parents []gitpack.SHA, when int64, ok bool, err error) {
	c, ok, err := r.v.graphCommit(ctx, sha)
	if err != nil || !ok {
		return gitpack.SHA{}, nil, 0, false, err
	}
	parents = make([]gitpack.SHA, len(c.Parents))
	for i, p := range c.Parents {
		parents[i] = p
	}
	return c.Tree, parents, c.Time, true, nil
}

// mergedGraph returns the entry e with the union of the graphs of the layers, stored as one file, or e as it is when no
// layer has a graph. A graph that cannot be read is left out: its commits are read from the objects.
func (s *Service) mergedGraph(ctx context.Context, repo Repo, prefix string, e midx.Entry, layers []midx.Entry) (out midx.Entry, err error) {
	ctx, end := StartRootSpan(ctx, "vcs.maintain.graph_merge", attribute.Int("layers", len(layers)))
	defer func() { end(err, attribute.Int("commits", out.Commits), attribute.Int64("graph_bytes", out.GraphSize)) }()
	var commits int
	for _, l := range layers {
		commits += l.Commits
	}
	if commits > midx.MaxGraphCommits {
		return e, nil
	}
	var gs []*midx.Graph
	for _, l := range layers {
		if l.Graph == "" {
			continue
		}
		g, err := s.graph(ctx, prefix, l)
		if errors.Is(err, ErrNotFound) || errors.Is(err, midx.ErrCorruptGraph) {
			continue
		}
		if err != nil {
			return e, err
		}
		gs = append(gs, g)
	}
	if len(gs) == 0 {
		return e, nil
	}
	file, err := midx.MergeGraphs(gs)
	if errors.Is(err, midx.ErrGraphTooLarge) {
		return e, nil
	}
	if err != nil {
		return e, err
	}
	g, err := midx.ParseGraph(file)
	if err != nil {
		return e, err
	}
	key, err := s.graphObject(ctx, repo, prefix, "", file)
	if err != nil {
		return e, err
	}
	e.Graph, e.Commits, e.GraphSize = key, g.Len(), int64(len(file))
	return e, nil
}

// importGraphWork bounds the inflated bytes of the commits that an import reads for its graph. A pack over it gets no
// graph, and a compaction adds one.
const importGraphWork = 64 << 20

// graphOfPack returns the graph records of the commits of a pack that ImportPack verified, with generations. It reads
// only the commits that the pack stores whole, so no delta chain is walked; a commit stored as a delta gets its record
// when a compaction rewrites the pack. It parses each commit at once and keeps no object data, stops when the request ends, and gives up on the graph past
// importGraphWork.
func (s *Service) graphOfPack(ctx context.Context, repo Repo, p *gitpack.Pack) []midx.Commit {
	var commits []midx.Commit
	var work int64
	for name := range p.Names() {
		if ctx.Err() != nil {
			return nil
		}
		t, n, whole := p.WholeType(name)
		if !whole || t != gitpack.Commit {
			continue
		}
		if work += n; work > importGraphWork {
			return nil
		}
		if _, data, err := p.Get(name); err == nil {
			if c, ok := commitRecord(name, data); ok {
				if commits = append(commits, c); len(commits) > s.graphCap {
					return nil
				}
			}
		}
	}
	return s.withGenerations(ctx, repo, commits)
}

// graphBytes is the size of the graph files that a merge or a compaction of layers reads: none when the layers hold
// more commits than a graph can, because it reads no graph then.
func graphBytes(layers []midx.Entry) int64 {
	var n, commits int64
	for _, e := range layers {
		n += e.GraphSize
		commits += int64(e.Commits)
	}
	if commits > midx.MaxGraphCommits {
		return 0
	}
	return n
}

// maxGraphWorkingBytes bounds the graph files that one lookup considers, which is half of the layer cache: graphs that
// cannot all stay in the cache would evict each other at each lookup.
const maxGraphWorkingBytes = maxIndexBytes / 2

// graphLayers returns the indexes of the layers whose graphs a lookup probes, newest first: the newest layers that
// have a graph, while their files together stay within maxGraphWorkingBytes. A commit in an older layer is read as an
// object.
func graphLayers(layers []midx.Entry) []int {
	var out []int
	var total int64
	for i := len(layers) - 1; i >= 0; i-- {
		if layers[i].Graph == "" {
			continue
		}
		if total += layers[i].GraphSize; total > maxGraphWorkingBytes {
			break
		}
		out = append(out, i)
	}
	return out
}
