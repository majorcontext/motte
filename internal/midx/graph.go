package midx

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"iter"
	"slices"
	"sort"
)

const (
	graphMagic   = "MTCG"
	graphVersion = 1
	graphHeader  = 16
	graphRecord  = 96
	// MaxGraphCommits bounds the commits of one graph file, so a file is at most 16 MiB and a few files fit the layer
	// cache together. MaxGraphParents bounds the parents of a commit that a graph holds.
	MaxGraphCommits = 1 << 17
	MaxGraphParents = 64
	// MaxGraphOverflow bounds the overflow list of one graph file, in bytes.
	MaxGraphOverflow = 3 << 20
)

var (
	ErrCorruptGraph  = errors.New("midx: corrupt commit graph")
	ErrGraphTooLarge = errors.New("midx: too many commits for one graph")
)

// Commit is what a walk of history needs of a commit: its tree, its parents and its commit time. Gen is the length of
// the longest path from the commit to a root, counting the commit, and 0 when it is not known.
type Commit struct {
	SHA, Tree SHA
	Time      int64
	Gen       uint32
	Parents   []SHA
}

// BuildGraph returns the commit graph file of commits: the records sorted by name, then the parents after the second of
// each commit with more than two parents, then the SHA-256 of all of it. A commit that repeats an earlier one is kept
// once.
func BuildGraph(commits []Commit) ([]byte, error) {
	cs := slices.Clone(commits)
	slices.SortStableFunc(cs, func(a, b Commit) int { return bytes.Compare(a.SHA[:], b.SHA[:]) })
	cs = slices.CompactFunc(cs, func(a, b Commit) bool { return a.SHA == b.SHA })
	if len(cs) > MaxGraphCommits {
		return nil, ErrGraphTooLarge
	}
	if len(cs) == 0 {
		return nil, ErrCorruptGraph
	}
	var extra []byte
	out := make([]byte, graphHeader, graphHeader+graphRecord*len(cs)+32)
	for _, c := range cs {
		if len(c.Parents) > MaxGraphParents {
			return nil, ErrGraphTooLarge
		}
		rec := make([]byte, graphRecord)
		copy(rec[0:], c.SHA[:])
		copy(rec[20:], c.Tree[:])
		binary.BigEndian.PutUint64(rec[40:], uint64(c.Time))
		binary.BigEndian.PutUint32(rec[48:], c.Gen)
		binary.BigEndian.PutUint32(rec[52:], uint32(len(c.Parents)))
		switch n := len(c.Parents); {
		case n == 0:
		case n == 1:
			copy(rec[56:], c.Parents[0][:])
		case n == 2:
			copy(rec[56:], c.Parents[0][:])
			copy(rec[76:], c.Parents[1][:])
		default:
			copy(rec[56:], c.Parents[0][:])
			binary.BigEndian.PutUint32(rec[76:], uint32(len(extra)))
			for _, p := range c.Parents[1:] {
				extra = append(extra, p[:]...)
			}
		}
		out = append(out, rec...)
	}
	if len(extra) > MaxGraphOverflow {
		return nil, ErrGraphTooLarge
	}
	out = append(out, extra...)
	copy(out, graphMagic)
	binary.BigEndian.PutUint32(out[4:], graphVersion)
	binary.BigEndian.PutUint32(out[8:], uint32(len(cs)))
	binary.BigEndian.PutUint32(out[12:], uint32(len(extra)))
	sum := sha256.Sum256(out)
	return append(out, sum[:]...), nil
}

// Graph is a parsed commit graph file. It holds the bytes of the file.
type Graph struct {
	n     int
	data  []byte
	extra []byte
}

// ParseGraph checks the structure and the checksum of a graph file before it trusts a byte of it.
func ParseGraph(b []byte) (*Graph, error) {
	if len(b) < graphHeader+sha256.Size || string(b[:4]) != graphMagic || binary.BigEndian.Uint32(b[4:8]) != graphVersion {
		return nil, ErrCorruptGraph
	}
	n, extra := int(binary.BigEndian.Uint32(b[8:12])), int(binary.BigEndian.Uint32(b[12:16]))
	if n <= 0 || n > MaxGraphCommits || extra < 0 || extra > MaxGraphOverflow || extra%20 != 0 || len(b) != graphHeader+graphRecord*n+extra+sha256.Size {
		return nil, ErrCorruptGraph
	}
	if sum := sha256.Sum256(b[:len(b)-sha256.Size]); !bytes.Equal(sum[:], b[len(b)-sha256.Size:]) {
		return nil, ErrCorruptGraph
	}
	g := &Graph{n: n, data: b[graphHeader : graphHeader+graphRecord*n], extra: b[graphHeader+graphRecord*n : len(b)-sha256.Size]}
	used := 0
	for i := range n {
		if i > 0 && bytes.Compare(g.data[graphRecord*(i-1):graphRecord*(i-1)+20], g.data[graphRecord*i:graphRecord*i+20]) >= 0 {
			return nil, ErrCorruptGraph
		}
		np := binary.BigEndian.Uint32(g.data[graphRecord*i+52:])
		if np > MaxGraphParents {
			return nil, ErrCorruptGraph
		}
		if np > 2 {
			if off := int(binary.BigEndian.Uint32(g.data[graphRecord*i+76:])); off != used {
				return nil, ErrCorruptGraph
			}
			used += 20 * int(np-1)
		}
	}
	if used != len(g.extra) {
		return nil, ErrCorruptGraph
	}
	return g, nil
}

// Len is the number of commits.
func (g *Graph) Len() int { return g.n }

// Lookup returns the commit named sha.
func (g *Graph) Lookup(sha SHA) (Commit, bool) {
	i := sort.Search(g.n, func(i int) bool { return bytes.Compare(g.data[graphRecord*i:graphRecord*i+20], sha[:]) >= 0 })
	if i == g.n || !bytes.Equal(g.data[graphRecord*i:graphRecord*i+20], sha[:]) {
		return Commit{}, false
	}
	return g.at(i), true
}

func (g *Graph) at(i int) Commit {
	rec := g.data[graphRecord*i : graphRecord*(i+1)]
	c := Commit{Time: int64(binary.BigEndian.Uint64(rec[40:])), Gen: binary.BigEndian.Uint32(rec[48:])}
	copy(c.SHA[:], rec[0:])
	copy(c.Tree[:], rec[20:])
	np := int(binary.BigEndian.Uint32(rec[52:]))
	switch {
	case np == 0:
	case np <= 2:
		c.Parents = make([]SHA, 0, np)
		for k := range np {
			var p SHA
			copy(p[:], rec[56+20*k:])
			c.Parents = append(c.Parents, p)
		}
	default:
		var p SHA
		c.Parents = make([]SHA, 0, np)
		copy(p[:], rec[56:])
		c.Parents = append(c.Parents, p)
		off := int(binary.BigEndian.Uint32(rec[76:]))
		for k := range np - 1 {
			copy(p[:], g.extra[off+20*k:])
			c.Parents = append(c.Parents, p)
		}
	}
	return c
}

// Commits returns every commit of the graph in the order of their names.
func (g *Graph) Commits() iter.Seq[Commit] {
	return func(yield func(Commit) bool) {
		for i := range g.n {
			if !yield(g.at(i)) {
				return
			}
		}
	}
}

// MergeGraphs returns the graph file that holds the commits of gs. A commit that two graphs hold takes the larger
// generation, because 0 means that it is not known.
func MergeGraphs(gs []*Graph) ([]byte, error) {
	var all []Commit
	for _, g := range gs {
		all = slices.AppendSeq(all, g.Commits())
	}
	slices.SortStableFunc(all, func(a, b Commit) int {
		if c := bytes.Compare(a.SHA[:], b.SHA[:]); c != 0 {
			return c
		}
		return cmp.Compare(b.Gen, a.Gen)
	})
	return BuildGraph(all)
}
