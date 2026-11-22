package land

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"

	"github.com/majorcontext/motte/internal/gitpack"
)

const dirMode = 0o40000

// Entry is one entry of a tree. A directory has the mode 040000. Any other entry, a submodule too, is a leaf.
type Entry struct {
	Name string
	Mode uint32
	SHA  gitpack.SHA
}

func (e Entry) isDir() bool { return e.Mode == dirMode }

func parseTree(data []byte) ([]Entry, error) {
	var out []Entry
	for len(data) > 0 {
		sp, nul := bytes.IndexByte(data, ' '), bytes.IndexByte(data, 0)
		if sp < 0 || nul < sp || len(data) < nul+21 {
			return nil, Invalid("tree is malformed")
		}
		mode, err := strconv.ParseUint(string(data[:sp]), 8, 32)
		if err != nil {
			return nil, Invalid("tree is malformed")
		}
		e := Entry{Name: string(data[sp+1 : nul]), Mode: uint32(mode)}
		copy(e.SHA[:], data[nul+1:nul+21])
		out = append(out, e)
		data = data[nul+21:]
	}
	return out, nil
}

// sortKey is the order of git: a directory sorts as its name and a slash.
func sortKey(e Entry) string {
	if e.isDir() {
		return e.Name + "/"
	}
	return e.Name
}

func encodeTree(entries []Entry) []byte {
	slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare([]byte(sortKey(a)), []byte(sortKey(b))) })
	var b bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&b, "%o %s\x00", e.Mode, e.Name)
		b.Write(e.SHA[:])
	}
	return b.Bytes()
}

// Change is a leaf that differs between two trees. Old is nil for an addition and New is nil for a removal, and a
// change of the content or the mode of a leaf has both. A rename is a removal and an addition. A directory that
// becomes a file is the removal of each leaf under it and the addition of the file.
type Change struct {
	Path     string
	Old, New *Entry
}

// entries returns the entries of a tree. The zero SHA is the absent tree.
func (b *Batch) entries(ctx context.Context, sha gitpack.SHA) ([]Entry, error) {
	if sha == (gitpack.SHA{}) {
		return nil, nil
	}
	if es, ok := b.trees[sha]; ok {
		return es, nil
	}
	t, data, err := b.get(ctx, sha)
	if err != nil {
		return nil, err
	}
	if t != gitpack.Tree {
		return nil, Invalid(sha.String() + " is not a tree")
	}
	es, err := parseTree(data)
	if err != nil {
		return nil, err
	}
	b.trees[sha] = es
	return es, nil
}

func (b *Batch) leaves(ctx context.Context, sha gitpack.SHA, prefix string, fn func(string, Entry) error) error {
	es, err := b.entries(ctx, sha)
	if err != nil {
		return err
	}
	for _, e := range es {
		if !e.isDir() {
			if err := fn(prefix+e.Name, e); err != nil {
				return err
			}
			continue
		}
		if err := b.leaves(ctx, e.SHA, prefix+e.Name+"/", fn); err != nil {
			return err
		}
	}
	return nil
}

// diff calls emit for each leaf that differs between trees a and c, in path order within a directory.
func (b *Batch) diff(ctx context.Context, a, c gitpack.SHA, prefix string, emit func(Change) error) error {
	if a == c {
		return nil
	}
	ea, err := b.entries(ctx, a)
	if err != nil {
		return err
	}
	ec, err := b.entries(ctx, c)
	if err != nil {
		return err
	}
	ma, mc := map[string]Entry{}, map[string]Entry{}
	var names []string
	for _, e := range ea {
		ma[e.Name] = e
		names = append(names, e.Name)
	}
	for _, e := range ec {
		if _, both := ma[e.Name]; !both {
			names = append(names, e.Name)
		}
		mc[e.Name] = e
	}
	sort.Strings(names)
	for _, name := range names {
		x, hasX := ma[name]
		y, hasY := mc[name]
		if hasX && hasY && x.Mode == y.Mode && x.SHA == y.SHA {
			continue
		}
		if hasX && hasY && x.isDir() && y.isDir() {
			if err := b.diff(ctx, x.SHA, y.SHA, prefix+name+"/", emit); err != nil {
				return err
			}
			continue
		}
		if hasX && hasY && !x.isDir() && !y.isDir() {
			if err := emit(Change{Path: prefix + name, Old: &x, New: &y}); err != nil {
				return err
			}
			continue
		}
		if hasX {
			if err := b.side(ctx, x, prefix+name, true, emit); err != nil {
				return err
			}
		}
		if hasY {
			if err := b.side(ctx, y, prefix+name, false, emit); err != nil {
				return err
			}
		}
	}
	return nil
}

// side emits the removal (gone) or the addition of an entry and, for a directory, of each leaf under it.
func (b *Batch) side(ctx context.Context, e Entry, path string, gone bool, emit func(Change) error) error {
	one := func(p string, leaf Entry) error {
		if gone {
			return emit(Change{Path: p, Old: &leaf})
		}
		return emit(Change{Path: p, New: &leaf})
	}
	if !e.isDir() {
		return one(path, e)
	}
	return b.leaves(ctx, e.SHA, path+"/", one)
}

type node struct {
	leaf     *Change
	children map[string]*node
}

func (n *node) at(parts []string) *node {
	for _, p := range parts {
		if n.children == nil {
			n.children = map[string]*node{}
		}
		next := n.children[p]
		if next == nil {
			next = &node{}
			n.children[p] = next
		}
		n = next
	}
	return n
}

// apply returns the root that results from the changes on root, and writes the trees that differ. Changes that
// share no path with what changed on root since their base apply cleanly in any order.
func (b *Batch) apply(ctx context.Context, root gitpack.SHA, changes []Change) (gitpack.SHA, error) {
	top := &node{}
	for i := range changes {
		top.at(splitPath(changes[i].Path)).leaf = &changes[i]
	}
	sha, _, err := b.applyNode(ctx, root, top)
	if err != nil {
		return sha, err
	}
	if sha == (gitpack.SHA{}) {
		return b.put(gitpack.Tree, nil), nil
	}
	return sha, nil
}

func splitPath(p string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			parts, start = append(parts, p[start:i]), i+1
		}
	}
	return append(parts, p[start:])
}

// applyNode returns the SHA of the tree at base with n applied, and the zero SHA when the tree is empty.
func (b *Batch) applyNode(ctx context.Context, base gitpack.SHA, n *node) (gitpack.SHA, bool, error) {
	cur, err := b.entries(ctx, base)
	if err != nil {
		return base, false, err
	}
	byName := make(map[string]Entry, len(cur)+len(n.children))
	for _, e := range cur {
		byName[e.Name] = e
	}
	for name, child := range n.children {
		old, had := byName[name]
		if _, merge := b.merging[pathOf(child)]; merge {
			e, err := b.mergeLeaf(ctx, old, had, child.leaf)
			if err != nil {
				return base, false, err
			}
			byName[name] = e
			continue
		}
		if child.leaf != nil && child.leaf.New != nil {
			byName[name] = Entry{Name: name, Mode: child.leaf.New.Mode, SHA: child.leaf.New.SHA}
			continue
		}
		var sub gitpack.SHA
		if had && old.isDir() && child.leaf == nil {
			sub = old.SHA
		}
		delete(byName, name)
		if len(child.children) == 0 {
			continue
		}
		subSHA, _, err := b.applyNode(ctx, sub, child)
		if err != nil {
			return base, false, err
		}
		if subSHA != (gitpack.SHA{}) {
			byName[name] = Entry{Name: name, Mode: dirMode, SHA: subSHA}
		}
	}
	if len(byName) == 0 {
		return gitpack.SHA{}, true, nil
	}
	out := make([]Entry, 0, len(byName))
	for _, e := range byName {
		out = append(out, e)
	}
	sha := b.put(gitpack.Tree, encodeTree(out))
	b.produced[sha] = out
	return sha, false, nil
}
