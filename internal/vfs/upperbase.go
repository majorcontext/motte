//go:build linux

package vfs

import (
	"context"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

// upperBase serves one base tree to upperdir.Scan, reading trees through the cache.
type upperBase struct {
	c    *Cache
	root gitpack.SHA
}

// UpperBase returns the base tree of a mount for upperdir.Scan. A submodule is a directory with no entries.
func UpperBase(c *Cache, root gitpack.SHA) upperdir.BaseTree {
	return upperBase{c: c, root: root}
}

// dirTree finds the tree of the directory p, or nil when p is not a directory of the base.
func (b upperBase) dirTree(ctx context.Context, p string) (*Tree, error) {
	t, err := b.c.Tree(ctx, b.root)
	if p == "" || err != nil {
		return t, err
	}
	for _, name := range strings.Split(p, "/") {
		e, ok := t.Lookup(name)
		if !ok || !e.IsDir() {
			return nil, nil
		}
		if t, err = b.c.Tree(ctx, e.SHA); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func baseEntry(e Entry) upperdir.BaseEntry {
	return upperdir.BaseEntry{Name: e.Name, Dir: e.IsDir() || e.IsGitlink(), Symlink: e.IsSymlink(), Exec: e.IsExec()}
}

func (b upperBase) Lookup(ctx context.Context, p string) (upperdir.BaseEntry, bool, error) {
	par, name := split(p)
	t, err := b.dirTree(ctx, par)
	if t == nil || err != nil {
		return upperdir.BaseEntry{}, false, err
	}
	e, ok := t.Lookup(name)
	return baseEntry(e), ok, nil
}

func (b upperBase) List(ctx context.Context, dir string) ([]upperdir.BaseEntry, error) {
	t, err := b.dirTree(ctx, dir)
	if t == nil || err != nil {
		return nil, err
	}
	out := make([]upperdir.BaseEntry, len(t.Entries))
	for i, e := range t.Entries {
		out[i] = baseEntry(e)
	}
	return out, nil
}

// DirtyFromUpper converts the entries of upperdir.Scan to the entries of the dirty op.
func DirtyFromUpper(es []upperdir.Entry) []DirtyEntry {
	out := make([]DirtyEntry, len(es))
	for i, e := range es {
		out[i] = DirtyEntry{Path: e.Path, Kind: e.Kind, Exec: e.Exec, Opaque: e.Opaque}
	}
	return out
}
