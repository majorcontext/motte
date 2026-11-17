//go:build linux

package lower

import (
	"context"
	"sort"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/ctl"
	"github.com/majorcontext/motte/internal/vfs/store"
)

// baseEntry finds the entry at p in the tree rt and fails when a subtree read fails.
func (v *VFS) baseEntry(ctx context.Context, rt *store.Tree, p string) (store.Entry, bool, error) {
	t := rt
	comps := strings.Split(p, "/")
	for i, c := range comps {
		e, ok := t.Lookup(c)
		if !ok || i == len(comps)-1 {
			return e, ok, nil
		}
		if !e.IsDir() {
			return store.Entry{}, false, nil
		}
		nt, err := v.c.Tree(ctx, e.SHA)
		if err != nil {
			return store.Entry{}, false, err
		}
		t = nt
	}
	return store.Entry{}, false, nil
}

func (v *VFS) baseLeaves(ctx context.Context, sha gitpack.SHA, prefix string, out map[string]store.Entry) error {
	t, err := v.c.Tree(ctx, sha)
	if err != nil {
		return err
	}
	for _, e := range t.Entries {
		p := join(prefix, e.Name)
		if e.IsDir() {
			if err := v.baseLeaves(ctx, e.SHA, p, out); err != nil {
				return err
			}
			continue
		}
		out[p] = e
	}
	return nil
}

func shaText(s gitpack.SHA) string {
	if s == (gitpack.SHA{}) {
		return ""
	}
	return s.String()
}

func diffEntries(olds, news map[string]store.Entry) []ctl.ChangeEntry {
	var out []ctl.ChangeEntry
	for p, o := range olds {
		n, ok := news[p]
		old := ctl.ChangeEntry{Path: p, OldSHA: o.SHA.String(), OldMode: o.Mode, OldSize: o.Size}
		switch {
		case !ok:
			old.Status = "deleted"
			out = append(out, old)
		case n.SHA != o.SHA || n.Mode != o.Mode:
			old.Status, old.NewSHA, old.NewMode, old.Size = "modified", shaText(n.SHA), n.Mode, n.Size
			out = append(out, old)
		}
	}
	for p, n := range news {
		if _, ok := olds[p]; !ok {
			out = append(out, ctl.ChangeEntry{Path: p, Status: "added", NewSHA: shaText(n.SHA), NewMode: n.Mode, Size: n.Size})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// maxHashedFile bounds the hash of one file; the shim hashes a larger file
// itself when it must compare it.
const maxHashedFile = 2 << 20

// maxHashedTotal bounds the bytes that one changes call hashes; past it the
// shim hashes the files it must compare.
const maxHashedTotal = 64 << 20
