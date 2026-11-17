package upperdir

import (
	"context"
	"io/fs"
	"sort"
	"strings"
)

// memBase is a BaseTree over a leaf list: "p" is a file, "p*" an executable, "p@" a symlink.
type memBase struct {
	kids map[string][]BaseEntry
	all  map[string]BaseEntry
}

func newBase(leaves ...string) *memBase {
	b := &memBase{kids: map[string][]BaseEntry{}, all: map[string]BaseEntry{}}
	for _, l := range leaves {
		e := BaseEntry{}
		switch {
		case strings.HasSuffix(l, "*"):
			l, e.Exec = strings.TrimSuffix(l, "*"), true
		case strings.HasSuffix(l, "@"):
			l, e.Symlink = strings.TrimSuffix(l, "@"), true
		}
		parts := strings.Split(l, "/")
		for i := range parts {
			p := strings.Join(parts[:i+1], "/")
			if _, ok := b.all[p]; ok {
				continue
			}
			ent := BaseEntry{Name: parts[i], Dir: i < len(parts)-1}
			if !ent.Dir {
				ent.Symlink, ent.Exec = e.Symlink, e.Exec
			}
			b.all[p] = ent
			b.kids[parent(p)] = append(b.kids[parent(p)], ent)
		}
	}
	return b
}

func (b *memBase) Lookup(_ context.Context, p string) (BaseEntry, bool, error) {
	e, ok := b.all[p]
	return e, ok, nil
}

func (b *memBase) List(_ context.Context, dir string) ([]BaseEntry, error) { return b.kids[dir], nil }

func (b *memBase) leaves() map[string]BaseEntry {
	out := map[string]BaseEntry{}
	for p, e := range b.all {
		if !e.Dir {
			out[p] = e
		}
	}
	return out
}

// upper builds a MemFS from lines "<kind> <path> [redirect]": f file, x executable, l symlink, p fifo,
// w whiteout, d directory, o opaque directory, r redirected directory, m metacopy file, mx executable
// metacopy file, mr metacopy file with a redirect.
func upper(lines ...string) *MemFS {
	u := NewMemFS()
	for _, l := range lines {
		f := strings.Fields(l)
		n := MemNode{Mode: 0o644, Xattr: map[string]string{}}
		switch f[0] {
		case "x", "mx":
			n.Mode = 0o755
		case "l":
			n.Mode = fs.ModeSymlink | 0o777
		case "p":
			n.Mode = fs.ModeNamedPipe | 0o644
		case "w":
			n.Mode, n.Whiteout = fs.ModeDevice|fs.ModeCharDevice, true
		case "d", "o", "r":
			n.Mode = fs.ModeDir | 0o755
		}
		switch f[0] {
		case "o":
			n.Xattr[XattrOpaque] = "y"
		case "m", "mx", "mr":
			n.Xattr[XattrMetacopy] = ""
		}
		if f[0] == "r" || f[0] == "mr" {
			n.Xattr[XattrRedirect] = f[2]
		}
		u.Put(f[1], n)
	}
	return u
}

func sorted(es []Entry) []Entry {
	sort.Slice(es, func(i, j int) bool { return es[i].Path < es[j].Path })
	return es
}
