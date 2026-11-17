package upperdir

import (
	"context"
	"io/fs"
	"sort"
	"strings"
)

// dirCtx describes how a merged directory relates to the base tree.
type dirCtx struct {
	origin    string // the base directory that the kernel merges below this one
	hasOrigin bool
	plain     bool // the origin is this path and the base keeps it, so unlisted base entries stay unchanged
}

type scanner struct {
	ctx  context.Context
	base BaseTree
	out  []Entry
}

// Scan lists the paths of the merged view that differ from the base tree, sorted by path. root is the
// upper directory in fsys.
func Scan(ctx context.Context, fsys FS, root string, base BaseTree) ([]Entry, error) {
	d, err := fsys.Open(root)
	if err != nil {
		return nil, err
	}
	defer func() { _ = d.Close() }()
	s := &scanner{ctx: ctx, base: base}
	if _, err := s.dir(d, "", dirCtx{hasOrigin: true, plain: true}); err != nil {
		return nil, err
	}
	sort.Slice(s.out, func(i, j int) bool { return s.out[i].Path < s.out[j].Path })
	return s.out, nil
}

// dir walks the open upper directory d of the merged path m and returns how many entries the merged
// directory has.
func (s *scanner) dir(d Dir, m string, c dirCtx) (int, error) {
	ents, err := d.Entries()
	if err != nil {
		if notExist(err) {
			return 0, nil
		}
		return 0, err
	}
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name < ents[j].Name })
	shadow := make(map[string]bool, len(ents))
	n := 0
	for _, e := range ents {
		shadow[e.Name] = true
		if reserved(m, e.Name) {
			continue
		}
		p := join(m, e.Name)
		switch {
		case e.Whiteout:
			err = s.whiteout(p, c)
		case e.Mode.IsDir():
			n++
			err = s.child(d, p, e, c)
		case e.Mode.IsRegular() || e.Mode&fs.ModeSymlink != 0:
			n++
			err = s.leaf(d, p, e, c)
		default:
			err = s.whiteout(p, c)
		}
		if err != nil {
			return 0, err
		}
	}
	if !c.hasOrigin || c.plain {
		return n, nil
	}
	be, err := s.base.List(s.ctx, c.origin)
	if err != nil {
		return 0, err
	}
	for _, b := range be {
		if shadow[b.Name] || reserved(m, b.Name) {
			continue
		}
		n++
		if err := s.listBase(b, join(m, b.Name), join(c.origin, b.Name)); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// whiteout deletes the base path p. A special file at p does the same: the workspace does not track it, and
// the base entry is gone from the merged view.
func (s *scanner) whiteout(p string, c dirCtx) error {
	if !c.plain {
		return nil
	}
	if _, ok, err := s.base.Lookup(s.ctx, p); err != nil || !ok {
		return err
	}
	s.out = append(s.out, Entry{Path: p, Kind: KindDeleted})
	return nil
}

// resolve turns a redirect value into a base path: absolute from the root, or one name relative to the
// origin of the parent. It rejects what the kernel rejects: an empty, "." or ".." component.
func resolve(c dirCtx, r string) (string, bool) {
	if strings.HasPrefix(r, "/") {
		for _, comp := range strings.Split(r[1:], "/") {
			if comp == "" || comp == "." || comp == ".." {
				return "", false
			}
		}
		return r[1:], true
	}
	if !c.hasOrigin || r == "" || r == "." || r == ".." || strings.Contains(r, "/") {
		return "", false
	}
	return join(c.origin, r), true
}

func xattr(d Dir, name, attr string) (string, bool, error) {
	v, ok, err := d.Xattr(name, attr)
	if notExist(err) {
		return "", false, nil
	}
	return v, ok, err
}

// child handles an upper directory p inside a merged directory with context c.
func (s *scanner) child(d Dir, p string, e Dirent, c dirCtx) error {
	cd, err := d.Open(e.Name)
	if notExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = cd.Close() }()
	opq, _, err := cd.Attr(XattrOpaque)
	if err != nil {
		return err
	}
	var cc dirCtx
	if opq != "y" {
		r, ok, err := cd.Attr(XattrRedirect)
		if err != nil {
			return err
		}
		switch o, valid := resolve(c, r); {
		case ok && valid:
			cc.origin, cc.hasOrigin = o, true
		case !ok && c.hasOrigin:
			cc.origin, cc.hasOrigin = join(c.origin, e.Name), true
		}
		if cc.hasOrigin {
			b, found, err := s.base.Lookup(s.ctx, cc.origin)
			if err != nil {
				return err
			}
			cc.hasOrigin = found && b.Dir
		}
	}
	var present, merges bool
	if c.plain {
		b, found, err := s.base.Lookup(s.ctx, p)
		if err != nil {
			return err
		}
		present = found
		merges = found && b.Dir && cc.hasOrigin && cc.origin == p
	}
	if present && !merges {
		s.out = append(s.out, Entry{Path: p, Kind: KindDir, Opaque: true})
	}
	cc.plain = merges
	n, err := s.dir(cd, p, cc)
	if err != nil {
		return err
	}
	if n == 0 && !present {
		st, err := d.Lstat(e.Name)
		if notExist(err) || err == nil && !st.Mode.IsDir() {
			return nil
		}
		if err != nil {
			return err
		}
		s.out = append(s.out, Entry{Path: p, Kind: KindDir})
	}
	return nil
}

// leaf handles a regular file or a symlink of the upper directory.
func (s *scanner) leaf(d Dir, p string, e Dirent, c dirCtx) error {
	ent := Entry{Path: p, Kind: KindFile}
	switch {
	case e.Mode&fs.ModeSymlink != 0:
		ent.Kind = KindSymlink
	case e.Mode.IsRegular():
		ent.Exec = e.Mode&0o100 != 0
		marked, same, origin, err := s.metacopy(d, p, e, c)
		if err != nil || same {
			return err
		}
		ent.Origin, ent.NoContent = origin, marked && origin == ""
	}
	s.out = append(s.out, ent)
	return nil
}

// metacopy reads the metacopy marker of the regular file p. origin is the base file whose content the file
// stands for: the redirect, else the name in the base directory that the parent merges. It is empty when the
// file holds its data, or when the marker names no base file. marked reports the marker. same reports a file
// at its own base path that holds only the metadata of the base file.
func (s *scanner) metacopy(d Dir, p string, e Dirent, c dirCtx) (marked, same bool, origin string, err error) {
	if _, ok, err := xattr(d, e.Name, XattrMetacopy); err != nil || !ok {
		return false, false, "", err
	}
	r, hasRedirect, err := xattr(d, e.Name, XattrRedirect)
	if err != nil {
		return true, false, "", err
	}
	var o string
	switch v, valid := resolve(c, r); {
	case hasRedirect && valid:
		o = v
	case !hasRedirect && c.hasOrigin:
		o = join(c.origin, e.Name)
	default:
		return true, false, "", nil
	}
	b, found, err := s.base.Lookup(s.ctx, o)
	if err != nil || !found || b.Dir || b.Symlink {
		return true, false, "", err
	}
	return true, c.plain && o == p && b.Exec == (e.Mode&0o100 != 0), o, nil
}

// listBase lists the base entry b, and every leaf below it, at the merged path m.
func (s *scanner) listBase(b BaseEntry, m, o string) error {
	if !b.Dir {
		d := Entry{Path: m, Kind: KindFile, Exec: b.Exec, Origin: o}
		if b.Symlink {
			d.Kind, d.Exec = KindSymlink, false
		}
		s.out = append(s.out, d)
		return nil
	}
	kids, err := s.base.List(s.ctx, o)
	if err != nil {
		return err
	}
	for _, k := range kids {
		if err := s.listBase(k, join(m, k.Name), join(o, k.Name)); err != nil {
			return err
		}
	}
	return nil
}
