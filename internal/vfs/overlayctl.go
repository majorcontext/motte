//go:build linux

package vfs

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"slices"
	"time"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

func (v *VFS) now() time.Time { return v.clk.Now() }

func (v *VFS) upperResp(tok upperdir.Token) ctlResp {
	return ctlResp{Boot: bootID, Token: tok.String(bootID), BaseTree: v.base.String()}
}

// upperChanges lists every file of the merged view that differs from the base tree, with the base blob and the
// blob of the upper file, from one walk of the upper directory. A file of an upper entry is hashed only when the
// base has its path.
func (v *VFS) upperChanges(ctx context.Context) ([]ChangeEntry, upperdir.Token, error) {
	es, tok, err := upperdir.ScanStable(ctx, v.upperFS, "", UpperBase(v.c, v.base), v.now)
	if err != nil {
		return nil, tok, err
	}
	olds := map[string]Entry{}
	for _, e := range es {
		be, ok, err := v.baseEntry(ctx, v.rootTree, e.Path)
		if err != nil {
			return nil, tok, err
		}
		switch {
		case !ok:
		case !be.IsDir():
			olds[e.Path] = be
		case e.Kind != upperdir.KindDir || e.Opaque:
			if err := v.baseLeaves(ctx, be.SHA, e.Path, olds); err != nil {
				return nil, tok, err
			}
		}
	}
	news, err := v.upperNews(ctx, es, olds)
	if err != nil {
		return nil, tok, err
	}
	return diffEntries(olds, news), tok, nil
}

// upperNews finds the blob of each file and symlink entry in the merged view. An entry with an Origin takes
// the blob of the base and touches no upper file. The others are read through their parent directory, opened
// once for all its entries: a file the base has is hashed (at most 2 MiB each and 64 MiB in all), a symlink is
// read, and any other file only gets its mode and size.
func (v *VFS) upperNews(ctx context.Context, es []upperdir.Entry, olds map[string]Entry) (map[string]Entry, error) {
	news := map[string]Entry{}
	byDir := map[string][]upperdir.Entry{}
	for _, e := range es {
		if e.Kind != upperdir.KindFile && e.Kind != upperdir.KindSymlink {
			continue
		}
		if e.Origin != "" {
			n, ok, err := v.originBlob(ctx, e)
			if err != nil {
				return nil, err
			}
			if ok {
				news[e.Path] = n
				continue
			}
			e.NoContent = true
		}
		par, _ := split(e.Path)
		byDir[par] = append(byDir[par], e)
	}
	var hashed int64
	for _, par := range slices.Sorted(maps.Keys(byDir)) {
		d, err := v.upperFS.Open(par)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		err = v.readDir(d, byDir[par], olds, news, &hashed)
		_ = d.Close()
		if err != nil {
			return nil, err
		}
	}
	return news, nil
}

func (v *VFS) readDir(d upperdir.Dir, es []upperdir.Entry, olds, news map[string]Entry, hashed *int64) error {
	for _, e := range es {
		_, name := split(e.Path)
		limit := int64(-1)
		if _, inBase := olds[e.Path]; inBase {
			limit = min(maxHashedFile, maxHashedTotal-*hashed)
		}
		b, err := readEntry(d, e, name, limit)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		*hashed += b.Read
		n := Entry{Mode: 0o100644, Size: b.Size}
		switch {
		case b.Mode&fs.ModeSymlink != 0:
			n.Mode = 0o120000
		case b.Mode&0o100 != 0:
			n.Mode = 0o100755
		}
		if b.HasData {
			n.SHA = gitpack.Hash(gitpack.Blob, b.Data)
		}
		news[e.Path] = n
	}
	return nil
}

// readEntry reads the entry e of d. A file that the base lacks, or whose bytes are no content, gets only its
// mode and size from one lstat. An entry of any other type is gone.
func readEntry(d upperdir.Dir, e upperdir.Entry, name string, limit int64) (upperdir.Blob, error) {
	if e.Kind != upperdir.KindFile || limit >= 0 && !e.NoContent {
		return d.Read(name, limit)
	}
	st, err := d.Lstat(name)
	switch {
	case err != nil:
		return upperdir.Blob{}, err
	case st.Mode&fs.ModeSymlink != 0:
		return d.Read(name, limit)
	case !st.Mode.IsRegular():
		return upperdir.Blob{}, fs.ErrNotExist
	}
	return upperdir.Blob{Mode: st.Mode, Size: st.Size}, nil
}

// originBlob is the blob of the base file that an entry stands for, with the mode of the upper entry. ok is
// false when the base has no file at the origin.
func (v *VFS) originBlob(ctx context.Context, e upperdir.Entry) (n Entry, ok bool, err error) {
	b, found, err := v.baseEntry(ctx, v.rootTree, e.Origin)
	if err != nil || !found || b.IsDir() {
		return n, false, err
	}
	n = Entry{Mode: 0o100644, SHA: b.SHA, Size: b.Size}
	switch {
	case e.Kind == upperdir.KindSymlink:
		n.Mode = 0o120000
	case e.Exec:
		n.Mode = 0o100755
	}
	return n, true, nil
}
