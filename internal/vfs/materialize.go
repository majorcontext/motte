package vfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/internal/gitpack"
)

type matFile struct {
	path string
	e    Entry
}

// Materialize writes the tree of commit below dest, for a workspace that has only a disk. A
// marker file records a finished checkout; Materialize then changes nothing, whatever commit the marker
// names, so a restart never overwrites the edits of the workspace. A half-written checkout has no marker, and
// Materialize removes it and writes it again.
func Materialize(ctx context.Context, st Store, commit gitpack.SHA, dest, marker string, uid, gid int) error {
	return MaterializeWith(ctx, Telemetry{}, st, commit, dest, marker, uid, gid)
}

// MaterializeWith is Materialize under the span vfs.materialize, with the files, bytes and store calls of the checkout.
func MaterializeWith(ctx context.Context, tel Telemetry, st Store, commit gitpack.SHA, dest, marker string, uid, gid int) error {
	ctx, span := tel.start(ctx, "vfs.materialize")
	if _, err := os.Stat(marker); err == nil {
		span.SetAttributes(attribute.Bool("skipped", true))
		finish(span, nil)
		return nil
	}
	var calls int64
	if c, ok := st.(interface{ Calls() int64 }); ok {
		calls = -c.Calls()
	}
	files, bytes, err := materialize(ctx, st, commit, dest, marker, uid, gid)
	if c, ok := st.(interface{ Calls() int64 }); ok {
		calls += c.Calls()
	}
	span.SetAttributes(attribute.Int("files", files), attribute.Int64("bytes", bytes), attribute.Int64("store_calls", calls))
	if err != nil {
		tel.logError(ctx, "vfs.materialize failed", err)
	}
	finish(span, err)
	return err
}

func materialize(ctx context.Context, st Store, commit gitpack.SHA, dest, marker string, uid, gid int) (int, int64, error) {
	treeSHA, when, err := st.Commit(ctx, commit)
	if err != nil {
		return 0, 0, err
	}
	if err := os.RemoveAll(dest); err != nil {
		return 0, 0, err
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return 0, 0, err
	}
	if err := os.Lchown(dest, uid, gid); err != nil {
		return 0, 0, err
	}
	c := NewCache(st, "")
	var files []matFile
	if err := walkTree(ctx, c, treeSHA, "", dest, uid, gid, &files); err != nil {
		return 0, 0, err
	}
	var size int64
	for _, f := range files {
		size += f.e.Size
	}
	if err := writeFiles(ctx, st, files, dest, when, uid, gid); err != nil {
		return len(files), size, err
	}
	if err := syncFS(dest); err != nil {
		return len(files), size, err
	}
	return len(files), size, writeMarker(marker, commit)
}

func walkTree(ctx context.Context, c *Cache, sha gitpack.SHA, rel, dest string, uid, gid int, files *[]matFile) error {
	t, err := c.Tree(ctx, sha)
	if err != nil {
		return err
	}
	for _, e := range t.Entries {
		p := join(rel, e.Name)
		full := filepath.Join(dest, p)
		switch {
		case e.IsDir() || e.IsGitlink():
			if err := os.Mkdir(full, 0o755); err != nil {
				return err
			}
			if err := os.Lchown(full, uid, gid); err != nil {
				return err
			}
			if e.IsDir() {
				if err := walkTree(ctx, c, e.SHA, p, dest, uid, gid, files); err != nil {
					return err
				}
			}
		default:
			*files = append(*files, matFile{p, e})
		}
	}
	return nil
}

func writeFiles(ctx context.Context, st Store, files []matFile, dest string, when time.Time, uid, gid int) error {
	const batchFiles, batchBytes = 4096, 8 << 20
	for len(files) > 0 {
		n, size := 0, int64(0)
		for n < len(files) && n < batchFiles && (n == 0 || size+files[n].e.Size <= batchBytes) {
			size += files[n].e.Size
			n++
		}
		shas := make([]gitpack.SHA, n)
		for i, f := range files[:n] {
			shas[i] = f.e.SHA
		}
		blobs, err := st.Blobs(ctx, shas)
		if err != nil {
			return err
		}
		for i, f := range files[:n] {
			if err := writeEntry(filepath.Join(dest, f.path), f.e, blobs[i], when, uid, gid); err != nil {
				return fmt.Errorf("vfs: write %s: %w", f.path, err)
			}
		}
		files = files[n:]
	}
	return nil
}

func writeEntry(full string, e Entry, data []byte, when time.Time, uid, gid int) error {
	if e.IsSymlink() {
		if err := os.Symlink(string(data), full); err != nil {
			return err
		}
		return os.Lchown(full, uid, gid)
	}
	mode := os.FileMode(0o644)
	if e.IsExec() {
		mode = 0o755
	}
	if err := os.WriteFile(full, data, mode); err != nil {
		return err
	}
	if err := os.Chmod(full, mode); err != nil {
		return err
	}
	if err := os.Lchown(full, uid, gid); err != nil {
		return err
	}
	return os.Chtimes(full, when, when)
}

func writeMarker(marker string, commit gitpack.SHA) error {
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(commit.String() + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(marker))
}

// syncFS writes the checkout to disk before the marker records it; only Linux can sync a whole file system.
var syncFS = func(string) error { return nil }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
