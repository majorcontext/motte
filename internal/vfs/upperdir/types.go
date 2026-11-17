// Package upperdir reads the upper directory of the kernel overlay that serves a mount-mode workspace and
// reports which paths differ from the base tree that the read-only lower layer serves. It knows the
// trusted.overlay.* markers (whiteout, opaque, redirect, metacopy) and holds no state.
package upperdir

import (
	"context"
	"errors"
	"io/fs"
	"strings"
)

const (
	KindFile    = "file"
	KindSymlink = "symlink"
	KindDir     = "dir"
	KindDeleted = "deleted"
)

const (
	XattrOpaque   = "trusted.overlay.opaque"
	XattrRedirect = "trusted.overlay.redirect"
	XattrMetacopy = "trusted.overlay.metacopy"
)

// Entry is one path of the merged view that differs from the base tree.
type Entry struct {
	Path      string
	Kind      string
	Exec      bool
	Opaque    bool
	Origin    string // base path whose blob is the content, when the upper entry holds none
	NoContent bool
}

// Dirent is the lstat of one upper entry. Ctime is in nanoseconds.
type Dirent struct {
	Name     string
	Mode     fs.FileMode
	Whiteout bool
	Size     int64
	Ctime    int64
	Ino      uint64
}

// Blob is one regular file or symlink of a directory, from one inode. Data is the file bytes or the symlink
// target; a regular file past the limit has none. Size is len(Data) when Data is set.
type Blob struct {
	Mode    fs.FileMode
	Size    int64
	Data    []byte
	HasData bool
	// Read counts the bytes read from the file, also when Read discards them.
	Read int64
}

// FS opens the directories of an upper directory. The empty path is the root, and a missing path answers
// an error that matches fs.ErrNotExist.
type FS interface {
	Open(path string) (Dir, error)
}

// Dir is an open directory. A name is one entry of the directory, never a path.
type Dir interface {
	// Entries lists the entries with their lstat.
	Entries() ([]Dirent, error)
	// Stat is the lstat of the directory itself.
	Stat() (Dirent, error)
	Lstat(name string) (Dirent, error)
	// Attr reads an attribute of the directory itself.
	Attr(attr string) (value string, ok bool, err error)
	// Xattr reads an attribute of a regular file of the directory.
	Xattr(name, attr string) (value string, ok bool, err error)
	Read(name string, limit int64) (Blob, error)
	// Open opens a child directory.
	Open(name string) (Dir, error)
	Close() error
}

// BaseEntry is one entry of a base directory.
type BaseEntry struct {
	Name    string
	Dir     bool
	Symlink bool
	Exec    bool
}

// BaseTree reads the base tree. Paths use "/" and the empty path is the root. A submodule is a directory
// with no entries, as the mount shows it.
type BaseTree interface {
	Lookup(ctx context.Context, path string) (BaseEntry, bool, error)
	List(ctx context.Context, dir string) ([]BaseEntry, error)
}

func join(dir, name string) string {
	if dir == "" {
		return name
	}
	return dir + "/" + name
}

// reserved reports whether the root entry name belongs to the version control shims.
func reserved(dir, name string) bool {
	return dir == "" && (name == ".jj" || name == ".git")
}

func notExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }

func parent(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return ""
	}
	return p[:i]
}
