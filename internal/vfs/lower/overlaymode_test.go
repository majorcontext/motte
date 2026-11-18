//go:build linux

package lower

import (
	"context"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/ctl"
	"github.com/majorcontext/motte/internal/vfs/store"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

type commitStore struct {
	store.Store
	tree gitpack.SHA
}

func (s commitStore) Commit(context.Context, gitpack.SHA) (gitpack.SHA, time.Time, error) {
	return s.tree, time.Unix(1, 0), nil
}

// overlayFixture is an overlay-mode VFS over a cached base tree and an in-memory upper directory. The base is
// a:"1", d/{x:"2", y:"3"}, f:"4", s:"same", k:"6", g/{h:"7", sub: a submodule}.
func overlayFixture(t *testing.T) (*VFS, *upperdir.MemFS) {
	t.Helper()
	c := store.NewCache(nil, t.TempDir())
	tree := func(es ...store.Entry) gitpack.SHA {
		sha := gitpack.Hash(gitpack.Tree, store.EncodeTree(es))
		putTree(c, store.NewTree(sha, es))
		return sha
	}
	d := tree(blobEntry("x", "2"), blobEntry("y", "3"))
	var ws []store.Entry
	for i := range 40 {
		ws = append(ws, blobEntry(fmt.Sprintf("w%02d", i), "x"))
	}
	w := tree(ws...)
	g := tree(blobEntry("h", "7"), store.Entry{Name: "sub", Mode: 0o160000, SHA: gitpack.Hash(gitpack.Blob, []byte("commit"))})
	base := tree(blobEntry("a", "1"), store.Entry{Name: "d", Mode: 0o40000, SHA: d}, blobEntry("f", "4"), blobEntry("s", "same"), blobEntry("k", "6"), store.Entry{Name: "g", Mode: 0o40000, SHA: g}, store.Entry{Name: "w", Mode: 0o40000, SHA: w})
	up := upperdir.NewMemFS()
	v, err := New(t.Context(), Config{Store: commitStore{tree: base}, Cache: c, Base: base, UpperFS: up})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Close)
	return v, up
}

func putTree(c *store.Cache, t *store.Tree) { c.AddTree(t) }

func blobEntry(name, body string) store.Entry {
	return store.Entry{Name: name, Mode: 0o100644, SHA: gitpack.Hash(gitpack.Blob, []byte(body)), Size: int64(len(body))}
}

func memFile(body string, mode fs.FileMode) upperdir.MemNode {
	return upperdir.MemNode{Mode: mode, Size: int64(len(body)), Data: []byte(body)}
}

var (
	memDir      = upperdir.MemNode{Mode: fs.ModeDir | 0o755}
	memWhiteout = upperdir.MemNode{Mode: fs.ModeDevice | fs.ModeCharDevice, Whiteout: true}
)

// TestOverlayModeAnswersEROFS requires every mutating operation of the lower layer to answer EROFS: a write
// that the lower accepts lands outside the upper directory, where the dirty set never sees it. An open for
// write or create fails.
func TestOverlayModeAnswersEROFS(t *testing.T) {
	t.Parallel()
	v, _ := overlayFixture(t)
	n, h, ctx := &Node{v: v}, &fhandle{v: v, fd: -1}, t.Context()
	for name, call := range map[string]func() syscall.Errno{
		"Mkdir":    func() syscall.Errno { _, e := n.Mkdir(ctx, "x", 0o755, &fuse.EntryOut{}); return e },
		"Symlink":  func() syscall.Errno { _, e := n.Symlink(ctx, "t", "x", &fuse.EntryOut{}); return e },
		"Create":   func() syscall.Errno { _, _, _, e := n.Create(ctx, "x", 0, 0o644, &fuse.EntryOut{}); return e },
		"Unlink":   func() syscall.Errno { return n.Unlink(ctx, "x") },
		"Rmdir":    func() syscall.Errno { return n.Rmdir(ctx, "x") },
		"Rename":   func() syscall.Errno { return n.Rename(ctx, "x", nil, "y", 0) },
		"Setattr":  func() syscall.Errno { return n.Setattr(ctx, nil, &fuse.SetAttrIn{}, &fuse.AttrOut{}) },
		"Write":    func() syscall.Errno { _, e := n.Write(ctx, nil, []byte("x"), 0); return e },
		"Allocate": func() syscall.Errno { return n.Allocate(ctx, nil, 0, 1, 0) },
		"hWrite":   func() syscall.Errno { _, e := h.Write(ctx, []byte("x"), 0); return e },
		"hAlloc":   func() syscall.Errno { return h.Allocate(ctx, 0, 1, 0) },
	} {
		if got := call(); got != syscall.EROFS {
			t.Errorf("%s answered %v, want EROFS", name, got)
		}
	}
	for _, flags := range []uint32{syscall.O_WRONLY, syscall.O_RDWR, syscall.O_TRUNC, syscall.O_APPEND, syscall.O_CREAT} {
		if _, _, got := n.Open(ctx, flags); got != syscall.EROFS {
			t.Errorf("open flags %#o answered %v, want EROFS", flags, got)
		}
	}
}

// TestOverlayModeRefusesAMountWithoutWhatItNeeds: a mount with no upper directory has no dirty set.
func TestOverlayModeRefusesAMountWithoutWhatItNeeds(t *testing.T) {
	t.Parallel()
	up := t.TempDir()
	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"no upper directory configured", Config{}, "needs the upper directory"},
		{"no upper directory", Config{UpperDir: up + "/none"}, "not a directory"},
	} {
		if _, err := New(t.Context(), tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}

// TestOverlayDirtyAndToken pins the dirty set and the change token of the upper directory against the base: the
// token moves with the walk, and an unsettled walk returns a token that equals no other.
func TestOverlayDirtyAndToken(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	up.Put("a", memFile("one", 0o644))
	up.Put("d", memWhiteout)
	up.Put("g", memWhiteout)
	up.Put("moved", upperdir.MemNode{Mode: fs.ModeDir | 0o755, Xattr: map[string]string{upperdir.XattrRedirect: "/d"}})
	es, tok, err := v.Dirty(t.Context())
	want := []ctl.DirtyEntry{{Path: "a", Kind: "file"}, {Path: "d", Kind: "deleted"}, {Path: "g", Kind: "deleted"}, {Path: "moved/x", Kind: "file"}, {Path: "moved/y", Kind: "file"}}
	if err != nil || !reflect.DeepEqual(es, want) {
		t.Fatalf("dirty = %+v %v, want %+v", es, err, want)
	}
	if again, err := v.Token(); err != nil || again.String("b") != tok.String("b") {
		t.Errorf("token = %v %v, want the token %v of dirty", again, err, tok)
	}
	up.Put("n", memFile("new", 0o644))
	if after, _ := v.Token(); after.String("b") == tok.String("b") {
		t.Errorf("token did not move after a new file: %q", after.String("b"))
	}
	up.Put("late", upperdir.MemNode{Mode: 0o644, Ctime: time.Now().UnixNano()})
	a, _ := v.Token()
	b, _ := v.Token()
	if a.String("b") == b.String("b") {
		t.Errorf("an unsettled walk returned one token twice: %q", a.String("b"))
	}
}

// TestOverlayCtlChanges requires the change list of the upper directory to match the list of the write
// overlay for each shape: an edit, an added file, an unchanged rewrite, a deleted directory, a moved directory
// (its leaves carry the blob of the base), a metadata-only copy-up with a new mode, a deleted directory that
// holds a submodule (reported like legacy mode, with the gitlink mode), and the reserved .jj.
func TestOverlayCtlChanges(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	up.Put("a", memFile("one", 0o644))
	up.Put("n", memFile("new", 0o644))
	up.Put("s", memFile("same", 0o644))
	up.Put("ln", upperdir.MemNode{Mode: fs.ModeSymlink | 0o777, Data: []byte("tgt")})
	up.Put("f", memWhiteout)
	up.Put("d", memWhiteout)
	up.Put("g", memWhiteout)
	up.Put("moved", upperdir.MemNode{Mode: fs.ModeDir | 0o755, Xattr: map[string]string{upperdir.XattrRedirect: "/d"}})
	up.Put("k", upperdir.MemNode{Mode: 0o755, Xattr: map[string]string{upperdir.XattrMetacopy: ""}})
	up.Put(".jj", memDir)
	up.Put(".jj/x", memFile("op", 0o644))
	rs := changesOf(t, v)
	sha := func(body string) string { return gitpack.Hash(gitpack.Blob, []byte(body)).String() }
	const reg, exe = 0o100644, 0o100755
	want := []ctl.ChangeEntry{
		{Path: "a", Status: "modified", OldSHA: sha("1"), OldMode: reg, OldSize: 1, NewSHA: sha("one"), NewMode: reg, Size: 3},
		{Path: "d/x", Status: "deleted", OldSHA: sha("2"), OldMode: reg, OldSize: 1},
		{Path: "d/y", Status: "deleted", OldSHA: sha("3"), OldMode: reg, OldSize: 1},
		{Path: "f", Status: "deleted", OldSHA: sha("4"), OldMode: reg, OldSize: 1},
		{Path: "g/h", Status: "deleted", OldSHA: sha("7"), OldMode: reg, OldSize: 1},
		{Path: "g/sub", Status: "deleted", OldSHA: sha("commit"), OldMode: 0o160000},
		{Path: "k", Status: "modified", OldSHA: sha("6"), OldMode: reg, OldSize: 1, NewSHA: sha("6"), NewMode: exe, Size: 1},
		{Path: "ln", Status: "added", NewSHA: sha("tgt"), NewMode: 0o120000, Size: 3},
		{Path: "moved/x", Status: "added", NewSHA: sha("2"), NewMode: reg, Size: 1},
		{Path: "moved/y", Status: "added", NewSHA: sha("3"), NewMode: reg, Size: 1},
		{Path: "n", Status: "added", NewMode: reg, Size: 3},
	}
	if !reflect.DeepEqual(rs, want) {
		t.Errorf("changes = %+v\nwant %+v", rs, want)
	}
}

type countFS struct {
	upperdir.FS
	opens map[string]int
}

func (c countFS) Open(p string) (upperdir.Dir, error) {
	c.opens[p]++
	return c.FS.Open(p)
}

// TestOverlayCtlChangesOpensEachDirectoryOnce: reading the content of every entry from the root would cost a
// descent per entry. The 20 added files of one directory, and the moved directory, take one open of their
// parent and none for the origin entries.
func TestOverlayCtlChangesOpensEachDirectoryOnce(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	up.Put("bulk", memDir)
	for i := range 20 {
		up.Put(fmt.Sprintf("bulk/f%d", i), memFile("x", 0o644))
	}
	up.Put("d", memWhiteout)
	up.Put("moved", upperdir.MemNode{Mode: fs.ModeDir | 0o755, Xattr: map[string]string{upperdir.XattrRedirect: "/d"}})
	c := countFS{up, map[string]int{}}
	v.upperFS = c
	if rs := changesOf(t, v); len(rs) != 24 {
		t.Fatalf("changes = %+v", rs)
	}
	if c.opens["bulk"] != 1 || c.opens["moved"] != 0 {
		t.Errorf("opens by path = %v, want bulk once and moved never", c.opens)
	}
}

// TestOverlayCtlChangesReportsAReplacedSubmodule: an opaque directory over a submodule deletes the gitlink, as in
// legacy mode, and lists the new file.
func TestOverlayCtlChangesReportsAReplacedSubmodule(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	up.Put("g", memDir)
	up.Put("g/sub", upperdir.MemNode{Mode: fs.ModeDir | 0o755, Xattr: map[string]string{upperdir.XattrOpaque: "y"}})
	up.Put("g/sub/f", memFile("n", 0o644))
	rs := changesOf(t, v)
	want := []ctl.ChangeEntry{
		{Path: "g/sub", Status: "deleted", OldSHA: gitpack.Hash(gitpack.Blob, []byte("commit")).String(), OldMode: 0o160000},
		{Path: "g/sub/f", Status: "added", NewMode: 0o100644, Size: 1},
	}
	if !reflect.DeepEqual(rs, want) {
		t.Errorf("changes = %+v, want %+v", rs, want)
	}
}

// swapFS reports another type from Lstat, and another answer from Read, after the walk: the entries that
// changed between the scan and the read.
type swapFS struct {
	upperdir.FS
	lstat map[string]fs.FileMode
	read  func(name string, limit int64) (upperdir.Blob, error)
}

type swapDir struct {
	upperdir.Dir
	f swapFS
}

func (s swapFS) Open(p string) (upperdir.Dir, error) {
	d, err := s.FS.Open(p)
	return swapDir{d, s}, err
}

func (d swapDir) Lstat(name string) (upperdir.Dirent, error) {
	st, err := d.Dir.Lstat(name)
	if m, ok := d.f.lstat[name]; ok {
		st.Mode = m
	}
	return st, err
}

func (d swapDir) Read(name string, limit int64) (upperdir.Blob, error) {
	if d.f.read != nil {
		return d.f.read(name, limit)
	}
	return d.Dir.Read(name, limit)
}

// TestOverlayCtlChangesSkipsAnEntryThatChangedType: a file that the scan found and that is a directory, a
// fifo, a socket or a whiteout at the read is gone for this call; one that became a symlink is read as a
// symlink. Treating any of them as a file reports a regular file that does not exist.
func TestOverlayCtlChangesSkipsAnEntryThatChangedType(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	swapped := map[string]fs.FileMode{
		"n1": fs.ModeDir | 0o755, "n2": fs.ModeNamedPipe | 0o644, "n3": fs.ModeSocket | 0o644,
		"n4": fs.ModeDevice | fs.ModeCharDevice, "n5": fs.ModeSymlink | 0o777,
	}
	for _, n := range []string{"n1", "n2", "n3", "n4", "n5", "n6"} {
		up.Put(n, memFile("abc", 0o644))
	}
	v.upperFS = swapFS{FS: up, lstat: swapped, read: func(name string, limit int64) (upperdir.Blob, error) {
		return upperdir.Blob{Mode: fs.ModeSymlink | 0o777, Size: 1, Data: []byte("t"), HasData: true, Read: 1}, nil
	}}
	rs := changesOf(t, v)
	want := []ctl.ChangeEntry{
		{Path: "n5", Status: "added", NewSHA: gitpack.Hash(gitpack.Blob, []byte("t")).String(), NewMode: 0o120000, Size: 1},
		{Path: "n6", Status: "added", NewMode: 0o100644, Size: 3},
	}
	if !reflect.DeepEqual(rs, want) {
		t.Errorf("changes = %+v, want %+v", rs, want)
	}
}

// TestOverlayCtlChangesChargesBytesItDiscarded: a file that changes size after its bytes were read reports no
// data but still used the read. The 64 MiB budget then ends after 32 files of 2 MiB, so a file that is resized
// again and again cannot make the call read without limit.
func TestOverlayCtlChangesChargesBytesItDiscarded(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	up.Put("w", memDir)
	for i := range 40 {
		up.Put(fmt.Sprintf("w/w%02d", i), memFile("changed", 0o644))
	}
	var limits []int64
	v.upperFS = swapFS{FS: up, read: func(name string, limit int64) (upperdir.Blob, error) {
		limits = append(limits, limit)
		return upperdir.Blob{Mode: 0o644, Size: 7, Read: 2 << 20}, nil
	}}
	if rs := changesOf(t, v); len(rs) != 40 {
		t.Fatalf("changes = %+v", rs)
	}
	var open int
	for _, l := range limits {
		if l > 0 {
			open++
		}
	}
	if len(limits) > 33 || open != 32 {
		t.Errorf("limits %v: %d reads had room, want 32 and none after the budget", limits, open)
	}
}

// TestOverlayCtlChangesDoesNotHashAMetacopyWithoutOrigin: the upper copy of a metacopy file whose origin is
// unknown is sparse, so its bytes are not the content. The entry reports its mode and size and no new SHA.
func TestOverlayCtlChangesDoesNotHashAMetacopyWithoutOrigin(t *testing.T) {
	t.Parallel()
	v, up := overlayFixture(t)
	up.Put("a", upperdir.MemNode{Mode: 0o644, Size: 4, Data: []byte("junk"), Xattr: map[string]string{
		upperdir.XattrMetacopy: "", upperdir.XattrRedirect: "/nowhere"}})
	rs := changesOf(t, v)
	want := []ctl.ChangeEntry{{Path: "a", Status: "modified", OldSHA: gitpack.Hash(gitpack.Blob, []byte("1")).String(), OldMode: 0o100644, OldSize: 1, NewMode: 0o100644, Size: 4}}
	if !reflect.DeepEqual(rs, want) {
		t.Errorf("changes = %+v, want %+v", rs, want)
	}
}

func changesOf(t *testing.T, v *VFS) []ctl.ChangeEntry {
	t.Helper()
	cs, _, err := v.Changes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return cs
}
