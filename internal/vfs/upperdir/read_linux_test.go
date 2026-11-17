package upperdir

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func readFixture(t *testing.T) (string, *MemFS) {
	t.Helper()
	dir := t.TempDir()
	for _, d := range []string{"a/sub"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "a/f"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("tgt", filepath.Join(dir, "a/ln")); err != nil {
		t.Fatal(err)
	}
	mem := NewMemFS()
	mem.Put("a", MemNode{Mode: fs.ModeDir | 0o755})
	mem.Put("a/sub", MemNode{Mode: fs.ModeDir | 0o755})
	mem.Put("a/f", MemNode{Mode: 0o644, Size: 5, Data: []byte("hello")})
	mem.Put("a/ln", MemNode{Mode: fs.ModeSymlink | 0o777, Data: []byte("tgt")})
	return dir, mem
}

// TestDirRead requires the same answer from OSFS and MemFS: a file over the limit reports its size and no
// bytes, a symlink always reports its target, and a missing entry or a directory answers fs.ErrNotExist. A
// wrong answer hashes a truncated blob into the change list.
func TestDirRead(t *testing.T) {
	dir, mem := readFixture(t)
	type got struct {
		size int64
		data string
		has  bool
	}
	for _, tc := range []struct {
		name  string
		entry string
		limit int64
		want  got
		gone  bool
	}{
		{"file within the limit", "f", 5, got{5, "hello", true}, false},
		{"file over the limit", "f", 4, got{5, "", false}, false},
		{"negative limit reads no file", "f", -1, got{5, "", false}, false},
		{"symlink ignores the limit", "ln", -1, got{3, "tgt", true}, false},
		{"directory", "sub", 9, got{}, true},
		{"missing", "none", 9, got{}, true},
	} {
		for name, fsys := range map[string]FS{"os": &OSFS{Dir: dir}, "mem": mem} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				d, err := fsys.Open("a")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = d.Close() }()
				b, err := d.Read(tc.entry, tc.limit)
				if tc.gone {
					if !errors.Is(err, fs.ErrNotExist) {
						t.Fatalf("err = %v, want fs.ErrNotExist", err)
					}
					return
				}
				if g := (got{b.Size, string(b.Data), b.HasData}); err != nil || g != tc.want {
					t.Fatalf("got %+v, %v; want %+v", g, err, tc.want)
				}
			})
		}
	}
}

// TestOSFSReadTakesModeSizeAndBytesFromOneInode swaps the file between the lstat and the open, and grows it
// after the read. The first must answer gone, never the metadata of one inode with the bytes of another; the
// second must report the new size and no bytes.
func TestOSFSReadTakesModeSizeAndBytesFromOneInode(t *testing.T) {
	dir, _ := readFixture(t)
	f := filepath.Join(dir, "a/f")
	o := &OSFS{Dir: dir}
	d, err := o.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	o.beforeOpen = func(string) {
		o.beforeOpen = nil
		if err := os.WriteFile(f+".new", []byte("other bytes"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(f+".new", f); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := d.Read("f", 99); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a swapped file answered %+v, %v; want fs.ErrNotExist", b, err)
	}
	o.afterRead = func(string) {
		o.afterRead = nil
		fh, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fh.WriteString("more")
		_ = fh.Close()
	}
	b, err := d.Read("f", 99)
	if err != nil || b.HasData || b.Size != int64(len("other bytes")+4) || b.Read != int64(len("other bytes")) {
		t.Fatalf("a grown file answered %+v, %v; want its new size, no bytes and the bytes it read", b, err)
	}
}

// TestOSFSReadSymlinkSwappedDuringTheReadIsGone: a symlink replaced by another between the first lstat and the
// check after readlinkat must not report the old inode's mode with the new target.
func TestOSFSReadSymlinkSwappedDuringTheReadIsGone(t *testing.T) {
	dir, _ := readFixture(t)
	ln := filepath.Join(dir, "a/ln")
	o := &OSFS{Dir: dir}
	d, err := o.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	o.afterRead = func(string) {
		o.afterRead = nil
		if err := os.Symlink("other", ln+".new"); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(ln+".new", ln); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := d.Read("ln", 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a swapped symlink answered %+v, %v; want fs.ErrNotExist", b, err)
	}
}

// TestOSFSReadSymlinkTurnedIntoAFileIsGone: readlinkat answers EINVAL for a regular file swapped over the
// symlink before the read. That is a vanished symlink, not an error that fails the whole walk.
func TestOSFSReadSymlinkTurnedIntoAFileIsGone(t *testing.T) {
	dir, _ := readFixture(t)
	ln := filepath.Join(dir, "a/ln")
	o := &OSFS{Dir: dir}
	d, err := o.Open("a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	o.beforeRead = func(string) {
		if err := os.Remove(ln); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(ln, []byte("file"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := d.Read("ln", 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a symlink turned into a file answered %+v, %v; want fs.ErrNotExist", b, err)
	}
}
