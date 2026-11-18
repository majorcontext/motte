//go:build linux

package upperdir

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

type dirBase struct{ root string }

func (d dirBase) entry(info os.FileInfo) BaseEntry {
	return BaseEntry{Name: info.Name(), Dir: info.IsDir(), Symlink: info.Mode()&fs.ModeSymlink != 0, Exec: info.Mode().IsRegular() && info.Mode()&0o100 != 0}
}

func (d dirBase) Lookup(_ context.Context, p string) (BaseEntry, bool, error) {
	info, err := os.Lstat(filepath.Join(d.root, p))
	if err != nil {
		return BaseEntry{}, false, nil
	}
	return d.entry(info), true, nil
}

func (d dirBase) List(_ context.Context, dir string) ([]BaseEntry, error) {
	es, err := os.ReadDir(filepath.Join(d.root, dir))
	var out []BaseEntry
	for _, e := range es {
		info, _ := e.Info()
		out = append(out, d.entry(info))
	}
	return out, err
}

// leaves reads every non-directory under root as path -> content, mode and link target.
func leaves(t *testing.T, root string) map[string]string {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		info, _ := e.Info()
		if !info.Mode().IsRegular() && info.Mode()&fs.ModeSymlink == 0 {
			return nil
		}
		v := fmt.Sprint(info.Mode()&0o100 != 0)
		if info.Mode()&fs.ModeSymlink != 0 {
			tgt, _ := os.Readlink(p)
			v = "link " + tgt
		} else if b, err := os.ReadFile(p); err == nil {
			v += " " + string(b)
		}
		rel, _ := filepath.Rel(root, p)
		out[rel] = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// editMerged applies the scripted edits through the merged directory.
func editMerged(t *testing.T, m func(string) string) {
	steps := [][3]string{
		{"write", "new", "new"}, {"write", "a/1", "changed"}, {"chmod", "a/2", "644"}, {"chmod", "b/1", "755"}, {"chmod", "b/1", "644"},
		{"rm", "c/g"}, {"mv", "a/2", "b/moved"}, {"mv", "c/d", "c/h"}, {"write", "c/h/extra", "x"}, {"rm", "c/h/e"},
		{"rm", "c/h/s"}, {"mkdir", "c/h/s"}, {"write", "c/h/s/u", "u"}, {"mv", "b", "c/b"}, {"rm", "a"}, {"mkdir", "a"},
		{"mkdir", "empty"}, {"rm", "top"}, {"mkdir", "top"}, {"write", "top/in", "in"}, {"mv", "r1", "r2"}, {"mkdir", "ed"},
		{"mv", "mv", "ed"}, {"mv", "rb", "rb2"}, {"mv", "rb2", "rb"}, {"link", "new", "new2"}, {"link", "lk", "lk2"},
		{"fifo", "fifo"}, {"mv", "ln", "ln2"},
	}
	for _, s := range steps {
		var err error
		switch p := m(s[1]); s[0] {
		case "write":
			err = os.WriteFile(p, []byte(s[2]), 0o644)
		case "chmod":
			mode, _ := strconv.ParseUint(s[2], 8, 32)
			err = os.Chmod(p, fs.FileMode(mode))
		case "rm":
			err = os.RemoveAll(p)
		case "mv":
			err = os.Rename(p, m(s[2]))
		case "mkdir":
			err = os.Mkdir(p, 0o755)
		case "link":
			err = os.Link(p, m(s[2]))
		case "fifo":
			err = unix.Mkfifo(p, 0o644)
		}
		// overlayfs may refuse a directory rename onto an empty directory; Scan must match the merged view either way.
		if s[0] == "mv" && errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			t.Fatalf("%v: %v", s, err)
		}
	}
}

// TestScanMatchesKernelOverlay mounts a real overlay with the options of mount mode, edits the merged
// directory and requires that the entries of Scan, applied to the lower files, give the merged files.
// It needs root and overlayfs with redirect_dir and metacopy, and skips otherwise.
func TestScanMatchesKernelOverlay(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount overlayfs and read trusted.* attributes")
	}
	root := t.TempDir()
	d := func(n string) string { return filepath.Join(root, n) }
	for _, n := range []string{"lower", "upper", "work", "merged"} {
		_ = os.Mkdir(d(n), 0o755)
	}
	for p, c := range map[string]string{"a/1": "one", "a/2": "two", "b/1": "b", "c/d/e": "e", "c/d/f": "f", "c/d/s/t": "t", "c/g": "g", "top": "top", "r1": "r1", "r2": "r2", "mv/1": "m", "rb/1": "rb", "lk": "lk"} {
		_ = os.MkdirAll(filepath.Dir(d("lower/"+p)), 0o755)
		if err := os.WriteFile(d("lower/"+p), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	_ = os.Chmod(d("lower/a/2"), 0o755)
	_ = os.Symlink("top", d("lower/ln"))
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s,redirect_dir=on,metacopy=on,index=off,xino=off,nfs_export=off", d("lower"), d("upper"), d("work"))
	if err := unix.Mount("overlay", d("merged"), "overlay", 0, opts); err != nil {
		t.Skipf("overlayfs mount unavailable: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(d("merged"), 0) })
	m := func(p string) string { return d("merged/" + p) }
	editMerged(t, m)

	got, err := Scan(t.Context(), &OSFS{Dir: d("upper")}, "", dirBase{d("lower")})
	if err != nil {
		t.Fatal(err)
	}
	lower, want := leaves(t, d("lower")), leaves(t, d("merged"))
	applied := maps.Clone(lower)
	drop := func(p string) {
		for k := range applied {
			if k == p || strings.HasPrefix(k, p+"/") {
				delete(applied, k)
			}
		}
	}
	for _, e := range got {
		drop(e.Path)
		st, err := os.Lstat(m(e.Path))
		switch {
		case e.Kind == KindDeleted:
			if err == nil {
				t.Errorf("entry %+v: the path exists in the merged view", e)
			}
		case err != nil:
			t.Errorf("entry %+v: the path is absent from the merged view", e)
		case e.Kind == KindDir:
			if !st.IsDir() {
				t.Errorf("entry %+v: not a directory", e)
			}
		case (e.Kind == KindSymlink) != (st.Mode()&fs.ModeSymlink != 0) || e.Kind == KindFile && (!st.Mode().IsRegular() || e.Exec != (st.Mode()&0o100 != 0)):
			t.Errorf("entry %+v: merged mode is %v", e, st.Mode())
		case lower[e.Path] == want[e.Path] || e.Path == "b/1":
			t.Errorf("entry %+v lists a file that equals the lower file", e)
		default:
			applied[e.Path] = want[e.Path]
		}
	}
	if fmt.Sprint(applied) != fmt.Sprint(want) {
		t.Fatalf("applied %v\nwant    %v\nentries %+v", applied, want, got)
	}
}
