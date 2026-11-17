//go:build linux

package overlaykit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func write(t *testing.T, root, rel, data string, mode os.FileMode) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(data), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
}

func rm(t *testing.T, root, rel string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(root, rel)); err != nil {
		t.Fatal(err)
	}
}

func emptyOverFile(t *testing.T, m string) {
	rm(t, m, "a.txt")
	if err := os.Mkdir(filepath.Join(m, "a.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestApplyGivesTheTreeOfTheMergedDirectory builds a base tree and a merged directory that differs from it,
// lists the differences as the daemon does, and requires that Apply gives the tree that `git add -A` of the
// merged directory gives. A wrong tree names a rule of the dirty set that Apply applied wrongly.
func TestApplyGivesTheTreeOfTheMergedDirectory(t *testing.T) {
	cases := []struct {
		name  string
		edit  func(t *testing.T, m string)
		dirty []Entry
	}{
		{"modify, add and chmod", func(t *testing.T, m string) {
			write(t, m, "a.txt", "changed", 0o644)
			write(t, m, "new/b.txt", "b", 0o644)
			write(t, m, "d/old1", "1", 0o755)
		}, []Entry{{Path: "a.txt", Kind: "file"}, {Path: "new/b.txt", Kind: "file"}, {Path: "d/old1", Kind: "file", Exec: true}}},
		{"deleted file and deleted directory", func(t *testing.T, m string) {
			rm(t, m, "a.txt")
			rm(t, m, "d")
		}, []Entry{{Path: "a.txt", Kind: "deleted"}, {Path: "d", Kind: "deleted"}}},
		{"opaque directory keeps only its new files", func(t *testing.T, m string) {
			rm(t, m, "d")
			write(t, m, "d/new", "n", 0o644)
		}, []Entry{{Path: "d", Kind: "dir", Opaque: true}, {Path: "d/new", Kind: "file"}}},
		{"file over a base directory", func(t *testing.T, m string) {
			rm(t, m, "d")
			write(t, m, "d", "file now", 0o644)
		}, []Entry{{Path: "d", Kind: "file"}}},
		{"directory over a base file", func(t *testing.T, m string) {
			rm(t, m, "a.txt")
			write(t, m, "a.txt/x", "x", 0o644)
		}, []Entry{{Path: "a.txt", Kind: "dir", Opaque: true}, {Path: "a.txt/x", Kind: "file"}}},
		{"moved leaves at the new path", func(t *testing.T, m string) {
			if err := os.Rename(filepath.Join(m, "d"), filepath.Join(m, "moved")); err != nil {
				t.Fatal(err)
			}
		}, []Entry{{Path: "d", Kind: "deleted"}, {Path: "moved/old1", Kind: "file"}, {Path: "moved/old2", Kind: "file"}}},
		{"empty directory adds nothing", func(t *testing.T, m string) {
			if err := os.Mkdir(filepath.Join(m, "empty"), 0o755); err != nil {
				t.Fatal(err)
			}
		}, []Entry{{Path: "empty", Kind: "dir"}}},
		{"empty directory over a base file", emptyOverFile, []Entry{{Path: "a.txt", Kind: "dir"}}},
		{"empty opaque directory over a base file", emptyOverFile, []Entry{{Path: "a.txt", Kind: "dir", Opaque: true}}},
		{"symlink", func(t *testing.T, m string) {
			if err := os.Symlink("../target", filepath.Join(m, "d/link")); err != nil {
				t.Fatal(err)
			}
		}, []Entry{{Path: "d/link", Kind: "symlink"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base, g := t.TempDir(), Git{Dir: filepath.Join(dir, "g.git"), Index: filepath.Join(dir, "apply.index")}
			if _, err := Init(g.Dir, ""); err != nil {
				t.Fatal(err)
			}
			write(t, base, "a.txt", "a", 0o644)
			write(t, base, "d/old1", "1", 0o644)
			write(t, base, "d/old2", "2", 0o644)
			o := Git{Dir: g.Dir, Work: base}
			if _, err := o.Run(nil, "add", "-A"); err != nil {
				t.Fatal(err)
			}
			baseTree, err := o.Line("write-tree")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := o.Run(nil, "checkout-index", "-a", "-f", "--prefix="+dir+"/m/"); err != nil {
				t.Fatal(err)
			}
			m := filepath.Join(dir, "m")
			tc.edit(t, m)
			want, err := (&Oracle{Git: Git{Dir: g.Dir, Work: m}, Shadow: m}).Tree(true)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Apply(g, baseTree, m, tc.dirty)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("tree %s, want %s\n%s", got, want, g.Report(baseTree, got, want))
			}
		})
	}
}

// TestApplyRefusesAWrongDirtyEntry lists a path that the merged directory lacks, an exec flag that its
// mode contradicts, and a fifo as a file. Apply must fail on each, because a tree built from a wrong set is a silent loss.
func TestApplyRefusesAWrongDirtyEntry(t *testing.T) {
	dir := t.TempDir()
	g := Git{Dir: filepath.Join(dir, "g.git"), Index: filepath.Join(dir, "apply.index")}
	if _, err := Init(g.Dir, ""); err != nil {
		t.Fatal(err)
	}
	empty, err := g.Line("hash-object", "-t", "tree", "-w", "--stdin")
	if err != nil {
		t.Fatal(err)
	}
	m := t.TempDir()
	write(t, m, "x", "x", 0o644)
	if err := unix.Mkfifo(filepath.Join(m, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, e := range []Entry{{Path: "gone", Kind: "file"}, {Path: "x", Kind: "file", Exec: true}, {Path: "pipe", Kind: "file"}} {
		if _, err := Apply(g, empty, m, []Entry{e}); err == nil || !strings.Contains(err.Error(), e.Path) {
			t.Fatalf("Apply(%+v) error = %v, want one that names the path", e, err)
		}
	}
}
