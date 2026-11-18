//go:build linux

package upperdir

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOSFSDoesNotDescendIntoASwappedSymlink replaces a listed directory with a symlink to a directory outside
// the upper root, as the sandbox can, after the listing and between the fstatat and the openat of the
// descent. The walk must not list what lies outside, also where O_NOFOLLOW lets the open follow the link.
func TestOSFSDoesNotDescendIntoASwappedSymlink(t *testing.T) {
	t.Parallel()
	for _, point := range []string{"afterEntries", "beforeOpen"} {
		root, outside := t.TempDir(), t.TempDir()
		for _, p := range []string{filepath.Join(root, "d", "inside"), filepath.Join(outside, "leak")} {
			_ = os.MkdirAll(filepath.Dir(p), 0o755)
			if err := os.WriteFile(p, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		swap := func() {
			_ = os.RemoveAll(filepath.Join(root, "d"))
			_ = os.Symlink(outside, filepath.Join(root, "d"))
		}
		o := &OSFS{Dir: root}
		if point == "afterEntries" {
			o.afterEntries = func(dir string) {
				if dir == "" {
					swap()
				}
			}
		} else {
			o.beforeOpen = func(name string) {
				if name == "d" {
					swap()
				}
			}
		}
		tok, err := Fingerprint(o, "", time.Now().Add(time.Hour))
		if err != nil || tok.Entries != 1 {
			t.Fatalf("%s: walk went through the symlink: %+v %v", point, tok, err)
		}
	}
}

// TestProbeTrustedNamesTheCapability requires a clear error without CAP_SYS_ADMIN, where a read of
// trusted.overlay.* answers no data and Scan would be wrong without a sign.
func TestProbeTrustedNamesTheCapability(t *testing.T) {
	t.Parallel()
	err := ProbeTrusted(t.TempDir())
	if os.Geteuid() == 0 {
		if err != nil {
			t.Skipf("file system holds no trusted.* attribute: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), "CAP_SYS_ADMIN") {
		t.Fatalf("ProbeTrusted without the capability = %v", err)
	}
}

// TestOSFSReadsAnEntryTurnedIntoASocketAsGone swaps a file for a socket between the fstatat and the openat of
// its attribute read, where open answers ENXIO. The scan must go on and list the file as it was found.
func TestOSFSReadsAnEntryTurnedIntoASocketAsGone(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	p := filepath.Join(root, "f")
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	o := &OSFS{Dir: root, beforeOpen: func(string) {
		_ = os.Remove(p)
		if l, err := net.Listen("unix", p); err == nil {
			t.Cleanup(func() { _ = l.Close() })
		}
	}}
	if _, err := Scan(t.Context(), o, "", newBase()); err != nil {
		t.Fatalf("a socket in place of a file failed the scan: %v", err)
	}
}

// TestProbeTrustedBesideLeavesTheUpperDirectoryAlone: the probe of an unprivileged process names the
// capability, and it neither writes into the upper directory nor leaves its sibling behind.
func TestProbeTrustedBesideLeavesTheUpperDirectoryAlone(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	upper := filepath.Join(parent, "upper")
	if err := os.Mkdir(upper, 0o755); err != nil {
		t.Fatal(err)
	}
	err := ProbeTrustedBeside(upper)
	if os.Geteuid() != 0 && (err == nil || !strings.Contains(err.Error(), "CAP_SYS_ADMIN")) {
		t.Errorf("ProbeTrustedBeside without the capability = %v", err)
	}
	if left, _ := os.ReadDir(parent); len(left) != 1 {
		t.Errorf("parent holds %v, want only upper", left)
	}
	if in, _ := os.ReadDir(upper); len(in) != 0 {
		t.Errorf("upper holds %v, want nothing", in)
	}
}
