//go:build linux

package overlaykit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRefuseGitlinksFindsASubmodule writes a tree with a gitlink and one without, in a real repository.
// RefuseGitlinks must name the submodule path in the first and pass the second, because a run on a base with
// a submodule gives a false TREE MISMATCH.
func TestRefuseGitlinksFindsASubmodule(t *testing.T) {
	dir := t.TempDir()
	g := Git{Dir: filepath.Join(dir, "g.git"), Index: filepath.Join(dir, "g.index")}
	if _, err := Init(g.Dir, ""); err != nil {
		t.Fatal(err)
	}
	out, err := g.Run([]byte("a"), "hash-object", "-w", "--stdin")
	if err != nil {
		t.Fatal(err)
	}
	blob := strings.TrimSpace(out)
	tree := func(info string) string {
		_ = os.Remove(g.Index)
		if _, err := g.Run([]byte(info), "update-index", "--add", "--index-info"); err != nil {
			t.Fatal(err)
		}
		out, err := g.Line("write-tree")
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	plain := tree("100644 " + blob + "\ta.txt\n")
	linked := tree("100644 " + blob + "\ta.txt\n160000 " + zeroSHA[:39] + "1\tdir/sub\n")
	if err := g.RefuseGitlinks(plain); err != nil {
		t.Fatalf("plain tree refused: %v", err)
	}
	if err := g.RefuseGitlinks(linked); err == nil || !strings.Contains(err.Error(), "dir/sub") {
		t.Fatalf("tree with a gitlink: error = %v, want one that names dir/sub", err)
	}
}
