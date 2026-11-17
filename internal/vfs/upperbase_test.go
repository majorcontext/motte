//go:build linux

package vfs

import (
	"io/fs"
	"reflect"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

// TestUpperScanReadsTheBaseThroughTheCache scans an upper directory against a base held by the cache: a moved
// directory lists the leaves of the cached subtree at its new path, and a path that is no base directory has no
// entries.
func TestUpperScanReadsTheBaseThroughTheCache(t *testing.T) {
	c := NewCache(nil, t.TempDir())
	tree := func(es ...Entry) gitpack.SHA {
		sha := gitpack.Hash(gitpack.Tree, EncodeTree(es))
		putTree(c, NewTree(sha, es))
		return sha
	}
	d := tree(blobEntry("x", "2"), Entry{Name: "run", Mode: 0o100755, SHA: gitpack.Hash(gitpack.Blob, []byte("3"))})
	base := tree(blobEntry("a", "1"), Entry{Name: "d", Mode: 0o40000, SHA: d}, Entry{Name: "sub", Mode: 0o160000})
	u := upperdir.NewMemFS()
	u.Put("moved", upperdir.MemNode{Mode: fs.ModeDir | 0o755, Xattr: map[string]string{upperdir.XattrRedirect: "/d"}})
	u.Put("d", upperdir.MemNode{Mode: fs.ModeDevice | fs.ModeCharDevice, Whiteout: true})
	es, err := upperdir.Scan(t.Context(), u, "", UpperBase(c, base))
	if err != nil {
		t.Fatal(err)
	}
	want := []DirtyEntry{{Path: "d", Kind: "deleted"}, {Path: "moved/run", Kind: "file", Exec: true}, {Path: "moved/x", Kind: "file"}}
	if got := DirtyFromUpper(es); !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
	if kids, err := UpperBase(c, base).List(t.Context(), "a/none"); err != nil || len(kids) != 0 {
		t.Errorf("List of a file path = %v, %v", kids, err)
	}
}
