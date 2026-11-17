package store

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/majorcontext/motte/internal/gitpack"
)

type Entry struct {
	Name string
	Mode uint32
	SHA  gitpack.SHA
	Size int64
}

func (e Entry) IsDir() bool     { return e.Mode == 0o40000 }
func (e Entry) IsSymlink() bool { return e.Mode == 0o120000 }
func (e Entry) IsExec() bool    { return e.Mode == 0o100755 }
func (e Entry) IsGitlink() bool { return e.Mode == 0o160000 }

type Tree struct {
	SHA     gitpack.SHA
	Entries []Entry
	byName  map[string]int
}

func NewTree(sha gitpack.SHA, es []Entry) *Tree {
	t := &Tree{SHA: sha, Entries: es, byName: make(map[string]int, len(es))}
	for i, e := range es {
		t.byName[e.Name] = i
	}
	return t
}

func (t *Tree) Lookup(name string) (Entry, bool) {
	i, ok := t.byName[name]
	if !ok {
		return Entry{}, false
	}
	return t.Entries[i], true
}

// Store is what the daemon needs from the version control store. One method is one remote call.
type Store interface {
	Commit(ctx context.Context, sha gitpack.SHA) (tree gitpack.SHA, when time.Time, err error)
	// TreeBulk returns the tree and its subtrees breadth first until maxEntries entries, with blob sizes.
	TreeBulk(ctx context.Context, sha gitpack.SHA, maxEntries int) ([]*Tree, error)
	Blobs(ctx context.Context, shas []gitpack.SHA) ([][]byte, error)
	PutObjects(ctx context.Context, objs []gitpack.Object) error
	Object(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error)
}

// HasStore reports which objects the store holds, without reading them.
type HasStore interface {
	Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error)
}

// WorkspaceStore records the head operation and mount commit, ordered by the op-heads generation of the swap.
type WorkspaceStore interface {
	PutWorkspace(ctx context.Context, op, commit string, generation int64) error
}

// EncodeTree serializes entries in git order.
func EncodeTree(es []Entry) []byte {
	sorted := append([]Entry(nil), es...)
	key := func(e Entry) string {
		if e.IsDir() {
			return e.Name + "/"
		}
		return e.Name
	}
	sort.Slice(sorted, func(i, j int) bool { return key(sorted[i]) < key(sorted[j]) })
	var out []byte
	for _, e := range sorted {
		out = strconv.AppendUint(out, uint64(e.Mode), 8)
		out = append(out, ' ')
		out = append(out, e.Name...)
		out = append(out, 0)
		out = append(out, e.SHA[:]...)
	}
	return out
}
