package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/store"
)

type wrongBlobs struct{ store.Store }

func (wrongBlobs) Blobs(_ context.Context, shas []gitpack.SHA) ([][]byte, error) {
	out := make([][]byte, len(shas))
	for i := range out {
		out[i] = []byte("not the blob")
	}
	return out, nil
}

// TestCacheRefusesABlobWhoseHashDiffers: the node cache is keyed by SHA and shared by all workspaces, so a blob that
// the store answers under the wrong SHA must not enter it.
func TestCacheRefusesABlobWhoseHashDiffers(t *testing.T) {
	dir := t.TempDir()
	c := store.NewCache(wrongBlobs{}, dir)
	want := gitpack.Hash(gitpack.Blob, []byte("the blob"))
	if p, err := c.Blob(t.Context(), want, nil); err == nil {
		t.Fatalf("Blob = %s, want an error", p)
	}
	if c.HasBlob(want) {
		t.Error("the cache holds the blob under the wrong content")
	}
	if es, _ := os.ReadDir(dir); len(es) != 0 {
		t.Errorf("the cache directory holds %d entries", len(es))
	}
}
