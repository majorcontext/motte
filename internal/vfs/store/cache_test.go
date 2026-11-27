package store_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/majorcontext/tools/testcount"

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
	t.Parallel()
	dir := t.TempDir()
	c := store.NewCache(wrongBlobs{}, dir)
	want := gitpack.Hash(gitpack.Blob, []byte("the blob"))
	if p, err := c.Blob(t.Context(), store.Entry{SHA: want, Size: 8}, nil); err == nil {
		t.Fatalf("Blob = %s, want an error", p)
	}
	if c.HasBlob(want) {
		t.Error("the cache holds the blob under the wrong content")
	}
	if es, _ := os.ReadDir(dir); len(es) != 0 {
		t.Errorf("the cache directory holds %d entries", len(es))
	}
}

type goodBlobs struct{ store.Store }

func (goodBlobs) Blobs(_ context.Context, shas []gitpack.SHA) ([][]byte, error) {
	out := make([][]byte, len(shas))
	for i := range out {
		out[i] = []byte("the blob")
	}
	return out, nil
}

func TestCacheRefetchesABlobFileOfTheWrongSize(t *testing.T) {
	t.Parallel()
	c := store.NewCache(goodBlobs{}, t.TempDir())
	e := store.Entry{SHA: gitpack.Hash(gitpack.Blob, []byte("the blob")), Size: 8}
	if err := os.MkdirAll(filepath.Dir(c.BlobPath(e.SHA)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.BlobPath(e.SHA), []byte("the"), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := c.Blob(t.Context(), e, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(p); string(b) != "the blob" {
		t.Errorf("the cache served %q", b)
	}
	if _, err := c.Blob(t.Context(), store.Entry{SHA: e.SHA, Size: 9}, nil); err == nil {
		t.Error("a tree entry of another size than the blob was served")
	}
}

func TestRemoveStrayTemps(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "ab")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{".tmp-123": "half", "cdef": "blob"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	store.RemoveStrayTemps(filepath.Dir(dir))
	if _, err := os.Stat(filepath.Join(dir, ".tmp-123")); err == nil {
		t.Error("the stray temporary file stays")
	}
	if _, err := os.Stat(filepath.Join(dir, "cdef")); err != nil {
		t.Error(err)
	}
}

func TestRemoveStrayTempsOfAMissingDirectoryIsANoOp(t *testing.T) {
	t.Parallel()
	store.RemoveStrayTemps(filepath.Join(t.TempDir(), "absent"))
}

const writerEnv = "MOTTE_TEST_TMP_WRITER"

func init() {
	if dir := os.Getenv(writerEnv); dir != "" {
		f, err := store.CreateLockedTempForTest(dir)
		if err != nil {
			os.Exit(2)
		}
		fmt.Println(f.Name())
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
}

// TestRemoveStrayTempsKeepsTheFileOfALiveWriterInAnotherProcess: a sweep that removed the temporary file of a live writer
// in a second process that shares the node cache would make the rename of that writer fail with ENOENT.
func TestRemoveStrayTempsKeepsTheFileOfALiveWriterInAnotherProcess(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cmd := testcount.Command(t, os.Args[0])
	cmd.Env = append(os.Environ(), writerEnv+"="+dir)
	in, _ := cmd.StdinPipe()
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	name, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	name = strings.TrimSpace(name)
	store.RemoveStrayTemps(dir)
	if _, err := os.Stat(name); err != nil {
		t.Fatal("the sweep removed the file of a live writer")
	}
	_ = in.Close()
	_ = cmd.Wait()
	store.RemoveStrayTemps(dir)
	if _, err := os.Stat(name); err == nil {
		t.Error("the sweep kept the file of a writer that exited")
	}
}
