package importer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
)

type packSink struct {
	sizes, objects []int
}

func (s *packSink) Has(_ context.Context, shas []gitpack.SHA) ([]bool, error) {
	return make([]bool, len(shas)), nil
}

func (s *packSink) ImportPack(_ context.Context, idx, pack []byte) error {
	s.sizes = append(s.sizes, len(pack))
	s.objects = append(s.objects, int(binary.BigEndian.Uint32(idx[8+255*4:])))
	return nil
}

func historyOrigin(t *testing.T, commits []int) string {
	dir := t.TempDir()
	git := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	git("init", "-q")
	for i, blobs := range commits {
		for j := range blobs {
			b := make([]byte, 1<<20)
			_, _ = rand.Read(b)
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d_%d", i, j)), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		git("add", "-A")
		git("commit", "-q", "-m", fmt.Sprint(i))
	}
	return dir
}

// TestImportHistoryBoundsEveryPack: a root commit of 5 incompressible 1 MiB blobs, and three commits whose range
// packs above a 4 MiB threshold, each reach the store only as packs of at most historyPackBytes, hold every object,
// and an oversize pack is never read into memory.
func TestImportHistoryBoundsEveryPack(t *testing.T) {
	historyPackBytes, historyChunkBytes = 4<<20, 2<<20
	t.Cleanup(func() { historyPackBytes, historyChunkBytes = 48<<20, 32<<20 })
	for _, tc := range []struct {
		name    string
		commits []int
		objects int
	}{
		{"one commit above the threshold", []int{5}, 5 + 2},
		{"a range above the threshold", []int{2, 2, 2}, 6 + 3 + 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin := historyOrigin(t, tc.commits)
			readFile = func(name string) ([]byte, error) {
				if fi, err := os.Stat(name); err == nil && fi.Size() > historyPackBytes {
					t.Errorf("read %s of %d bytes; want a pack over the threshold never loaded", name, fi.Size())
				}
				return os.ReadFile(name)
			}
			t.Cleanup(func() { readFile = os.ReadFile })
			var sink packSink
			if _, _, err := ImportHistory(t.Context(), &sink, "file://"+origin, "HEAD", t.Logf); err != nil {
				t.Fatal(err)
			}
			total := 0
			for i, n := range sink.sizes {
				if int64(n) > historyPackBytes {
					t.Errorf("pack %d has %d bytes; want at most %d", i, n, historyPackBytes)
				}
				total += sink.objects[i]
			}
			if total != tc.objects {
				t.Errorf("packs hold %d objects; want %d", total, tc.objects)
			}
		})
	}
}

// TestChunkObjectsBoundsBytesAndCount: 300,000 small blobs stay under the byte bound but would make an index over
// the 8 MiB limit of the store, so a chunk also stops at maxObjects; a blob over the byte bound stands alone.
func TestChunkObjectsBoundsBytesAndCount(t *testing.T) {
	var small []histObj
	for i := range 250 {
		small = append(small, histObj{0, 10, fmt.Sprint("b", i)})
	}
	for _, tc := range []struct {
		name  string
		in    []histObj
		bytes int64
		count int
		want  []int
	}{
		{"count", small, 1 << 20, 100, []int{100, 100, 50}},
		{"bytes", small, 1000, 1000, []int{100, 100, 50}},
		{"a blob over the bound", []histObj{{0, 500, "big"}, {0, 1, "a"}}, 100, 10, []int{1, 1}},
	} {
		var got []int
		for _, c := range chunkObjects(tc.in, tc.bytes, tc.count) {
			got = append(got, len(c))
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: chunk sizes %v; want %v", tc.name, got, tc.want)
		}
	}
}
