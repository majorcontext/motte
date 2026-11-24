package store_test

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
)

func (w *world) classes() (out []string) {
	row, err := w.meta.Manifest(w.t.Context(), repo.String())
	if err != nil {
		w.t.Fatal(err)
	}
	m, err := midx.DecodeManifest(row.Body)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, l := range m.Layers {
		out = append(out, l.Class)
	}
	return out
}

func treeOf(names []string, shas []gitpack.SHA) []byte {
	var b []byte
	for i, n := range names {
		b = append(append(b, "100644 "+n+"\x00"...), shas[i][:]...)
	}
	return b
}

func TestAfterCompactionAWalkOfTheNewestTreesTouchesFewRanges(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	rng := rand.New(rand.NewPCG(3, 4))
	const dirs = 4
	names := []string{"a", "b", "c", "d"}
	sub := make([]gitpack.SHA, dirs)
	var all []gitpack.SHA
	var root, tip gitpack.SHA
	put := func(objs ...gitpack.Object) {
		if _, err := w.svc.PutObjects(t.Context(), repo, objs); err != nil {
			t.Fatal(err)
		}
	}
	for push := range 40 {
		data := make([]byte, 200<<10)
		fillRandom(rng, data)
		blob := gitpack.Object{Type: gitpack.Blob, Data: data}
		bsha := gitpack.Hash(gitpack.Blob, data)
		d := push % dirs
		leaf := gitpack.Object{Type: gitpack.Tree, Data: treeOf([]string{"f"}, []gitpack.SHA{bsha})}
		sub[d] = gitpack.Hash(gitpack.Tree, leaf.Data)
		top := gitpack.Object{Type: gitpack.Tree, Data: treeOf(names, sub)}
		root = gitpack.Hash(gitpack.Tree, top.Data)
		commit := gitpack.Object{Type: gitpack.Commit, Data: fmt.Appendf(nil, "tree %s\nauthor a <a> %d +0000\ncommitter a <a> %d +0000\n\nc%d\n", root, 1e9+push, 1e9+push, push)}
		tip = gitpack.Hash(gitpack.Commit, commit.Data)
		put(blob, leaf, top, commit)
	}
	for range 3 {
		if err := w.svc.Maintain(t.Context(), repo); err != nil {
			t.Fatal(err)
		}
	}
	trees := append([]gitpack.SHA{root}, sub...)
	all = append(all, trees...)
	all = append(all, tip)
	svc := w.cold()
	rd, err := svc.Reader(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	rd.Prefetch(t.Context(), all)
	for _, sha := range all {
		if _, _, err := rd.Get(t.Context(), sha); err != nil {
			t.Fatal(err)
		}
	}
	if st := svc.CacheStats(); st.RangeReads > 2 {
		t.Fatalf("the newest commit and its %d trees cost %d range reads after compaction; metadata must sit in dense ranges", len(trees), st.RangeReads)
	}
}

func entryBytes(typ byte, extra, data []byte) []byte {
	var b bytes.Buffer
	size, c := len(data), typ<<4|byte(len(data)&15)
	for size >>= 4; size > 0; size >>= 7 {
		b.WriteByte(c | 0x80)
		c = byte(size & 0x7f)
	}
	b.WriteByte(c)
	b.Write(extra)
	zw := zlib.NewWriter(&b)
	_, _ = zw.Write(data)
	_ = zw.Close()
	return b.Bytes()
}

func insertDelta(base int, result []byte) []byte {
	d := binary.AppendUvarint(binary.AppendUvarint(nil, uint64(base)), uint64(len(result)))
	for r := result; len(r) > 0; r = r[min(len(r), 127):] {
		d = append(append(d, byte(min(len(r), 127))), r[:min(len(r), 127)]...)
	}
	return d
}

type mixedObj struct {
	typ  gitpack.Type
	data []byte
}

func importMixed(w *world, seed int) (objs []mixedObj) {
	t := w.t
	blob := bytes.Repeat([]byte(fmt.Sprintf("blob %d ", seed)), 200)
	tree := treeOf([]string{"x"}, []gitpack.SHA{gitpack.Hash(gitpack.Blob, blob)})
	tree2 := append(slices.Clone(tree), treeOf([]string{"y"}, []gitpack.SHA{gitpack.Hash(gitpack.Blob, blob)})...)
	blob2 := append(slices.Clone(blob), "tail"...)
	commit := fmt.Appendf(nil, "tree %s\nauthor a <a> 1 +0000\ncommitter a <a> %d +0000\n\nm\n", gitpack.Hash(gitpack.Tree, tree), seed)
	treeSHA := gitpack.Hash(gitpack.Tree, tree)
	body := []byte("PACK\x00\x00\x00\x02\x00\x00\x00\x05")
	at := map[string]int{}
	add := func(name string, e []byte) {
		at[name] = len(body)
		body = append(body, e...)
	}
	add("blob", entryBytes(3, nil, blob))
	add("commit", entryBytes(1, nil, commit))
	add("tree", entryBytes(2, nil, tree))
	d := insertDelta(len(blob), blob2)
	add("blob2", entryBytes(6, gitpack.EncodeOfs(int64(len(body)-at["blob"])), d))
	d = insertDelta(len(tree), tree2)
	add("tree2", entryBytes(7, treeSHA[:], d))
	sum := sha1.Sum(body)
	pack := append(body, sum[:]...)
	objs = []mixedObj{{gitpack.Blob, blob}, {gitpack.Commit, commit}, {gitpack.Tree, tree}, {gitpack.Blob, blob2}, {gitpack.Tree, tree2}}
	var es []gitpack.IdxEntry
	for i, k := range []string{"blob", "commit", "tree", "blob2", "tree2"} {
		es = append(es, gitpack.IdxEntry{Name: gitpack.Hash(objs[i].typ, objs[i].data), Off: uint32(at[k])})
	}
	slices.SortFunc(es, func(a, b gitpack.IdxEntry) int { return bytes.Compare(a.Name[:], b.Name[:]) })
	id := sha256.Sum256(pack)
	if err := w.svc.ImportPack(t.Context(), repo, hex.EncodeToString(id[:]), gitpack.BuildIdx(es, sum), pack); err != nil {
		t.Fatal(err)
	}
	return objs
}

func TestCompactionOfMixedPacksSplitsClassesAndKeepsEveryObject(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	objs := append(importMixed(w, 1), importMixed(w, 2)...)
	for range 3 {
		if err := w.svc.Maintain(t.Context(), repo); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.classes(); !slices.Equal(got, []string{midx.ClassMeta, midx.ClassBlob}) && !slices.Equal(got, []string{midx.ClassBlob, midx.ClassMeta}) {
		t.Fatalf("classes of the layers after compaction: %q, want one metadata and one blob layer", got)
	}
	svc := w.cold()
	for _, o := range objs {
		typ, data, err := svc.Get(t.Context(), repo, gitpack.Hash(o.typ, o.data))
		if err != nil || typ != o.typ || !bytes.Equal(data, o.data) {
			t.Fatalf("object %s after the split: type %v, equal %v, error %v", gitpack.Hash(o.typ, o.data), typ, bytes.Equal(data, o.data), err)
		}
	}
}
