package gitpack

import (
	"bytes"
	"context"
	"testing"
)

func TestBuildRoundTrip(t *testing.T) {
	t.Parallel()
	objs := []Object{{Blob, []byte("one")}, {Blob, []byte("two")}, {Blob, []byte("one")}, {Tree, nil}}
	pack, idx, names := Build(objs)
	p, err := NewPack(idx, bytes.NewReader(pack))
	if err != nil {
		t.Fatal(err)
	}
	if p.Count() != 3 || names[0] != names[2] {
		t.Fatalf("count = %d, names = %v", p.Count(), names)
	}
	if err := p.Verify(context.Background(), idx, pack); err != nil {
		t.Fatal(err)
	}
	for i, o := range objs {
		typ, data, err := p.Get(names[i])
		if err != nil || typ != o.Type || !bytes.Equal(data, o.Data) {
			t.Errorf("object %d: %v %v %q", i, err, typ, data)
		}
	}
	pack[20] ^= 1
	if err := p.Verify(context.Background(), idx, pack); err == nil {
		t.Error("corrupt pack verified")
	}
}

func TestVerifyRestoresReadState(t *testing.T) {
	t.Parallel()
	pack, idx, names := Build([]Object{{Blob, []byte("x")}})
	p, _ := NewPack(idx, bytes.NewReader(pack))
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Verify(ctx, idx, pack); err != nil {
		t.Fatal(err)
	}
	cancel()
	if p.CachedBytes() > maxCacheBytes {
		t.Errorf("cache holds %d bytes", p.CachedBytes())
	}
	if _, _, err := p.Get(names[0]); err != nil {
		t.Errorf("read after canceled verify: %v", err)
	}
}
