package gitpack

import (
	"bytes"
	"context"
	"testing"
)

func TestBuildRoundTrip(t *testing.T) {
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
