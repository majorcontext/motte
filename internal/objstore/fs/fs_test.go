package fs

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/majorcontext/motte/internal/objstore"
)

func TestBucket(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := s.Bucket("test-bucket")
	if ok, err := b.Create(ctx, "a/b", []byte("hello")); err != nil || !ok {
		t.Fatalf("create: %v %v", ok, err)
	}
	if ok, _ := b.Create(ctx, "a/b", []byte("other")); ok {
		t.Error("create replaced an object")
	}
	if got, _ := b.GetRange(ctx, "a/b", 1, 3); !bytes.Equal(got, []byte("ell")) {
		t.Errorf("range = %q", got)
	}
	if got, _ := b.GetRange(ctx, "a/b", 9, 3); len(got) != 0 {
		t.Errorf("range past end = %q", got)
	}
	if _, err := b.Get(ctx, "missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	gen, err := b.Copy(ctx, objstore.Copy{Source: "a/b", Destination: "c"})
	if err != nil || gen == 0 {
		t.Fatalf("copy: %v %d", err, gen)
	}
	if names, _ := b.List(ctx, "a/"); len(names) != 1 || names[0] != "a/b" {
		t.Errorf("list = %v", names)
	}
	if err := b.Delete(ctx, "a/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Attrs(ctx, "a/b"); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("after delete: %v", err)
	}
}
