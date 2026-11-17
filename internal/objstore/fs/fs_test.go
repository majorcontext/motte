package fs

import (
	"bytes"
	"context"
	"errors"
	"os"
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
	if got, _ := b.GetRange(ctx, "a/b", 1, -1); !bytes.Equal(got, []byte("ello")) {
		t.Errorf("range to end = %q", got)
	}
	if got, _ := b.GetRange(ctx, "a/b", 9, 3); len(got) != 0 {
		t.Errorf("range past end = %q", got)
	}
	p, _ := b.(bucket).path("a/b")
	if raw, _ := os.ReadFile(p); len(raw) != headerSize+5 || string(raw[headerSize:]) != "hello" {
		t.Errorf("file layout: %d bytes", len(raw))
	}
	if _, err := b.Get(ctx, "missing"); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
}
