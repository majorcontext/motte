package fs

import (
	"context"
	"os"
	"testing"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/bucket/buckettest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	buckettest.Run(t, func(t *testing.T) bucket.Store {
		r, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return r.Bucket("test-bucket")
	}, buckettest.Caps{ConditionalDelete: true})
}

func TestFileLayoutIsAHeaderBlockThenTheData(t *testing.T) {
	t.Parallel()
	r, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := r.Bucket("test-bucket")
	if _, err := b.Create(context.Background(), "a/b", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	p, _ := b.(dirBucket).path("a/b")
	if raw, _ := os.ReadFile(p); len(raw) != headerSize+5 || string(raw[headerSize:]) != "hello" {
		t.Errorf("file layout: %d bytes", len(raw))
	}
}
