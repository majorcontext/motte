package ctl

import (
	"encoding/binary"
	"testing"

	"github.com/majorcontext/motte/internal/gitpack"
)

// TestObjectCacheBoundsManyTinyObjects: a cache that charged only the data of an object would keep millions of tiny
// objects and hold far more memory than its bound.
func TestObjectCacheBoundsManyTinyObjects(t *testing.T) {
	t.Parallel()
	var c objectCache
	limit := objectCacheBytes / entryCost
	for i := range limit + 1000 {
		var data [8]byte
		binary.LittleEndian.PutUint64(data[:], uint64(i))
		c.add(gitpack.Hash(gitpack.Blob, data[:]), gitpack.Blob, data[:])
	}
	if len(c.objs) >= limit || len(c.order) != len(c.objs) {
		t.Errorf("kept %d objects in a map and %d in the queue, want fewer than %d and equal", len(c.objs), len(c.order), limit)
	}
	if c.size > objectCacheBytes {
		t.Errorf("size %d passes the bound %d", c.size, objectCacheBytes)
	}
}
