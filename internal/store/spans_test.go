package store

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/majorcontext/motte/internal/bucket"
)

type countingStore struct {
	bucket.Store
	reads atomic.Int64
}

func (c *countingStore) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.reads.Add(1)
	return c.Store.GetRange(ctx, key, off, n)
}

type fixedStore struct{ bucket.Store }

func (fixedStore) GetRange(context.Context, string, int64, int64) ([]byte, error) {
	return make([]byte, 16), nil
}

func TestAFlightThatFollowsAFilledCacheMakesNoBucketRead(t *testing.T) {
	t.Parallel()
	cs := &countingStore{Store: fixedStore{}}
	s := New(cs, nil)
	k := rangeKey{"pack", 0, 16}
	for range 3 {
		if _, err := s.loadRange(t.Context(), cs, k); err != nil {
			t.Fatal(err)
		}
	}
	if n := cs.reads.Load(); n != 1 {
		t.Errorf("3 flights for one range made %d bucket reads, want 1: a flight must look in the cache before it reads", n)
	}
}

func (c *countingStore) Unwrap() bucket.Store { return c.Store }

func (f fixedStore) Unwrap() bucket.Store { return f.Store }
