package store

import "sync/atomic"

// DefaultRangeCacheBytes is the memory of the range cache when the option and the configuration do not set it.
const DefaultRangeCacheBytes = 256 << 20

// rangeEntryBytes is what an entry charges beyond its data and the length of its pack name: the entry, the boxed key,
// the map node and the ring slot.
const rangeEntryBytes = 256

type rangeKey struct {
	pack     string
	off, end int64
}

// rangeCache keeps the byte ranges that requests read from packs, so another request that reads the same range makes
// no bucket read. A pack is named by the hash of its bytes, so a cached range never changes.
type rangeCache struct {
	c      *clockCache
	epoch  atomic.Int64
	hits   atomic.Int64
	misses atomic.Int64
}

// newRangeCache returns a cache of at most max bytes. A max of 0 or less keeps nothing.
func newRangeCache(max int64) *rangeCache { return &rangeCache{c: newClockCache(max)} }

func (c *rangeCache) get(k rangeKey) ([]byte, bool) {
	if c.c.max <= 0 {
		return nil, false
	}
	v, ok := c.c.get(k)
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.hits.Add(1)
	return v.([]byte), true
}

// peek is get without the counters.
func (c *rangeCache) peek(k rangeKey) ([]byte, bool) {
	if c.c.max <= 0 {
		return nil, false
	}
	v, ok := c.c.peek(k)
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}

// version is the epoch that a reader takes before it reads from the bucket. dropPack changes it.
func (c *rangeCache) version() int64 { return c.epoch.Load() }

// put keeps data under k, unless a pack was dropped since version v was taken: the read may have begun before the
// deletion and must not bring the pack back. The caller never changes data afterward. The charge is the capacity of data, which the cache
// keeps whole. A range larger than the cache is not kept.
func (c *rangeCache) put(k rangeKey, data []byte, v int64) {
	size := int64(cap(data)) + int64(len(k.pack)) + rangeEntryBytes
	if c.c.max <= 0 || size > c.c.max {
		return
	}
	c.c.putIf(k, data, size, func() bool { return c.epoch.Load() == v })
}

// dropPack removes every range of the pack with the key pack, so a deleted pack is never served from here.
func (c *rangeCache) dropPack(pack string) {
	c.c.removeIf(func(k any) bool { return k.(rangeKey).pack == pack }, func() { c.epoch.Add(1) })
}

func (c *rangeCache) stats() (hits, misses, used, evicted int64) {
	return c.hits.Load(), c.misses.Load(), c.c.used.Load(), c.c.evicted.Load()
}
