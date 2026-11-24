package store

import (
	"sync"
	"sync/atomic"
)

// DefaultRangeCacheBytes is the memory of the range cache when the option and the configuration do not set it.
const DefaultRangeCacheBytes = 64 << 20

// rangeEntryBytes is what an entry charges beyond its data and the length of its pack name: the entry, the boxed key,
// the map node and the ring slot.
const rangeEntryBytes = 256

type rangeKey struct {
	pack     string
	off, end int64
}

type rangeEntry struct {
	key     rangeKey
	data    []byte
	size    int64
	visited atomic.Bool
}

// rangeCache keeps the byte ranges that requests read from packs, so another request that reads the same range makes
// no bucket read. A pack is named by the hash of its bytes, so a cached range never changes. A hit takes no lock: it
// reads a sync.Map and sets a flag. An insert and an eviction take mu. Eviction is CLOCK: the hand skips an entry that
// a hit flagged since its last pass, and clears the flag.
type rangeCache struct {
	max     int64
	m       sync.Map
	used    atomic.Int64
	epoch   atomic.Int64
	hits    atomic.Int64
	misses  atomic.Int64
	evicted atomic.Int64

	mu   sync.Mutex
	ring []*rangeEntry
	hand int
}

// newRangeCache returns a cache of at most max bytes. A max of 0 or less keeps nothing.
func newRangeCache(max int64) *rangeCache { return &rangeCache{max: max} }

func (c *rangeCache) get(k rangeKey) ([]byte, bool) {
	if c.max <= 0 {
		return nil, false
	}
	v, ok := c.m.Load(k)
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	e := v.(*rangeEntry)
	if !e.visited.Load() {
		e.visited.Store(true)
	}
	c.hits.Add(1)
	return e.data, true
}

// peek is get without the counters.
func (c *rangeCache) peek(k rangeKey) ([]byte, bool) {
	if c.max <= 0 {
		return nil, false
	}
	v, ok := c.m.Load(k)
	if !ok {
		return nil, false
	}
	return v.(*rangeEntry).data, true
}

// version is the epoch that a reader takes before it reads from the bucket. dropPack changes it.
func (c *rangeCache) version() int64 { return c.epoch.Load() }

// put keeps data under k, unless a pack was dropped since version v was taken: the read may have begun before the
// deletion and must not bring the pack back. The caller never changes data afterward. The charge is the capacity of data, which the cache
// keeps whole. A range larger than the cache is not kept.
func (c *rangeCache) put(k rangeKey, data []byte, v int64) {
	size := int64(cap(data)) + int64(len(k.pack)) + rangeEntryBytes
	if c.max <= 0 || size > c.max {
		return
	}
	e := &rangeEntry{key: k, data: data, size: size}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epoch.Load() != v {
		return
	}
	if old, loaded := c.m.LoadOrStore(k, e); loaded {
		old.(*rangeEntry).visited.Store(true)
		return
	}
	c.ring = append(c.ring, e)
	c.used.Add(size)
	for c.used.Load() > c.max && len(c.ring) > 1 {
		if c.hand >= len(c.ring) {
			c.hand = 0
		}
		victim := c.ring[c.hand]
		if victim == e || victim.visited.Swap(false) {
			c.hand++
			continue
		}
		c.removeAt(c.hand)
		c.evicted.Add(1)
	}
}

// removeAt takes the entry at ring index i out of the cache. The caller holds mu.
func (c *rangeCache) removeAt(i int) {
	victim := c.ring[i]
	last := len(c.ring) - 1
	c.ring[i] = c.ring[last]
	c.ring[last] = nil
	c.ring = c.ring[:last]
	c.m.Delete(victim.key)
	c.used.Add(-victim.size)
}

// dropPack removes every range of the pack with the key pack, so a deleted pack is never served from here.
func (c *rangeCache) dropPack(pack string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch.Add(1)
	for i := 0; i < len(c.ring); {
		if c.ring[i].key.pack == pack {
			c.removeAt(i)
			continue
		}
		i++
	}
}

func (c *rangeCache) stats() (hits, misses, used, evicted int64) {
	return c.hits.Load(), c.misses.Load(), c.used.Load(), c.evicted.Load()
}
