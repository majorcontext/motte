package store

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type clockEntry struct {
	key, val any
	size     int64
	visited  atomic.Bool
}

// clockCache is a byte-bounded cache of immutable values. A hit takes no lock: it reads a sync.Map and sets a flag.
// An insert and an eviction take mu. Eviction is CLOCK over a circular list: the hand skips an entry that a hit
// flagged since its last pass, and clears the flag. A new entry is flagged and goes in just behind the hand, so it
// outlives the entries that the hand reaches first: an unflagged new entry would be the first one that the hand meets
// after it cleared the flags of all older entries. A reader can still get an entry that an eviction removed a moment before, which is safe
// because the values never change.
type clockCache struct {
	max     int64
	m       sync.Map
	used    atomic.Int64
	evicted atomic.Int64

	mu   sync.Mutex
	ring *list.List
	hand *list.Element
}

func newClockCache(max int64) *clockCache { return &clockCache{max: max, ring: list.New()} }

func (c *clockCache) get(k any) (any, bool) {
	v, ok := c.m.Load(k)
	if !ok {
		return nil, false
	}
	e := v.(*clockEntry)
	if !e.visited.Load() {
		e.visited.Store(true)
	}
	return e.val, true
}

// peek is get without the flag.
func (c *clockCache) peek(k any) (any, bool) {
	v, ok := c.m.Load(k)
	if !ok {
		return nil, false
	}
	return v.(*clockEntry).val, true
}

func (c *clockCache) put(k, v any, size int) { c.putIf(k, v, int64(size), nil) }

// putIf keeps v under k and charges size, and evicts until the charge fits, though never the entry it just added. It
// keeps nothing when admit is set and returns false; admit runs under mu. A key that is present only gets flagged.
func (c *clockCache) putIf(k, v any, size int64, admit func() bool) {
	e := &clockEntry{key: k, val: v, size: size}
	e.visited.Store(true)
	c.mu.Lock()
	defer c.mu.Unlock()
	if admit != nil && !admit() {
		return
	}
	if old, loaded := c.m.LoadOrStore(k, e); loaded {
		old.(*clockEntry).visited.Store(true)
		return
	}
	if c.hand == nil {
		c.ring.PushBack(e)
		c.hand = c.ring.Front()
	} else {
		c.ring.InsertBefore(e, c.hand)
	}
	c.used.Add(size)
	for c.used.Load() > c.max && c.ring.Len() > 1 {
		victim := c.hand.Value.(*clockEntry)
		if victim == e || victim.visited.Swap(false) {
			c.advance()
			continue
		}
		c.remove(c.hand)
		c.evicted.Add(1)
	}
}

// advance moves the hand to the next entry of the circle. The caller holds mu.
func (c *clockCache) advance() {
	if c.hand = c.hand.Next(); c.hand == nil {
		c.hand = c.ring.Front()
	}
}

// remove takes the element el out of the cache, and moves the hand past it when it points there. The caller holds mu.
func (c *clockCache) remove(el *list.Element) {
	next := el.Next()
	victim := c.ring.Remove(el).(*clockEntry)
	if el == c.hand {
		if c.hand = next; next == nil {
			c.hand = c.ring.Front()
		}
	}
	c.m.Delete(victim.key)
	c.used.Add(-victim.size)
}

// removeIf removes every entry whose key drop accepts, and runs after, when set, under mu.
func (c *clockCache) removeIf(drop func(k any) bool, after func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if after != nil {
		after()
	}
	for el := c.ring.Front(); el != nil; {
		next := el.Next()
		if drop(el.Value.(*clockEntry).key) {
			c.remove(el)
		}
		el = next
	}
}

func (c *clockCache) stats() (used, evicted int) { return int(c.used.Load()), int(c.evicted.Load()) }

func (c *clockCache) evictions() int { return int(c.evicted.Load()) }
