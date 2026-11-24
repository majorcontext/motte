package store

import (
	"sync"
	"testing"
)

func TestAHitTakesNoLock(t *testing.T) {
	t.Parallel()
	c := newClockCache(100)
	c.put("k", "v", 10)
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.get("k"); !ok || v != "v" {
		t.Errorf("get = %v, %v", v, ok)
	}
}

func TestAnEntryThatAHitFlaggedOutlivesOneThatNoHitFlagged(t *testing.T) {
	t.Parallel()
	c := newClockCache(30)
	for _, k := range []string{"a", "b", "c", "d"} {
		c.put(k, k, 10)
	}
	c.get("b")
	c.put("e", "e", 10)
	if _, ok := c.peek("b"); !ok {
		t.Error("the flagged entry b was evicted")
	}
	if _, ok := c.peek("c"); ok {
		t.Error("the unflagged entry c was kept")
	}
}

func TestTheChargeStaysExactUnderConcurrentUse(t *testing.T) {
	t.Parallel()
	c := newClockCache(500)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				k := (i*7 + g) % 64
				if _, ok := c.get(k); !ok {
					c.put(k, k, 10+k%5)
				}
				if i%1000 == 0 {
					c.removeIf(func(k any) bool { return k.(int)%16 == 0 }, nil)
				}
			}
		}()
	}
	wg.Wait()
	sum := 0
	for el := c.ring.Front(); el != nil; el = el.Next() {
		e := el.Value.(*clockEntry)
		sum += int(e.size)
		if _, ok := c.peek(e.key); !ok {
			t.Errorf("ring entry %v is not in the map", e.key)
		}
	}
	if used, _ := c.stats(); used != sum || used > 500 {
		t.Errorf("charge %d, entries hold %d, budget 500", used, sum)
	}
}

// A cache of 100 entries that a stream of new entries churns, where each entry is read again 10 inserts after it
// went in, must serve those reads: LRU does, and a CLOCK that evicts the entries it just added does not.
func TestAnEntryReadSoonAfterItsInsertIsStillThere(t *testing.T) {
	t.Parallel()
	c := newClockCache(1000)
	for i := range 100 {
		c.put(i, i, 10)
		c.get(i)
	}
	hits, total := 0, 0
	for i := 1000; i < 3000; i++ {
		c.put(i, i, 10)
		if i >= 1010 {
			total++
			if _, ok := c.get(i - 10); ok {
				hits++
			}
		}
	}
	if rate := float64(hits) / float64(total); rate < 0.95 {
		t.Errorf("hit rate %.2f for reads 10 inserts after the insert in a cache of 100 entries, want at least 0.95", rate)
	}
}

// A hot set that is read every round must outlive a scan of entries that are read once.
func TestAHotSetOutlivesAScan(t *testing.T) {
	t.Parallel()
	c := newClockCache(1000)
	for i := range 20 {
		c.put(i, i, 10)
	}
	hits, total := 0, 0
	for i := 1000; i < 6000; i++ {
		c.put(i, i, 10)
		k := i % 20
		total++
		if _, ok := c.get(k); ok {
			hits++
		} else {
			c.put(k, k, 10)
		}
	}
	if rate := float64(hits) / float64(total); rate < 0.95 {
		t.Errorf("hot set hit rate %.2f during a scan, want at least 0.95", rate)
	}
}
