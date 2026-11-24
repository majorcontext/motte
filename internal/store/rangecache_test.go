package store

import "testing"

func TestAReadThatBeganBeforeADropDoesNotBringThePackBack(t *testing.T) {
	t.Parallel()
	c := newRangeCache(1 << 20)
	k := rangeKey{pack: "p", off: 0, end: 10}
	v := c.version()
	c.dropPack("p")
	c.put(k, make([]byte, 10), v)
	if _, ok := c.get(k); ok {
		t.Error("a range read before the drop entered the cache after it")
	}
	c.put(k, make([]byte, 10), c.version())
	if _, ok := c.get(k); !ok {
		t.Error("a range read after the drop was not kept")
	}
}

func TestTheRangeCacheChargesTheWholeAllocationOfARange(t *testing.T) {
	t.Parallel()
	c := newRangeCache(1 << 20)
	data := make([]byte, 10, 5000)
	c.put(rangeKey{pack: "p", off: 0, end: 10}, data, c.version())
	if _, _, used, _ := c.stats(); used != 5000+1+rangeEntryBytes {
		t.Errorf("the cache charged %d bytes for a range that holds 5000, want %d", used, 5000+1+rangeEntryBytes)
	}
	c.dropPack("p")
	if _, _, used, _ := c.stats(); used != 0 {
		t.Errorf("the cache charges %d bytes after the drop, want 0", used)
	}
}
