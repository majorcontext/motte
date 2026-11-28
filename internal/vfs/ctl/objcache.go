package ctl

import (
	"sync"

	"github.com/majorcontext/motte/internal/gitpack"
)

// objectCacheBytes bounds the objects that one server keeps. A jj command reads the commits and trees along its
// paths, some kilobytes each, and the next command reads most of them again.
const objectCacheBytes = 16 << 20

// entryCost is what one cached object costs beside its data: the map entry, the slice header and the queue entry,
// rounded up. Without it many tiny objects would hold far more memory than the bound.
const entryCost = 128

// objectCache keeps the objects that get answered and put stored, by name. An object never changes under its name,
// so a hit needs no store read. Past the bound it drops the oldest objects first.
type objectCache struct {
	mu    sync.Mutex
	objs  map[gitpack.SHA]cachedObject
	order []gitpack.SHA
	size  int
}

type cachedObject struct {
	t    gitpack.Type
	data []byte
}

func (c *objectCache) get(sha gitpack.SHA) (cachedObject, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, ok := c.objs[sha]
	return o, ok
}

func (c *objectCache) add(sha gitpack.SHA, t gitpack.Type, data []byte) {
	if len(data) > objectCacheBytes/16 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.objs == nil {
		c.objs = map[gitpack.SHA]cachedObject{}
	}
	if _, ok := c.objs[sha]; ok {
		return
	}
	c.objs[sha] = cachedObject{t, data}
	c.order = append(c.order, sha)
	c.size += entryCost + len(data)
	for c.size > objectCacheBytes {
		old := c.order[0]
		c.order = c.order[1:]
		c.size -= entryCost + len(c.objs[old].data)
		delete(c.objs, old)
	}
}
