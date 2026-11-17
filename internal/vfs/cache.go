package vfs

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/internal/gitpack"
)

// Cache is tier 1 (decoded trees in memory) and tier 2 (blobs on node-local disk).
type Cache struct {
	Store       Store
	Dir         string
	BulkEntries int
	PrefetchN   int
	PrefetchB   int64

	mu       sync.Mutex
	trees    map[gitpack.SHA]*Tree
	treeWait map[gitpack.SHA]chan struct{}
	have     map[gitpack.SHA]struct{}
	inflight map[gitpack.SHA]chan struct{}

	BlobHits, BlobMisses, TreeMisses, TreeHits atomic.Int64
	// Tel carries the correlation attributes of the spans of this cache.
	Tel Telemetry
}

func NewCache(st Store, dir string) *Cache {
	return &Cache{
		Store: st, Dir: dir, BulkEntries: 30000, PrefetchN: 4096, PrefetchB: 8 << 20,
		trees:    map[gitpack.SHA]*Tree{},
		treeWait: map[gitpack.SHA]chan struct{}{},
		have:     map[gitpack.SHA]struct{}{},
		inflight: map[gitpack.SHA]chan struct{}{},
	}
}

func (c *Cache) blobPath(s gitpack.SHA) string {
	h := s.String()
	return filepath.Join(c.Dir, h[:2], h[2:])
}

func (c *Cache) Tree(ctx context.Context, sha gitpack.SHA) (*Tree, error) {
	for {
		c.mu.Lock()
		if t, ok := c.trees[sha]; ok {
			c.mu.Unlock()
			c.TreeHits.Add(1)
			return t, nil
		}
		if ch, ok := c.treeWait[sha]; ok {
			c.mu.Unlock()
			<-ch
			continue
		}
		ch := make(chan struct{})
		c.treeWait[sha] = ch
		c.mu.Unlock()

		c.TreeMisses.Add(1)
		ts, err := c.Store.TreeBulk(ctx, sha, c.BulkEntries)
		c.mu.Lock()
		if err == nil {
			for _, t := range ts {
				c.trees[t.SHA] = t
			}
		}
		delete(c.treeWait, sha)
		close(ch)
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
}

// CachedTree returns a tree only when it is already in memory.
func (c *Cache) CachedTree(sha gitpack.SHA) *Tree {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trees[sha]
}

func (c *Cache) hasBlob(s gitpack.SHA) bool {
	c.mu.Lock()
	_, ok := c.have[s]
	c.mu.Unlock()
	if ok {
		return true
	}
	if _, err := os.Stat(c.blobPath(s)); err == nil {
		c.mu.Lock()
		c.have[s] = struct{}{}
		c.mu.Unlock()
		return true
	}
	return false
}

// Blob returns the path of the blob in the node cache, fetching it (plus read-ahead) if absent.
// next yields read-ahead candidates after s; it may be nil.
func (c *Cache) Blob(ctx context.Context, s gitpack.SHA, next func(yield func(Entry) bool)) (string, error) {
	for {
		if c.hasBlob(s) {
			c.BlobHits.Add(1)
			return c.blobPath(s), nil
		}
		var cands []Entry
		if next != nil {
			next(func(e Entry) bool {
				if e.Size > 1<<20 || e.IsSymlink() {
					return true
				}
				cands = append(cands, e)
				return len(cands) < 2*c.PrefetchN
			})
		}
		for i := 0; i < len(cands); {
			if c.hasBlob(cands[i].SHA) {
				cands[i] = cands[len(cands)-1]
				cands = cands[:len(cands)-1]
				continue
			}
			i++
		}
		c.mu.Lock()
		if ch, ok := c.inflight[s]; ok {
			c.mu.Unlock()
			<-ch
			continue
		}
		batch := []gitpack.SHA{s}
		chs := map[gitpack.SHA]chan struct{}{s: make(chan struct{})}
		c.inflight[s] = chs[s]
		var bytes int64
		for _, e := range cands {
			if len(batch) >= c.PrefetchN || bytes >= c.PrefetchB {
				break
			}
			if _, ok := c.have[e.SHA]; ok {
				continue
			}
			if _, ok := c.inflight[e.SHA]; ok {
				continue
			}
			if _, ok := chs[e.SHA]; ok {
				continue
			}
			ch := make(chan struct{})
			chs[e.SHA] = ch
			c.inflight[e.SHA] = ch
			batch = append(batch, e.SHA)
			bytes += e.Size
		}
		c.mu.Unlock()

		c.BlobMisses.Add(int64(len(batch)))
		n, err := c.fetchTraced(ctx, batch, bytes)
		got := map[gitpack.SHA]bool{}
		for _, h := range batch[:n] {
			got[h] = true
		}
		c.mu.Lock()
		for k, ch := range chs {
			if got[k] {
				c.have[k] = struct{}{}
			}
			delete(c.inflight, k)
			close(ch)
		}
		c.mu.Unlock()
		if err != nil {
			return "", err
		}
	}
}

// tier2Written is the number of blob bytes that this process wrote to the node cache.
var tier2Written atomic.Int64

// Tier2Bytes is the number of blob bytes that this process wrote to the node cache, plus the size of the cache
// that SeedTier2Bytes found at startup.
func Tier2Bytes() int64 { return tier2Written.Load() }

// SeedTier2Bytes adds the size of the cache directory at startup to Tier2Bytes.
func SeedTier2Bytes(n int64) { tier2Written.Add(n) }

// fetchTraced is fetchBlobs under the span of a read-ahead when the batch holds more than the wanted blob.
func (c *Cache) fetchTraced(ctx context.Context, batch []gitpack.SHA, extraBytes int64) (int, error) {
	if len(batch) < 2 {
		return c.fetchBlobs(ctx, batch)
	}
	ctx, sp := c.Tel.start(ctx, "vfs.readahead", attribute.Int("files", len(batch)), attribute.Int64("bytes", extraBytes))
	n, err := c.fetchBlobs(ctx, batch)
	sp.SetAttributes(attribute.Int("fetched", n), attribute.Bool("fell_back", err == nil && n < len(batch)))
	finish(sp, err)
	return n, err
}

// fetchBlobs reads batch and stores it in the node cache. When the store refuses the whole batch, it
// retries the first blob alone; the read-ahead blobs are not worth failing the one that is wanted.
func (c *Cache) fetchBlobs(ctx context.Context, batch []gitpack.SHA) (int, error) {
	n := len(batch)
	data, err := c.Store.Blobs(ctx, batch)
	if err != nil && n > 1 {
		n = 1
		data, err = c.Store.Blobs(ctx, batch[:1])
	}
	if err != nil {
		return 0, err
	}
	for i, b := range data[:n] {
		if err := c.writeBlob(batch[i], b); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (c *Cache) writeBlob(s gitpack.SHA, b []byte) error {
	p := c.blobPath(s)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return err
	}
	tier2Written.Add(int64(len(b)))
	return nil
}

// ReadBlob returns blob bytes from the node cache.
func (c *Cache) ReadBlob(ctx context.Context, s gitpack.SHA) ([]byte, error) {
	p, err := c.Blob(ctx, s, nil)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}
