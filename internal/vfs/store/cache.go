package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/tel"
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
	Tel tel.Telemetry
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

// BlobPath is the path of the blob in the node cache, whether or not it exists.
func (c *Cache) BlobPath(s gitpack.SHA) string {
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

// AddTree puts a decoded tree in the memory tier.
func (c *Cache) AddTree(t *Tree) {
	c.mu.Lock()
	c.trees[t.SHA] = t
	c.mu.Unlock()
}

// CachedTree returns a tree only when it is already in memory.
func (c *Cache) CachedTree(sha gitpack.SHA) *Tree {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trees[sha]
}

// HasBlob reports whether the blob is in the node cache.
func (c *Cache) HasBlob(s gitpack.SHA) bool {
	c.mu.Lock()
	_, ok := c.have[s]
	c.mu.Unlock()
	if ok {
		return true
	}
	if _, err := os.Stat(c.BlobPath(s)); err == nil {
		c.mu.Lock()
		c.have[s] = struct{}{}
		c.mu.Unlock()
		return true
	}
	return false
}

// held reports whether the file of the blob is in the node cache with the size of its tree entry. A file of another
// size is removed: a crash or a full disk left it short, and a read must not serve it.
func (c *Cache) held(e Entry) bool {
	p := c.BlobPath(e.SHA)
	fi, err := os.Stat(p)
	if err == nil && fi.Size() == e.Size {
		return true
	}
	c.mu.Lock()
	delete(c.have, e.SHA)
	c.mu.Unlock()
	if err == nil {
		_ = os.Remove(p)
	}
	return false
}

// Blob returns the path of the blob of e in the node cache, fetching it (plus read-ahead) if absent or if its file
// does not have the size of e. next yields read-ahead candidates after e; it may be nil.
func (c *Cache) Blob(ctx context.Context, e Entry, next func(yield func(Entry) bool)) (string, error) {
	s := e.SHA
	fetched := false
	for {
		if c.held(e) {
			c.BlobHits.Add(1)
			return c.BlobPath(s), nil
		}
		if fetched {
			return "", fmt.Errorf("store: the blob %s has %d bytes in the cache, and its tree entry says %d", s, c.size(s), e.Size)
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
			if c.HasBlob(cands[i].SHA) {
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
		fetched = true
	}
}

func (c *Cache) size(s gitpack.SHA) int64 {
	fi, err := os.Stat(c.BlobPath(s))
	if err != nil {
		return -1
	}
	return fi.Size()
}

const tmpPrefix = ".tmp-"

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
	ctx, sp := c.Tel.Start(ctx, "vfs.readahead", attribute.Int("files", len(batch)), attribute.Int64("bytes", extraBytes))
	n, err := c.fetchBlobs(ctx, batch)
	sp.SetAttributes(attribute.Int("fetched", n), attribute.Bool("fell_back", err == nil && n < len(batch)))
	tel.Finish(sp, err)
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
	if got := gitpack.Hash(gitpack.Blob, b); got != s {
		return fmt.Errorf("store: the blob answered for %s hashes to %s", s, got)
	}
	p := c.BlobPath(s)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := createLockedTemp(filepath.Dir(p))
	if err != nil {
		return err
	}
	defer tmp.Close()
	if _, err := tmp.Write(b); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return err
	}
	tier2Written.Add(int64(len(b)))
	return nil
}

// createLockedTemp creates a temporary file in dir and holds an exclusive flock on it until the caller closes it. The
// sweep of the cache removes only a temporary file that it can lock, and the kernel drops the lock of a process that
// exits, so a file that the sweep can lock has no live writer. The sweep may remove the file between the create and
// the lock; the writer then starts again.
func createLockedTemp(dir string) (*os.File, error) {
	for {
		f, err := os.CreateTemp(dir, tmpPrefix)
		if err != nil {
			return nil, err
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if pi, err := os.Stat(f.Name()); err == nil && os.SameFile(fi, pi) {
			return f, nil
		}
		_ = f.Close()
	}
}

// ReadBlob returns blob bytes from the node cache.
func (c *Cache) ReadBlob(ctx context.Context, e Entry) ([]byte, error) {
	p, err := c.Blob(ctx, e, nil)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

// RemoveStrayTemps removes the temporary files that a dead writer left in dir. It keeps every temporary file that it
// cannot lock: a live writer, in this process or another one that shares the cache, holds the lock.
func RemoveStrayTemps(dir string) {
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasPrefix(d.Name(), tmpPrefix) {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return nil
		}
		defer f.Close()
		if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			_ = os.Remove(p)
		}
		return nil
	})
}

// SeedCacheSize adds the size of the blobs that an earlier process left in dir to Tier2Bytes.
func SeedCacheSize(dir string) {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(d.Name(), tmpPrefix) {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	SeedTier2Bytes(n)
}
