package store

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
	"weak"

	"golang.org/x/sync/errgroup"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
	"github.com/majorcontext/motte/meta"
)

// lru is a byte-bounded cache of immutable values: layers and blocks of records are named by content.
type lru struct {
	mu      sync.Mutex
	max     int
	used    int
	evicted int
	ll      *list.List
	m       map[any]*list.Element
}

type lruEntry struct {
	key, val any
	size     int
}

func newLRU(max int) *lru { return &lru{max: max, ll: list.New(), m: map[any]*list.Element{}} }

func (c *lru) get(k any) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[k]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(e)
	return e.Value.(*lruEntry).val, true
}

func (c *lru) put(k, v any, size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.m[k]; ok {
		c.ll.MoveToFront(e)
		return
	}
	c.m[k] = c.ll.PushFront(&lruEntry{k, v, size})
	c.used += size
	for c.used > c.max && c.ll.Len() > 1 {
		old := c.ll.Back()
		en := old.Value.(*lruEntry)
		c.ll.Remove(old)
		delete(c.m, en.key)
		c.used -= en.size
		c.evicted++
	}
}

func (c *lru) stats() (used, evicted int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.evicted
}

func (c *lru) evictions() int {
	_, e := c.stats()
	return e
}

// layerLoadTimeout bounds a layer load that callers share, which no single caller cancels.
const layerLoadTimeout = 2 * time.Minute

// layerH is a loaded layer: the index of one pack kept whole, or the directory of a layer file whose blocks of records
// are read when a lookup needs them.
type layerH struct {
	key   string
	entry midx.Entry
	packs []midx.Pack
	idx   *gitpack.Pack
	dir   *midx.Dir
}

type blockKey struct {
	layer string
	blk   int
}

func (s *Service) layer(ctx context.Context, prefix string, e midx.Entry) (*layerH, error) {
	key := prefix + "/" + e.Key
	if h, ok := s.icache.get(key); ok {
		s.ihits.Add(1)
		return h.(*layerH), nil
	}
	ch := s.loads.DoChan(key, func() (any, error) {
		s.imisses.Add(1)
		load, cancel := context.WithTimeout(context.WithoutCancel(ctx), layerLoadTimeout)
		defer cancel()
		h, size, err := s.loadLayer(load, key, e)
		if err != nil {
			return nil, err
		}
		s.icache.put(key, h, size)
		return h, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*layerH), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// handle returns layer i of the view. The first probe of a request goes through the layer cache; the next ones read a
// weak pointer, so a layer that the cache evicts is not kept alive outside its budget.
func (v *view) handle(ctx context.Context, i int) (*layerH, error) {
	if w := v.handles[i].Load(); w != nil {
		if h := w.Value(); h != nil {
			return h, nil
		}
	}
	h, err := v.s.layer(ctx, v.prefix, v.layers[i])
	if err != nil {
		return nil, err
	}
	w := weak.Make(h)
	v.handles[i].Store(&w)
	return h, nil
}

func (s *Service) loadLayer(ctx context.Context, key string, e midx.Entry) (*layerH, int, error) {
	h := &layerH{key: key, entry: e}
	if e.IsIndex() {
		if len(e.Packs) != 1 {
			return nil, 0, midx.ErrCorrupt
		}
		data, err := s.st.Get(ctx, key)
		if err != nil {
			return nil, 0, mapNotFound(err)
		}
		s.idxBytes.Add(int64(len(data)))
		p, err := gitpack.NewPack(data, nil)
		if err != nil {
			return nil, 0, err
		}
		h.idx, h.packs = p, e.Packs
		return h, len(data), nil
	}
	if e.Dir <= 0 || int64(e.Dir) > e.Size {
		return nil, 0, midx.ErrCorrupt
	}
	tail, err := s.st.GetRange(ctx, key, e.Size-int64(e.Dir), int64(e.Dir))
	if err != nil {
		return nil, 0, mapNotFound(err)
	}
	s.idxBytes.Add(int64(len(tail)))
	d, err := midx.Parse(tail, e.Size)
	if err != nil {
		return nil, 0, err
	}
	if d.N != e.Objects || len(d.Packs) != e.NPacks {
		return nil, 0, midx.ErrCorrupt
	}
	h.dir, h.packs = d, d.Packs
	return h, len(tail), nil
}

// lookup finds sha in a layer, in the pack ord of the layer, or in any pack when ord is negative.
func (s *Service) lookup(ctx context.Context, h *layerH, sha gitpack.SHA, ord int) (int, int64, bool, error) {
	if h.idx != nil {
		if ord > 0 {
			return 0, 0, false, nil
		}
		off, ok := h.idx.Lookup(sha)
		return 0, off, ok, nil
	}
	r, ok, err := h.dir.Lookup(ctx, sha, ord, func(ctx context.Context, i int) ([]byte, error) { return s.block(ctx, h, i) })
	return int(r.Ord), int64(r.Off), ok, err
}

// place finds the pack and offset of each name of shas, sorted and distinct, newest layer first. A layer probes its
// names in one pass and reads each block of records once, at most readWorkers at a time.
func (v *view) place(ctx context.Context, shas []gitpack.SHA) (map[*openPack][]int64, error) {
	out := map[*openPack][]int64{}
	found, _, err := v.placeNamed(ctx, shas)
	for _, p := range found {
		out[p.e] = append(out[p.e], p.off)
	}
	return out, err
}

type placed struct {
	sha gitpack.SHA
	e   *openPack
	off int64
}

// placeNamed is place that keeps the name of each object, and returns the names that no layer holds.
func (v *view) placeNamed(ctx context.Context, shas []gitpack.SHA) ([]placed, []gitpack.SHA, error) {
	var out []placed
	pending := slices.Clone(shas)
	for i := len(v.layers) - 1; i >= 0 && len(pending) > 0; i-- {
		h, err := v.handle(ctx, i)
		if err != nil {
			return nil, nil, err
		}
		keep := pending[:0]
		if h.idx != nil {
			for _, sha := range pending {
				if off, ok := h.idx.Lookup(sha); ok {
					e := v.s.openPack(v.prefix, h.packs[0], h.entry.Class == midx.ClassMeta)
					out = append(out, placed{sha, e, off})
				} else {
					keep = append(keep, sha)
				}
			}
			pending = keep
			continue
		}
		names := make([]midx.SHA, len(pending))
		for j, sha := range pending {
			names[j] = sha
		}
		recs, found, err := h.dir.LookupMany(ctx, names, func(ctx context.Context, idx []int) ([][]byte, error) {
			return v.s.blocks(ctx, h, idx)
		})
		if err != nil {
			return nil, nil, err
		}
		for j, sha := range pending {
			if found[j] {
				e := v.s.openPack(v.prefix, h.packs[recs[j].Ord], h.entry.Class == midx.ClassMeta)
				out = append(out, placed{sha, e, int64(recs[j].Off)})
			} else {
				keep = append(keep, sha)
			}
		}
		pending = keep
	}
	return out, pending, nil
}

func (s *Service) blocks(ctx context.Context, h *layerH, idx []int) ([][]byte, error) {
	out := make([][]byte, len(idx))
	var g errgroup.Group
	g.SetLimit(readWorkers)
	for i, b := range idx {
		g.Go(func() (err error) {
			out[i], err = s.block(ctx, h, b)
			return err
		})
	}
	return out, g.Wait()
}

func (s *Service) block(ctx context.Context, h *layerH, i int) ([]byte, error) {
	k := blockKey{h.key, i}
	if b, ok := s.icache.get(k); ok {
		s.ihits.Add(1)
		return b.([]byte), nil
	}
	s.imisses.Add(1)
	off, n := h.dir.BlockRange(i)
	ctx, cancel := context.WithTimeout(ctx, blockTimeout)
	defer cancel()
	b, err := s.st.GetRange(ctx, h.key, off, n)
	if err != nil {
		return nil, mapNotFound(err)
	}
	if int64(len(b)) != n {
		return nil, midx.ErrCorrupt
	}
	s.idxBytes.Add(n)
	s.icache.put(k, b, len(b))
	return b, nil
}

func (s *Service) readLayer(h *layerH) midx.ReadFunc {
	return func(ctx context.Context, off, n int64) ([]byte, error) {
		b, err := s.st.GetRange(ctx, h.key, off, n)
		s.idxBytes.Add(int64(len(b)))
		return b, mapNotFound(err)
	}
}

// loadManifest returns the manifest of repo and its generation. A repo that has packs and no manifest gets one from
// its pack list, with one index layer for each pack. A repo with neither has an empty manifest of generation 0.
func (s *Service) loadManifest(ctx context.Context, repo Repo) (midx.Manifest, int64, error) {
	row, err := s.index.Manifest(ctx, repo.String())
	if errors.Is(err, meta.ErrNotFound) {
		return s.bootstrap(ctx, repo)
	}
	if err != nil {
		return midx.Manifest{}, 0, err
	}
	m, err := midx.DecodeManifest(row.Body)
	return m, row.Generation, err
}

func (s *Service) bootstrap(ctx context.Context, repo Repo) (midx.Manifest, int64, error) {
	type result struct {
		m   midx.Manifest
		gen int64
	}
	v, err, _ := s.loads.Do("bootstrap|"+repo.String(), func() (any, error) {
		ctx := context.WithoutCancel(ctx)
		packs, err := s.index.Packs(ctx, repo.String())
		if err != nil || len(packs) == 0 {
			return result{}, err
		}
		layers, err := s.indexLayers(ctx, repo, packs)
		if err != nil {
			return nil, err
		}
		m := midx.Manifest{Layers: layers}
		body, err := m.Encode()
		if err != nil {
			return nil, err
		}
		gen, err := s.index.SwapManifest(ctx, repo.String(), 0, body)
		if errors.Is(err, meta.ErrManifestStale) {
			row, err := s.index.Manifest(ctx, repo.String())
			if err != nil {
				return nil, err
			}
			m, err := midx.DecodeManifest(row.Body)
			return result{m, row.Generation}, err
		}
		return result{m, gen}, err
	})
	if err != nil {
		return midx.Manifest{}, 0, err
	}
	r := v.(result)
	return r.m, r.gen, nil
}

// indexLayers makes an index layer for each pack, reading only the header of its index.
func (s *Service) indexLayers(ctx context.Context, repo Repo, packs []meta.Pack) ([]midx.Entry, error) {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return nil, err
	}
	out := make([]midx.Entry, len(packs))
	var g errgroup.Group
	g.SetLimit(16)
	for i, p := range packs {
		g.Go(func() error {
			head, err := s.st.GetRange(ctx, prefix+"/"+p.ID+".idx", 0, gitpack.IdxHeadBytes)
			if err != nil {
				return fmt.Errorf("store: index of pack %s: %w", p.ID, mapNotFound(err))
			}
			s.idxBytes.Add(int64(len(head)))
			n, err := gitpack.IdxCount(head)
			if err != nil {
				return err
			}
			out[i] = indexEntry(p.ID, p.Bytes, n)
			return nil
		})
	}
	return out, g.Wait()
}

func indexEntry(id string, bytes int64, objects int) midx.Entry {
	return midx.Entry{Key: id + ".idx", Objects: objects, Bytes: bytes, NPacks: 1, Packs: []midx.Pack{{ID: id, Bytes: bytes}}}
}

const manifestRetries = 64

// updateManifest applies f to the current manifest and stores the result with a compare and swap, again on a lost
// swap. f returns false to leave the manifest as it is. f may run more than once.
func (s *Service) updateManifest(ctx context.Context, repo Repo, f func(*midx.Manifest) (bool, error)) error {
	for range manifestRetries {
		m, gen, err := s.loadManifest(ctx, repo)
		if err != nil {
			return err
		}
		changed, err := f(&m)
		if err != nil || !changed {
			return err
		}
		body, err := m.Encode()
		if err != nil {
			return err
		}
		if _, err := s.index.SwapManifest(ctx, repo.String(), gen, body); !errors.Is(err, meta.ErrManifestStale) {
			return err
		}
	}
	return fmt.Errorf("store: manifest of %s changes too fast", repo)
}

// appendPack adds the pack that putPack stored as the newest layer, and merges the small layers at the tail.
func (s *Service) appendPack(ctx context.Context, repo Repo, id string, bytes int64, idx []byte, graph []midx.Commit, sizes string) error {
	n, err := gitpack.IdxCount(idx[:min(len(idx), gitpack.IdxHeadBytes)])
	if err != nil {
		return err
	}
	e := indexEntry(id, bytes, n)
	e.Sizes = sizes
	if len(graph) > 0 {
		prefix, err := repoPrefix(repo)
		if err != nil {
			return err
		}
		if e, err = s.withGraph(ctx, repo, prefix, id, e, graph); err != nil {
			return err
		}
	}
	err = s.updateManifest(ctx, repo, func(m *midx.Manifest) (bool, error) {
		for i, l := range m.Layers {
			if l.IsIndex() && l.Packs[0].ID == id {
				graph, size := l.Graph == "" && e.Graph != "", l.Sizes == "" && e.Sizes != ""
				if !graph && !size {
					return false, nil
				}
				if graph {
					m.Layers[i].Graph, m.Layers[i].Commits, m.Layers[i].GraphSize = e.Graph, e.Commits, e.GraphSize
				}
				if size {
					m.Layers[i].Sizes = e.Sizes
				}
				return true, nil
			}
		}
		m.Layers = append(m.Layers, e)
		return true, nil
	})
	if err != nil {
		return err
	}
	s.mergeTail(ctx, repo)
	return nil
}
