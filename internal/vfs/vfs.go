//go:build linux

// Package vfs is the node-local lazy workspace: a git tree served over FUSE as the read-only lower layer of a
// kernel overlay.
package vfs

import (
	"context"
	"hash/fnv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

type VFS struct {
	cfg   Config
	c     *Cache
	mtime time.Time

	base     gitpack.SHA
	rootTree *Tree

	// upperFS reads the upper directory of the kernel overlay that mounts over this lower layer.
	upperFS upperdir.FS
	upper   string

	tel Telemetry
	clk clock.Clock
	statsState
	cancelStats context.CancelFunc

	statMu   sync.Mutex
	statMemo map[gitpack.SHA]subStats
}

// New opens the workspace of cfg.Base: it reads the commit and its root tree. It runs under the span vfs.open.
func New(ctx context.Context, cfg Config) (*VFS, error) {
	ctx, span := cfg.Telemetry.start(ctx, "vfs.open", attribute.String("commit", cfg.Base.String()))
	v, err := open(ctx, cfg)
	if err != nil {
		cfg.Telemetry.logError(ctx, "vfs.open failed", err)
	}
	finish(span, err)
	return v, err
}

func open(ctx context.Context, cfg Config) (*VFS, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	v := &VFS{cfg: cfg, c: cfg.Cache, statMemo: map[gitpack.SHA]subStats{}}
	v.tel, v.clk = cfg.Telemetry, clock.Or(cfg.Clock)
	v.published = v.clk.Now()
	if v.published.IsZero() {
		v.published = time.Now()
	}
	if v.c == nil {
		v.c = NewCache(cfg.Store, cfg.CacheDir)
	}
	v.c.Tel = cfg.Telemetry
	tree, when, err := cfg.Store.Commit(ctx, cfg.Base)
	if err != nil {
		return nil, err
	}
	v.mtime = when
	rt, err := v.c.Tree(ctx, tree)
	if err != nil {
		return nil, err
	}
	v.base, v.rootTree = tree, rt
	v.upper, v.upperFS = cfg.UpperDir, cfg.upperFS
	if v.upperFS == nil {
		v.upperFS = &upperdir.OSFS{Dir: cfg.UpperDir}
	}
	sctx, cancel := context.WithCancel(context.Background())
	v.cancelStats = cancel
	v.startStats(sctx)
	return v, nil
}

// Close writes the last vfs.stats record. The upper directory stays on disk.
func (v *VFS) Close() {
	v.cancelStats()
	v.emitStats(true)
}

// view is the state of one path in the base tree.
type view struct {
	be       Entry
	baseTree *Tree
	isDir    bool
	isLink   bool
}

func (w *view) kindMode() uint32 {
	switch {
	case w.isDir:
		return syscall.S_IFDIR
	case w.isLink:
		return syscall.S_IFLNK
	}
	return syscall.S_IFREG
}

// resolve walks p through the base tree.
func (v *VFS) resolve(ctx context.Context, p string) (view, syscall.Errno) {
	cur := view{isDir: true, baseTree: v.rootTree}
	if p == "" {
		return cur, 0
	}
	for _, name := range strings.Split(p, "/") {
		if !cur.isDir {
			return view{}, syscall.ENOTDIR
		}
		if cur.baseTree == nil {
			return view{}, syscall.ENOENT
		}
		e, ok := cur.baseTree.Lookup(name)
		if !ok {
			return view{}, syscall.ENOENT
		}
		nv := view{be: e, isDir: e.IsDir() || e.IsGitlink(), isLink: e.IsSymlink()}
		if e.IsDir() {
			t, err := v.c.Tree(ctx, e.SHA)
			if err != nil {
				return view{}, syscall.EIO
			}
			nv.baseTree = t
		}
		cur = nv
	}
	return cur, 0
}

type subStats struct {
	files int
	bytes int64
	ok    bool
}

// stats returns file count and bytes under a tree when every subtree is in memory.
func (v *VFS) stats(t *Tree) subStats {
	v.statMu.Lock()
	if st, ok := v.statMemo[t.SHA]; ok {
		v.statMu.Unlock()
		return st
	}
	v.statMu.Unlock()
	st := subStats{ok: true}
	for _, e := range t.Entries {
		switch {
		case e.IsDir():
			nt := v.c.CachedTree(e.SHA)
			if nt == nil {
				st.ok = false
				continue
			}
			c := v.stats(nt)
			st.files += c.files
			st.bytes += c.bytes
			st.ok = st.ok && c.ok
		case e.IsGitlink():
		default:
			st.files++
			st.bytes += e.Size
		}
	}
	if st.ok {
		v.statMu.Lock()
		v.statMemo[t.SHA] = st
		v.statMu.Unlock()
	}
	return st
}
func (v *VFS) walkFiles(t *Tree, yield func(Entry) bool) bool {
	for _, e := range t.Entries {
		switch {
		case e.IsDir():
			if nt := v.c.CachedTree(e.SHA); nt != nil {
				if !v.walkFiles(nt, yield) {
					return false
				}
			}
		case e.IsGitlink():
		default:
			if !yield(e) {
				return false
			}
		}
	}
	return true
}

// readAhead yields blobs worth fetching with the one at p: all files under the highest ancestor
// directory whose subtree fits one batch, else the entries that follow p in depth-first order.
func (v *VFS) readAhead(p string) func(yield func(Entry) bool) {
	return func(yield func(Entry) bool) {
		comps := strings.Split(p, "/")
		type frame struct {
			t   *Tree
			idx int
		}
		var stack []frame
		t := v.rootTree
		for i, c := range comps {
			var idx = -1
			for j := range t.Entries {
				if t.Entries[j].Name == c {
					idx = j
					break
				}
			}
			if idx < 0 {
				return
			}
			stack = append(stack, frame{t, idx})
			if i == len(comps)-1 {
				break
			}
			nt := v.c.CachedTree(t.Entries[idx].SHA)
			if nt == nil {
				return
			}
			t = nt
		}
		// candidate roots: the trees on the path, top-down (stack[0].t is the repository root).
		for _, f := range stack {
			st := v.stats(f.t)
			if st.ok && st.files <= v.c.PrefetchN && st.bytes <= v.c.PrefetchB {
				v.walkFiles(f.t, yield)
				return
			}
		}
		for len(stack) > 0 {
			f := &stack[len(stack)-1]
			f.idx++
			if f.idx >= len(f.t.Entries) {
				stack = stack[:len(stack)-1]
				continue
			}
			e := f.t.Entries[f.idx]
			if e.IsDir() {
				if nt := v.c.CachedTree(e.SHA); nt != nil {
					stack = append(stack, frame{nt, -1})
				}
				continue
			}
			if e.IsGitlink() {
				continue
			}
			if !yield(e) {
				return
			}
		}
	}
}

func inoOf(p string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(p))
	n := h.Sum64() &^ (1 << 63)
	if n < 16 {
		n += 16
	}
	return n
}
