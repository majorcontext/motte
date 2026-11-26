// Package store keeps the git objects of a repository in packs and the
// operation objects of a workspace, in one object store. HTTP, grants and the
// SQL index are in other packages.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"weak"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"

	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
	"github.com/majorcontext/motte/meta"
)

var (
	ErrNotFound = errors.New("store: object not found")
	ErrInvalid  = errors.New("store: invalid object name or content")
	ErrTooLarge = errors.New("store: object too large")
)

const (
	MaxOpBytes   = 8 << 20
	MaxPackBytes = 64 << 20
	MaxIdxBytes  = 8 << 20
	// MaxBlobBatchBytes bounds the blobs that one batch read returns.
	MaxBlobBatchBytes = 16 << 20
	// MaxBatch bounds the objects of one put.
	MaxBatch     = 100000
	maxTreeBytes = gitpack.MaxObjectBytes
	maxOpenBytes = 256 << 20
	// maxIndexBytes is the memory of the layer cache: layer directories, index layers and blocks of records.
	maxIndexBytes = 128 << 20
	// MemoryBudget is the bytes that requests may hold while they work, apart from the bodies that are still being read.
	MemoryBudget = 512 << 20
	// inlineBudget is the bytes that the tail merges of writes may hold, apart from MemoryBudget. A write that runs
	// while requests hold the whole of MemoryBudget still merges its tail.
	inlineBudget = 64 << 20
	// LandBudget is the bytes that the leaders of lands may hold, apart from MemoryBudget. A leader that waits behind
	// fetches holds up every land of its ref.
	LandBudget   = 128 << 20
	blockTimeout = 30 * time.Second
	// inFlightBytes bounds the bytes that the prefetches of one request have in flight. ReadAheadBytes adds what they
	// keep, the most that the read-ahead of a request holds.
	inFlightBytes  = 2 << 20
	ReadAheadBytes = inFlightBytes + aheadKeep
	// maxObjectCacheBytes is the memory of the cache of commits and trees.
	maxObjectCacheBytes = 64 << 20
	// maxCachedObject is the largest commit or tree that the cache keeps.
	maxCachedObject = 64 << 10
	// readWorkers bounds the concurrent bucket reads of one prefetch or one batched lookup.
	readWorkers = 16
	// treeAhead is the trees of a bulk tree read that one prefetch reads in advance.
	treeAhead = 256
	// sizeBatch is the blobs whose headers a bulk tree read prefetches together, and sizeWorkers the concurrent reads
	// of that prefetch. The headers of a batch must fit the read-ahead buffer of the request, aheadKeep, so the batch
	// stays well under it.
	sizeBatch   = 512
	sizeWorkers = 64
)

// Repo names a repository as <prefix>:<path>.
type Repo struct{ Prefix, Path string }

func (r Repo) String() string { return r.Prefix + ":" + r.Path }

// ParseRepo reads the text form. It rejects a prefix or a path that the
// object keys cannot carry.
func ParseRepo(s string) (Repo, error) {
	prefix, path, ok := strings.Cut(s, ":")
	if !ok || !forgeID.MatchString(prefix) || path == "" || len(path) > 512 || !utf8.ValidString(path) {
		return Repo{}, ErrInvalid
	}
	return Repo{Prefix: prefix, Path: path}, nil
}

// PackIndex is the metadata that the store keeps about the packs of a repo: the packs, and the manifest of layers.
type PackIndex interface {
	AddPack(ctx context.Context, repo, packID string, bytes int64) error
	Packs(ctx context.Context, repo string) ([]meta.Pack, error)
	RemovePack(ctx context.Context, repo, packID string) error
	RetireFiles(ctx context.Context, repo string, keys []string, at time.Time) error
	RetiredFiles(ctx context.Context, repo string) ([]meta.Retired, error)
	ForgetRetired(ctx context.Context, repo string, keys []string) error
	BeginDelete(ctx context.Context, repo string, keys []string, now, staleBefore, retiredBefore time.Time) ([]string, error)
	ClaimRetired(ctx context.Context, repo string, keys []string) (bool, error)
	Manifest(ctx context.Context, repo string) (meta.Manifest, error)
	SwapManifest(ctx context.Context, repo string, expect int64, body []byte) (int64, error)
}

type OpKind string

const (
	OpOperation OpKind = "operations"
	OpView      OpKind = "views"
)

var (
	opID    = regexp.MustCompile(`^[0-9a-f]{16,128}$`)
	packID  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	forgeID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	wsID    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// Service stores the objects of every repo and workspace in one object store.
type Service struct {
	st    bucket.Store
	index PackIndex
	clk   clock.Clock
	grace time.Duration

	budget *semaphore.Weighted
	inline *semaphore.Weighted
	landed *semaphore.Weighted

	icache *clockCache
	ocache *clockCache
	rcache *rangeCache
	// rflight bounds the bytes of the bucket reads that outlive the requests that began them.
	rflight    *semaphore.Weighted
	readBudget int64
	loads      singleflight.Group
	maint      sync.Map
	stats      maintStats

	mu        sync.Mutex
	open      map[string]*openPack
	order     []string
	openBytes int
	metaBytes int
	evicted   int

	hits, misses, idxBytes, rangeReads, rangeBytes atomic.Int64
	ihits, imisses                                 atomic.Int64

	slowAfter time.Duration
	// graphCap is the most commits that the graph of an imported pack holds.
	graphCap int
	slowAt   atomic.Int64
}

// CacheStats counts the open-pack cache, the layer cache and the bucket reads that fill them.
type CacheStats struct {
	Hits, Misses, Evictions int64
	OpenPacks, OpenBytes    int64
	IndexBytes              int64
	RangeReads, RangeBytes  int64
	IndexHits, IndexMisses  int64
	IndexEvictions          int64
	IndexResident           int64
	RangeCacheHits          int64
	RangeCacheMisses        int64
	RangeCacheBytes         int64
	RangeCacheEvictions     int64
}

// CacheStats returns the counters of the open-pack cache.
func (s *Service) CacheStats() CacheStats {
	s.mu.Lock()
	open, bytes, ev := len(s.open), s.openBytes, s.evicted
	s.mu.Unlock()
	resident, iev := s.icache.stats()
	rhits, rmisses, rused, revicted := s.rcache.stats()
	return CacheStats{
		Hits: s.hits.Load(), Misses: s.misses.Load(), Evictions: int64(ev), OpenPacks: int64(open), OpenBytes: int64(bytes),
		IndexBytes: s.idxBytes.Load(), RangeReads: s.rangeReads.Load(), RangeBytes: s.rangeBytes.Load(),
		IndexHits: s.ihits.Load(), IndexMisses: s.imisses.Load(), IndexEvictions: int64(iev), IndexResident: int64(resident),
		RangeCacheHits: rhits, RangeCacheMisses: rmisses, RangeCacheBytes: rused, RangeCacheEvictions: revicted,
	}
}

// Reserve takes bytes of MemoryBudget, at most the whole budget, and waits while the budget is spent. The caller
// releases it.
func (s *Service) Reserve(ctx context.Context, bytes int64) (release func(), err error) {
	n := min(bytes, MemoryBudget)
	if err := s.budget.Acquire(ctx, n); err != nil {
		return nil, err
	}
	return func() { s.budget.Release(n) }, nil
}

// ReserveLand takes bytes of LandBudget, at most the whole budget, and waits while the budget is spent. The caller
// releases it.
func (s *Service) ReserveLand(ctx context.Context, bytes int64) (release func(), err error) {
	n := min(bytes, LandBudget)
	if err := s.landed.Acquire(ctx, n); err != nil {
		return nil, err
	}
	return func() { s.landed.Release(n) }, nil
}

// Option tunes a Service.
type Option func(*Service)

// WithClock sets the clock that stamps retired files. It defaults to the real clock.
func WithClock(c clock.Clock) Option { return func(s *Service) { s.clk = c } }

// WithGrace sets how long a retired file stays before Maintain deletes it. A request that began within the grace
// reads a consistent set. A value of zero or less keeps DefaultGrace.
func WithGrace(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.grace = d
		}
	}
}

const (
	// DefaultReadBudget is the bytes that the shared bucket reads of all requests may hold at once.
	DefaultReadBudget = 48 << 20
	// MinReadBudget is the least budget: a range that a prefetch plans is at most 1 MiB, and a read reserves three times
	// its length.
	MinReadBudget = 4 << 20
)

// WithReadBudget sets the bytes that the shared bucket reads of all requests may hold at once, at least MinReadBudget.
// It defaults to DefaultReadBudget.
func WithReadBudget(bytes int64) Option {
	return func(s *Service) {
		n := max(bytes, MinReadBudget)
		s.rflight, s.readBudget = semaphore.NewWeighted(n), n
	}
}

// WithRangeCache sets the memory of the cache of pack ranges that requests share. A value of zero or less turns the
// cache off. It defaults to DefaultRangeCacheBytes.
func WithRangeCache(bytes int64) Option { return func(s *Service) { s.rcache = newRangeCache(bytes) } }

// tryReserveInline takes bytes of inlineBudget and returns nil at once when it lacks them.
func (s *Service) tryReserveInline(bytes int64) (release func()) {
	n := min(bytes, inlineBudget)
	if !s.inline.TryAcquire(n) {
		return nil
	}
	return func() { s.inline.Release(n) }
}

func New(st bucket.Store, index PackIndex, opts ...Option) *Service {
	s := &Service{st: st, index: index, clk: clock.Real(), grace: DefaultGrace, open: map[string]*openPack{}, budget: semaphore.NewWeighted(MemoryBudget), inline: semaphore.NewWeighted(inlineBudget), landed: semaphore.NewWeighted(LandBudget), icache: newClockCache(maxIndexBytes), ocache: newClockCache(maxObjectCacheBytes), rcache: newRangeCache(DefaultRangeCacheBytes), rflight: semaphore.NewWeighted(DefaultReadBudget), readBudget: DefaultReadBudget, slowAfter: slowReadAfter, graphCap: midx.MaxGraphCommits}
	for _, o := range opts {
		o(s)
	}
	return s
}

func mapNotFound(err error) error {
	if errors.Is(err, bucket.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

// ValidWorkspaceID reports whether id can name a workspace.
func ValidWorkspaceID(id string) bool { return wsID.MatchString(id) }

func opKey(ws string, kind OpKind, id string) (string, error) {
	if !wsID.MatchString(ws) || (kind != OpOperation && kind != OpView) || !opID.MatchString(id) {
		return "", ErrInvalid
	}
	return "ops/" + ws + "/" + string(kind) + "/" + id, nil
}

func repoPrefix(r Repo) (string, error) {
	if !forgeID.MatchString(r.Prefix) || r.Path == "" {
		return "", ErrInvalid
	}
	return "repos/" + r.Prefix + "/" + base64.RawURLEncoding.EncodeToString([]byte(r.Path)), nil
}

func (s *Service) PutOp(ctx context.Context, ws string, kind OpKind, id string, data []byte) error {
	key, err := opKey(ws, kind, id)
	if err != nil {
		return err
	}
	if len(data) > MaxOpBytes {
		return ErrTooLarge
	}
	_, err = s.st.Create(ctx, key, data)
	return err
}

func (s *Service) GetOp(ctx context.Context, ws string, kind OpKind, id string) ([]byte, error) {
	key, err := opKey(ws, kind, id)
	if err != nil {
		return nil, err
	}
	data, err := s.st.Get(ctx, key)
	return data, mapNotFound(err)
}

// PutObjects stores objects as one pack and returns their names in order.
func (s *Service) PutObjects(ctx context.Context, repo Repo, objs []gitpack.Object) ([]gitpack.SHA, error) {
	if len(objs) == 0 || len(objs) > MaxBatch {
		return nil, ErrInvalid
	}
	size := 0
	for _, o := range objs {
		if o.Type < gitpack.Commit || o.Type > gitpack.Tag {
			return nil, ErrInvalid
		}
		if len(o.Data) > gitpack.MaxObjectBytes {
			return nil, ErrTooLarge
		}
		size += len(o.Data)
	}
	if size > MaxPackBytes {
		return nil, ErrTooLarge
	}
	pack, idx, names := gitpack.Build(objs)
	sum := sha256.Sum256(pack)
	if err := s.putPack(ctx, repo, hex.EncodeToString(sum[:]), idx, pack, nil, objectSizes(objs, names)); err != nil {
		return nil, err
	}
	s.keepObjects(repo, objs, names)
	return names, nil
}

// ImportPack stores a pack and its v2 index under the SHA-256 of the pack.
func (s *Service) ImportPack(ctx context.Context, repo Repo, id string, idx, pack []byte) error {
	sum := sha256.Sum256(pack)
	if !packID.MatchString(id) || id != hex.EncodeToString(sum[:]) || len(pack) == 0 {
		return ErrInvalid
	}
	if len(pack) > MaxPackBytes || len(idx) > MaxPackBytes {
		return ErrTooLarge
	}
	if len(idx) > MaxIdxBytes {
		return ErrTooLarge
	}
	p, err := gitpack.NewPack(idx, bytesReader(pack))
	if err != nil {
		return ErrInvalid
	}
	if err := p.Verify(ctx, idx, pack); err != nil {
		if errors.Is(err, gitpack.ErrCorrupt) {
			return ErrInvalid
		}
		if errors.Is(err, gitpack.ErrTooLarge) {
			return ErrTooLarge
		}
		return err
	}
	return s.putPack(ctx, repo, id, idx, pack, s.graphOfPack(ctx, repo, p), packSizes(p))
}

// ReceivePack stores the objects of a pushed pack as one pack of the repo. The pack may be thin: the service
// resolves the base of a ref delta from the repo, so the stored pack holds whole objects. A non-nil held is called
// with the inflated bytes that the unpack holds each time that count grows.
func (s *Service) ReceivePack(ctx context.Context, repo Repo, pack []byte, held func(int64) error) error {
	v, err := s.view(ctx, repo)
	if err != nil {
		return err
	}
	var last int64
	track := held
	if held != nil {
		track = func(n int64) error { last = n; return held(n) }
	}
	external := func(sha gitpack.SHA) (gitpack.Type, []byte, error) {
		return v.getSized(ctx, sha, func(size int64) error {
			if held == nil {
				return nil
			}
			return held(last + size)
		})
	}
	objs, err := gitpack.UnpackHeld(ctx, pack, external, track)
	switch {
	case errors.Is(err, gitpack.ErrCorrupt), errors.Is(err, ErrNotFound):
		return ErrInvalid
	case errors.Is(err, gitpack.ErrTooLarge):
		return ErrTooLarge
	case err != nil:
		return err
	case len(objs) == 0:
		return nil
	}
	built, idx, names := gitpack.Build(objs)
	sum := sha256.Sum256(built)
	if err := s.putPack(ctx, repo, hex.EncodeToString(sum[:]), idx, built, nil, objectSizes(objs, names)); err != nil {
		return err
	}
	s.keepObjects(repo, objs, names)
	return nil
}

func (s *Service) putPack(ctx context.Context, repo Repo, id string, idx, pack []byte, graph []midx.Commit, sizes []uint32) error {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return err
	}
	zfile := midx.BuildSizes(sizes)
	zkey := sizeKey(zfile)
	claims := []string{id + ".pack", id + ".idx"}
	if zkey != "" {
		claims = append(claims, zkey)
	}
	if err := s.claim(ctx, repo, claims...); err != nil {
		return err
	}
	var g errgroup.Group
	if zkey != "" {
		g.Go(func() error { return s.storeSizes(ctx, prefix, zkey, zfile, len(sizes)) })
	}
	g.Go(func() error { _, err := s.st.Create(ctx, prefix+"/"+id+".idx", idx); return err })
	g.Go(func() error { _, err := s.st.Create(ctx, prefix+"/"+id+".pack", pack); return err })
	if err := g.Wait(); err != nil {
		return err
	}
	if err := s.index.AddPack(ctx, repo.String(), id, int64(len(pack))); err != nil {
		return err
	}
	return s.appendPack(ctx, repo, id, int64(len(pack)), idx, graph, zkey)
}

type bytesReader []byte

func (b bytesReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (b bytesReader) Size() int64 { return int64(len(b)) }

// openPack is a pack kept across requests. A request reads it through
// Bind, so the reads carry the context of the request.
type openPack struct {
	p *gitpack.Pack
	b *packReader
	// charged and cached are guarded by Service.mu. charged is what the pack
	// adds to Service.openBytes while cached is true.
	charged int
	cached  bool
	meta    bool
}

// recharge sets the charge of e to its index plus its cached blocks and
// evicts the oldest packs while the cache is over budget.
func (s *Service) recharge(e *openPack) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !e.cached {
		return
	}
	n := e.p.Bytes() + e.p.CachedBytes() + e.b.cachedBytes()
	s.openBytes += n - e.charged
	if e.meta {
		s.metaBytes += n - e.charged
	}
	e.charged = n
	for len(s.order) > 1 && s.openBytes > maxOpenBytes {
		s.evict()
	}
}

func (s *Service) evict() {
	at := 0
	if s.metaBytes <= maxOpenBytes/4*3 {
		for i, k := range s.order {
			if !s.open[k].meta {
				at = i
				break
			}
		}
	}
	old := s.open[s.order[at]]
	s.openBytes -= old.charged
	if old.meta {
		s.metaBytes -= old.charged
	}
	old.cached = false
	s.evicted++
	delete(s.open, s.order[at])
	s.order = append(s.order[:at], s.order[at+1:]...)
}

func (s *Service) openPack(prefix string, ref midx.Pack, meta bool) *openPack {
	key := prefix + "|" + ref.ID
	s.mu.Lock()
	if e, ok := s.open[key]; ok {
		s.mu.Unlock()
		s.hits.Add(1)
		return e
	}
	s.mu.Unlock()
	s.misses.Add(1)
	br := &packReader{svc: s, st: s.st, key: prefix + "/" + ref.ID + ".pack", size: ref.Bytes}
	e := &openPack{meta: meta, b: br, p: gitpack.NewAt(reqReader{b: br, ctx: context.Background()})}
	br.changed = func() { s.recharge(e) }
	e.p.OnCacheChange(func() { s.recharge(e) })
	s.mu.Lock()
	if dup, ok := s.open[key]; ok {
		s.mu.Unlock()
		return dup
	}
	s.open[key] = e
	s.order = append(s.order, key)
	e.cached = true
	s.mu.Unlock()
	s.recharge(e)
	return e
}

// Warm loads the layers of repos, newest layer first, with at most parallel reads at a time. It stops loading when the
// layer cache first evicts, because a layer that it loads then only evicts another. It returns the first error.
func (s *Service) Warm(ctx context.Context, repos []Repo, parallel int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	start := s.icache.evictions()
	for _, repo := range repos {
		if ctx.Err() != nil || s.icache.evictions() != start {
			break
		}
		v, err := s.view(ctx, repo)
		if err != nil {
			_ = g.Wait()
			return err
		}
		for i := len(v.layers) - 1; i >= 0 && ctx.Err() == nil && s.icache.evictions() == start; i-- {
			e := v.layers[i]
			g.Go(func() error { _, err := s.layer(ctx, v.prefix, e); return err })
		}
	}
	return g.Wait()
}

// view is the layers of one repo as of one request.
type view struct {
	s      *Service
	prefix string
	layers []midx.Entry
	// handles holds weak pointers to the layers that the request has probed, so a probe takes no lock after the first.
	handles []atomic.Pointer[weak.Pointer[layerH]]
	// graphs holds weak pointers to the graphs that the request has probed, for the same reason.
	graphs []atomic.Pointer[weak.Pointer[midx.Graph]]
	// noGraph holds the keys of the graphs that this request found missing or corrupt, so a walk does not load one again
	// for each commit.
	noGraph sync.Map
	// lerr is the last failed read of a delta base, which the pack reports as a missing name.
	lerr atomic.Pointer[error]
	// ahead holds what the prefetches of the request read.
	ahead *aheadBuf
	// before is a time before the view read the manifest, and after a time after it. A write that ended before
	// before is in the view, and a view of a later before holds what this view read.
	before, after time.Time
}

// missing is the error for a name that a pack lacks: the failed read of a delta base when there was one.
func (v *view) missing() error {
	if e := v.lerr.Load(); e != nil {
		return *e
	}
	return ErrNotFound
}

func (s *Service) view(ctx context.Context, repo Repo) (*view, error) {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return nil, err
	}
	before := time.Now()
	m, _, err := s.loadManifest(ctx, repo)
	if err != nil {
		return nil, err
	}
	return &view{s: s, prefix: prefix, layers: m.Layers, handles: make([]atomic.Pointer[weak.Pointer[layerH]], len(m.Layers)), graphs: make([]atomic.Pointer[weak.Pointer[midx.Graph]], len(m.Layers)), ahead: &aheadBuf{}, before: before, after: time.Now()}, nil
}

// locate returns the pack that holds sha, probing the newest layer first, with the layer and the number of the pack in
// it. A layer costs at most one block read.
func (v *view) locate(ctx context.Context, sha gitpack.SHA) (*openPack, *layerH, int, int64, error) {
	st := statsFrom(ctx)
	for i := len(v.layers) - 1; i >= 0; i-- {
		st.layer()
		h, err := v.handle(ctx, i)
		if err != nil {
			return nil, nil, 0, 0, err
		}
		ord, off, ok, err := v.s.lookup(ctx, h, sha, -1)
		if err != nil {
			return nil, nil, 0, 0, err
		}
		if ok {
			return v.s.openPack(v.prefix, h.packs[ord], h.entry.Class == midx.ClassMeta), h, ord, off, nil
		}
	}
	return nil, nil, 0, 0, ErrNotFound
}

func (v *view) find(ctx context.Context, sha gitpack.SHA) (*gitpack.Pack, error) {
	e, h, ord, _, err := v.locate(ctx, sha)
	if err != nil {
		return nil, err
	}
	return e.p.Bind(reqReader{b: e.b, ctx: ctx, ahead: v.ahead}).WithLookup(func(name gitpack.SHA) (int64, bool) {
		_, off, ok, err := v.s.lookup(ctx, h, name, ord)
		if err != nil {
			v.lerr.Store(&err)
		}
		return off, ok && err == nil
	}), nil
}

// size is the inflated size of sha, from the pack header.
func (v *view) size(ctx context.Context, sha gitpack.SHA) (int64, error) {
	p, err := v.find(ctx, sha)
	if err != nil {
		return 0, err
	}
	n, _, err := p.Size(sha)
	if errors.Is(err, gitpack.ErrNotFound) {
		return 0, v.missing()
	}
	return n, err
}

func (v *view) get(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	return v.getSized(ctx, sha, nil)
}

// getSized is get that tells before the size of the object, from the pack header, so a caller can reserve memory
// before the object is inflated.
func (v *view) getSized(ctx context.Context, sha gitpack.SHA, before func(size int64) error) (gitpack.Type, []byte, error) {
	if t, d, ok := v.s.cached(v.prefix, sha, v.before); ok {
		if before != nil {
			if err := before(int64(len(d))); err != nil {
				return 0, nil, err
			}
		}
		return t, d, nil
	}
	p, err := v.find(ctx, sha)
	if err != nil {
		return 0, nil, err
	}
	if before != nil {
		n, err := p.Footprint(sha)
		if errors.Is(err, gitpack.ErrNotFound) {
			return 0, nil, v.missing()
		}
		if err != nil {
			return 0, nil, err
		}
		if err := before(n); err != nil {
			return 0, nil, err
		}
	}
	t, d, depth, err := p.GetDepth(sha)
	statsFrom(ctx).object(len(d), depth)
	if err == nil {
		v.s.keepObject(v.prefix, sha, t, d, v.after)
	}
	switch {
	case errors.Is(err, gitpack.ErrNotFound):
		return 0, nil, v.missing()
	case errors.Is(err, gitpack.ErrTooLarge):
		return 0, nil, ErrTooLarge
	case errors.Is(err, gitpack.ErrCorrupt):
		return 0, nil, ErrInvalid
	}
	return t, d, err
}

type objKey struct {
	prefix string
	sha    gitpack.SHA
}

type cachedObject struct {
	t    gitpack.Type
	data []byte
	at   time.Time
}

// cached returns a commit or tree that an earlier read or write kept, when a view that read its manifest at before
// holds it: the cache keeps the time after which every such view does. The caller never changes the data.
func (s *Service) cached(prefix string, sha gitpack.SHA, before time.Time) (gitpack.Type, []byte, bool) {
	o, ok := s.ocache.get(objKey{prefix, sha})
	if !ok {
		return 0, nil, false
	}
	c := o.(cachedObject)
	if c.at.After(before) {
		return 0, nil, false
	}
	return c.t, c.data, true
}

func (s *Service) keepObject(prefix string, sha gitpack.SHA, t gitpack.Type, data []byte, at time.Time) {
	if (t == gitpack.Commit || t == gitpack.Tree) && len(data) <= maxCachedObject {
		s.ocache.put(objKey{prefix, sha}, cachedObject{t, data, at}, len(data)+96)
	}
}

func (s *Service) keepObjects(repo Repo, objs []gitpack.Object, names []gitpack.SHA) {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return
	}
	at := time.Now()
	for i, o := range objs {
		s.keepObject(prefix, names[i], o.Type, o.Data, at)
	}
}

func (s *Service) Get(ctx context.Context, repo Repo, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return 0, nil, err
	}
	return v.get(ctx, sha)
}

// Reader reads the objects of one repo against the pack list of the moment that Reader was called, so a request
// that reads many objects lists the packs once.
type Reader struct {
	v      *view
	flight *semaphore.Weighted
}

func newReader(v *view) *Reader {
	return &Reader{v: v, flight: semaphore.NewWeighted(inFlightBytes)}
}

func (s *Service) Reader(ctx context.Context, repo Repo) (*Reader, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	return newReader(v), nil
}

// Packs is the number of packs that the layers of the reader list.
func (r *Reader) Packs() int {
	n := 0
	for _, l := range r.v.layers {
		n += l.NPacks
	}
	return n
}

func (r *Reader) Get(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	return r.v.get(ctx, sha)
}

// GetSized is Get that calls before with the size of the object before it reads the object. An error of before stops
// the read.
func (r *Reader) GetSized(ctx context.Context, sha gitpack.SHA, before func(size int64) error) (gitpack.Type, []byte, error) {
	return r.v.getSized(ctx, sha, before)
}

// Prefetch reads the pack bytes that Get of shas will need, in few large ranges that it reads at most readWorkers at
// a time. It skips what it cannot place, and a failed read is left to the Get that needs it.
func (r *Reader) Prefetch(ctx context.Context, shas []gitpack.SHA) {
	r.prefetch(ctx, shas, true, readWorkers)
}

// PrefetchHeaders is Prefetch for a caller that reads only the headers of the objects, as the sizes of the blobs of a
// tree: it keeps the first bytes of each object, not the whole object.
func (r *Reader) PrefetchHeaders(ctx context.Context, shas []gitpack.SHA) {
	r.prefetch(ctx, shas, false, readWorkers)
}

func (r *Reader) prefetch(ctx context.Context, shas []gitpack.SHA, whole bool, workers int) {
	sorted := slices.Clone(shas)
	slices.SortFunc(sorted, func(a, b gitpack.SHA) int { return bytes.Compare(a[:], b[:]) })
	sorted = slices.Compact(sorted)
	sorted = slices.DeleteFunc(sorted, func(sha gitpack.SHA) bool { _, _, ok := r.v.s.cached(r.v.prefix, sha, r.v.before); return ok })
	byPack, _ := r.v.place(ctx, sorted)
	var g errgroup.Group
	g.SetLimit(workers)
	for e, offs := range byPack {
		for _, run := range e.b.plan(offs, r.v.ahead) {
			if r.flight.Acquire(ctx, run.end-run.start) != nil {
				break
			}
			release := sync.OnceFunc(func() { r.flight.Release(run.end - run.start) })
			g.Go(func() error {
				defer release()
				_ = e.b.fetchRun(ctx, r.v.ahead, run, whole, release, r.flight)
				return nil
			})
		}
	}
	_ = g.Wait()
}

// Has reports for each sha whether the repo holds it, from the pack indexes only.
func (r *Reader) Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error) {
	out := make([]bool, len(shas))
	for i, sha := range shas {
		_, err := r.v.find(ctx, sha)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		out[i] = err == nil
	}
	return out, nil
}

// Blobs returns the blobs of shas. Before it reads a blob, it calls grow, when it is set, with the bytes of the blobs
// read so far and the size that this one holds while it is read, taken from the pack.
func (s *Service) Blobs(ctx context.Context, repo Repo, shas []gitpack.SHA, grow func(size int64) error) ([][]byte, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(shas))
	total := 0
	for i, sha := range shas {
		t, d, err := v.getSized(ctx, sha, func(n int64) error {
			if grow == nil {
				return nil
			}
			return grow(int64(total) + n)
		})
		if err != nil {
			return nil, err
		}
		if t != gitpack.Blob {
			return nil, ErrInvalid
		}
		if total += len(d); total > MaxBlobBatchBytes {
			return nil, ErrTooLarge
		}
		out[i] = d
	}
	return out, nil
}

// Has reports for each sha whether a pack of the repo holds it. It reads the pack indexes only.
func (s *Service) Has(ctx context.Context, repo Repo, shas []gitpack.SHA) ([]bool, error) {
	r, err := s.Reader(ctx, repo)
	if err != nil {
		return nil, err
	}
	return r.Has(ctx, shas)
}

// CommitInfo is the root tree, the parents and the committer time of a commit.
type CommitInfo struct {
	Tree    gitpack.SHA
	Parents []gitpack.SHA
	Time    time.Time
}

// Commit returns the tree, the parents and the committer time of a commit. It calls grow, when it is set, with the
// size of the commit before it reads it.
func (s *Service) Commit(ctx context.Context, repo Repo, sha gitpack.SHA, grow func(size int64) error) (CommitInfo, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return CommitInfo{}, err
	}
	return v.commit(ctx, sha, grow)
}

func (v *view) commit(ctx context.Context, sha gitpack.SHA, grow func(size int64) error) (CommitInfo, error) {
	t, d, err := v.getSized(ctx, sha, grow)
	if err != nil {
		return CommitInfo{}, err
	}
	if t != gitpack.Commit || !bytes.HasPrefix(d, []byte("tree ")) || len(d) < 45 {
		return CommitInfo{}, ErrInvalid
	}
	tree, err := gitpack.ParseSHA(string(d[5:45]))
	if err != nil {
		return CommitInfo{}, ErrInvalid
	}
	info := CommitInfo{Tree: tree}
	head, _, _ := bytes.Cut(d, []byte("\n\n"))
	for _, line := range bytes.Split(head, []byte("\n"))[1:] {
		rest, ok := bytes.CutPrefix(line, []byte("parent "))
		if !ok {
			break
		}
		p, err := gitpack.ParseSHA(string(rest))
		if err != nil {
			return CommitInfo{}, ErrInvalid
		}
		info.Parents = append(info.Parents, p)
	}
	var sec int64
	if i := bytes.Index(d, []byte("\ncommitter ")); i >= 0 {
		line := d[i+1:]
		if j := bytes.IndexByte(line, '\n'); j >= 0 {
			line = line[:j]
		}
		if j := bytes.LastIndexByte(line, '>'); j >= 0 {
			_, _ = fmt.Sscanf(string(line[j+1:]), "%d", &sec)
		}
	}
	info.Time = time.Unix(sec, 0).UTC()
	return info, nil
}

// ErrNoPath is the failure of File for a path that names no file of the commit.
var ErrNoPath = errors.New("store: no such file")

// File returns the content of the file at path in the tree of a commit. It fails with ErrNoPath when a segment is
// missing or the path does not end at a file, and with ErrTooLarge when the file has more than max bytes. It calls
// grow with the bytes that it holds before it reads each object.
func (s *Service) File(ctx context.Context, repo Repo, commit gitpack.SHA, path string, max int64, grow func(size int64) error) ([]byte, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	info, err := v.commit(ctx, commit, grow)
	if err != nil {
		return nil, err
	}
	cur := info.Tree
	segs := strings.Split(path, "/")
	for i, seg := range segs {
		t, d, err := v.getSized(ctx, cur, grow)
		if err != nil {
			return nil, err
		}
		if t != gitpack.Tree || len(d) > maxTreeBytes {
			return nil, ErrInvalid
		}
		tree, err := parseEntries(ctx, cur, d)
		if err != nil {
			return nil, err
		}
		j := slices.IndexFunc(tree.Entries, func(e Entry) bool { return e.Name == seg })
		if j < 0 {
			return nil, ErrNoPath
		}
		e := tree.Entries[j]
		if last := i == len(segs)-1; last && !isBlobEntry(e) || !last && e.Mode != 0o40000 {
			return nil, ErrNoPath
		}
		cur = e.SHA
	}
	size, err := v.size(ctx, cur)
	if err != nil {
		return nil, err
	}
	if size > max {
		return nil, ErrTooLarge
	}
	_, data, err := v.getSized(ctx, cur, grow)
	return data, err
}

type Entry struct {
	Name string
	Mode uint32
	SHA  gitpack.SHA
	Size int64
}

type Tree struct {
	SHA     gitpack.SHA
	Entries []Entry
}

// TreeBulk returns a tree and its subtrees breadth first, until the entries
// returned reach maxEntries. Blob entries carry their size. Before it reads a
// tree, it calls grow with the tree bytes of the trees read so far and this
// one, taken from the pack index. It reads the trees of a batch together, and
// then the headers of their blobs in groups, so a bulk read costs a few rounds
// of reads, not one for each tree.
func (s *Service) TreeBulk(ctx context.Context, repo Repo, sha gitpack.SHA, maxEntries int, grow func(treeBytes int64) error) (out []Tree, err error) {
	ctx, end := StartSpan(ctx, "vcs.treebulk", attribute.Int("max_entries", maxEntries))
	var batches int
	var fill time.Duration
	defer func() {
		entries := 0
		for _, t := range out {
			entries += len(t.Entries)
		}
		end(err, attribute.Int("trees", len(out)), attribute.Int("entries", entries), attribute.Int("batches", batches), attribute.Float64("fill_sizes_ms", float64(fill)/1e6))
	}()
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	queue := []gitpack.SHA{sha}
	total, held := 0, int64(0)
	rd := newReader(v)
	for k := 0; k < len(queue) && (len(out) == 0 || total < maxEntries); {
		end := min(k+treeAhead, len(queue))
		rd.Prefetch(ctx, queue[k:end])
		first := len(out)
		for ; k < end && (len(out) == 0 || total < maxEntries); k++ {
			cur := queue[k]
			size, err := v.size(ctx, cur)
			if err != nil {
				return nil, err
			}
			held += size
			if err := grow(held); err != nil {
				return nil, err
			}
			t, d, err := v.get(ctx, cur)
			if err != nil {
				return nil, err
			}
			if t != gitpack.Tree {
				return nil, ErrInvalid
			}
			if len(d) > maxTreeBytes {
				return nil, ErrInvalid
			}
			tree, err := parseEntries(ctx, cur, d)
			if err != nil {
				return nil, err
			}
			out = append(out, tree)
			total += len(tree.Entries)
			for _, e := range tree.Entries {
				if e.Mode == 0o40000 {
					queue = append(queue, e.SHA)
				}
			}
		}
		t0 := s.clk.Now()
		err := v.fillSizes(ctx, out[first:])
		fill += s.clk.Now().Sub(t0)
		batches++
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func isBlobEntry(e Entry) bool { return e.Mode != 0o40000 && e.Mode != 0o160000 }

// parseEntries decodes the entries of a tree. It leaves the size of each blob entry at 0.
func parseEntries(ctx context.Context, sha gitpack.SHA, data []byte) (Tree, error) {
	tree := Tree{SHA: sha}
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return Tree{}, err
		}
		sp := bytes.IndexByte(data, ' ')
		nul := bytes.IndexByte(data, 0)
		if sp < 0 || nul < sp || len(data) < nul+21 {
			return Tree{}, ErrInvalid
		}
		var mode uint32
		if _, err := fmt.Sscanf(string(data[:sp]), "%o", &mode); err != nil {
			return Tree{}, ErrInvalid
		}
		e := Entry{Name: string(data[sp+1 : nul]), Mode: mode}
		copy(e.SHA[:], data[nul+1:nul+21])
		tree.Entries = append(tree.Entries, e)
		data = data[nul+21:]
	}
	return tree, nil
}

// fillSizes sets the size of each blob entry of trees. It takes the sizes that the size files of the layers hold, and
// reads the headers of the other blobs sizeBatch at a time, with sizeWorkers reads at once, so the headers of a group stay in the read-ahead buffer until they are used.
func (v *view) fillSizes(ctx context.Context, trees []Tree) error {
	var blobs []gitpack.SHA
	for _, t := range trees {
		for _, e := range t.Entries {
			if isBlobEntry(e) {
				blobs = append(blobs, e.SHA)
			}
		}
	}
	sorted := slices.Clone(blobs)
	slices.SortFunc(sorted, func(a, b gitpack.SHA) int { return bytes.Compare(a[:], b[:]) })
	sorted = slices.Compact(sorted)
	sizes := make(map[gitpack.SHA]int64, len(sorted))
	for from := 0; from < len(sorted); from += sizeBatch {
		maps.Copy(sizes, v.indexedSizes(ctx, sorted[from:min(from+sizeBatch, len(sorted))]))
	}
	blobs = slices.DeleteFunc(blobs, func(sha gitpack.SHA) bool { _, ok := sizes[sha]; return ok })
	rd := newReader(v)
	for from := 0; from < len(blobs); from += sizeBatch {
		group := blobs[from:min(from+sizeBatch, len(blobs))]
		rd.prefetch(ctx, group, false, sizeWorkers)
		for _, sha := range group {
			if _, ok := sizes[sha]; ok {
				continue
			}
			p, err := v.find(ctx, sha)
			if err != nil {
				return err
			}
			n, _, err := p.Size(sha)
			if err != nil {
				return ErrNotFound
			}
			sizes[sha] = n
		}
	}
	for _, t := range trees {
		for i := range t.Entries {
			if e := &t.Entries[i]; isBlobEntry(*e) {
				e.Size = sizes[e.SHA]
			}
		}
	}
	return nil
}
