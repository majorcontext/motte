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
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

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
	blockTimeout = 30 * time.Second
	// readWorkers bounds the concurrent bucket reads of one prefetch or one batched lookup.
	readWorkers = 16
	// treeAhead is the trees of a bulk tree read that one prefetch reads in advance.
	treeAhead = 256
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

	icache *lru
	loads  singleflight.Group
	maint  sync.Map
	stats  maintStats

	mu        sync.Mutex
	open      map[string]*openPack
	order     []string
	openBytes int
	metaBytes int
	evicted   int

	hits, misses, idxBytes, rangeReads, rangeBytes atomic.Int64
	ihits, imisses                                 atomic.Int64
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
}

// CacheStats returns the counters of the open-pack cache.
func (s *Service) CacheStats() CacheStats {
	s.mu.Lock()
	open, bytes, ev := len(s.open), s.openBytes, s.evicted
	s.mu.Unlock()
	resident, iev := s.icache.stats()
	return CacheStats{
		Hits: s.hits.Load(), Misses: s.misses.Load(), Evictions: int64(ev), OpenPacks: int64(open), OpenBytes: int64(bytes),
		IndexBytes: s.idxBytes.Load(), RangeReads: s.rangeReads.Load(), RangeBytes: s.rangeBytes.Load(),
		IndexHits: s.ihits.Load(), IndexMisses: s.imisses.Load(), IndexEvictions: int64(iev), IndexResident: int64(resident),
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

// tryReserveInline takes bytes of inlineBudget and returns nil at once when it lacks them.
func (s *Service) tryReserveInline(bytes int64) (release func()) {
	n := min(bytes, inlineBudget)
	if !s.inline.TryAcquire(n) {
		return nil
	}
	return func() { s.inline.Release(n) }
}

func New(st bucket.Store, index PackIndex, opts ...Option) *Service {
	s := &Service{st: st, index: index, clk: clock.Real(), grace: DefaultGrace, open: map[string]*openPack{}, budget: semaphore.NewWeighted(MemoryBudget), inline: semaphore.NewWeighted(inlineBudget), icache: newLRU(maxIndexBytes)}
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
	if err := s.putPack(ctx, repo, hex.EncodeToString(sum[:]), idx, pack); err != nil {
		return nil, err
	}
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
	return s.putPack(ctx, repo, id, idx, pack)
}

// ReceivePack stores the objects of a pushed pack as one pack of the repo. The pack may be thin: the service
// resolves the base of a ref delta from the repo, so the stored pack holds whole objects. A non-nil held is called
// with the inflated bytes that the unpack holds each time that count grows.
func (s *Service) ReceivePack(ctx context.Context, repo Repo, pack []byte, held func(int64) error) error {
	v, err := s.view(ctx, repo)
	if err != nil {
		return err
	}
	objs, err := gitpack.UnpackHeld(ctx, pack, func(sha gitpack.SHA) (gitpack.Type, []byte, error) { return v.get(ctx, sha) }, held)
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
	built, idx, _ := gitpack.Build(objs)
	sum := sha256.Sum256(built)
	return s.putPack(ctx, repo, hex.EncodeToString(sum[:]), idx, built)
}

func (s *Service) putPack(ctx context.Context, repo Repo, id string, idx, pack []byte) error {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return err
	}
	if err := s.claim(ctx, repo, id+".pack", id+".idx"); err != nil {
		return err
	}
	var g errgroup.Group
	g.Go(func() error { _, err := s.st.Create(ctx, prefix+"/"+id+".idx", idx); return err })
	g.Go(func() error { _, err := s.st.Create(ctx, prefix+"/"+id+".pack", pack); return err })
	if err := g.Wait(); err != nil {
		return err
	}
	if err := s.index.AddPack(ctx, repo.String(), id, int64(len(pack))); err != nil {
		return err
	}
	return s.appendPack(ctx, repo, id, int64(len(pack)), idx)
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
	// lerr is the last failed read of a delta base, which the pack reports as a missing name.
	lerr atomic.Pointer[error]
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
	m, _, err := s.loadManifest(ctx, repo)
	if err != nil {
		return nil, err
	}
	return &view{s: s, prefix: prefix, layers: m.Layers}, nil
}

// locate returns the pack that holds sha, probing the newest layer first, with the layer and the number of the pack in
// it. A layer costs at most one block read.
func (v *view) locate(ctx context.Context, sha gitpack.SHA) (*openPack, *layerH, int, int64, error) {
	st := statsFrom(ctx)
	for i := len(v.layers) - 1; i >= 0; i-- {
		st.layer()
		h, err := v.s.layer(ctx, v.prefix, v.layers[i])
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
	return e.p.Bind(reqReader{b: e.b, ctx: ctx}).WithLookup(func(name gitpack.SHA) (int64, bool) {
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

func (s *Service) Get(ctx context.Context, repo Repo, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return 0, nil, err
	}
	return v.get(ctx, sha)
}

// Reader reads the objects of one repo against the pack list of the moment that Reader was called, so a request
// that reads many objects lists the packs once.
type Reader struct{ v *view }

func (s *Service) Reader(ctx context.Context, repo Repo) (*Reader, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	return &Reader{v}, nil
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
	sorted := slices.Clone(shas)
	slices.SortFunc(sorted, func(a, b gitpack.SHA) int { return bytes.Compare(a[:], b[:]) })
	sorted = slices.Compact(sorted)
	byPack, _ := r.v.place(ctx, sorted)
	var g errgroup.Group
	g.SetLimit(readWorkers)
	for e, offs := range byPack {
		for _, run := range e.b.plan(offs) {
			g.Go(func() error {
				if ctx.Err() == nil {
					_ = e.b.fetchRun(ctx, run)
				}
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

// Commit returns the tree of a commit and its committer time. It calls grow, when it is set, with the size of the
// commit before it reads it.
func (s *Service) Commit(ctx context.Context, repo Repo, sha gitpack.SHA, grow func(size int64) error) (gitpack.SHA, time.Time, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return gitpack.SHA{}, time.Time{}, err
	}
	t, d, err := v.getSized(ctx, sha, grow)
	if err != nil {
		return gitpack.SHA{}, time.Time{}, err
	}
	if t != gitpack.Commit || !bytes.HasPrefix(d, []byte("tree ")) || len(d) < 45 {
		return gitpack.SHA{}, time.Time{}, ErrInvalid
	}
	tree, err := gitpack.ParseSHA(string(d[5:45]))
	if err != nil {
		return gitpack.SHA{}, time.Time{}, ErrInvalid
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
	return tree, time.Unix(sec, 0).UTC(), nil
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
// one, taken from the pack index.
func (s *Service) TreeBulk(ctx context.Context, repo Repo, sha gitpack.SHA, maxEntries int, grow func(treeBytes int64) error) ([]Tree, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	var out []Tree
	queue := []gitpack.SHA{sha}
	total, held := 0, int64(0)
	rd := &Reader{v}
	ahead := 0
	for k := 0; k < len(queue) && (len(out) == 0 || total < maxEntries); k++ {
		if k == ahead {
			ahead = min(k+treeAhead, len(queue))
			rd.Prefetch(ctx, queue[k:ahead])
		}
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
		tree, err := v.parseTree(ctx, cur, d)
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
	return out, nil
}

func (v *view) parseTree(ctx context.Context, sha gitpack.SHA, data []byte) (Tree, error) {
	tree := Tree{SHA: sha}
	var blobs []gitpack.SHA
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
		if mode != 0o40000 && mode != 0o160000 {
			blobs = append(blobs, e.SHA)
		}
		tree.Entries = append(tree.Entries, e)
		data = data[nul+21:]
	}
	rd := &Reader{v}
	next := 0
	for i := range tree.Entries {
		e := &tree.Entries[i]
		if e.Mode == 0o40000 || e.Mode == 0o160000 {
			continue
		}
		if next%treeAhead == 0 {
			rd.Prefetch(ctx, blobs[next:min(next+treeAhead, len(blobs))])
		}
		next++
		p, err := v.find(ctx, e.SHA)
		if err != nil {
			return Tree{}, err
		}
		if e.Size, _, err = p.Size(e.SHA); err != nil {
			return Tree{}, ErrNotFound
		}
	}
	return tree, nil
}
