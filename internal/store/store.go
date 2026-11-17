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
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"golang.org/x/sync/semaphore"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/objstore"
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
	MaxBatch = 100000
	// MaxTreeBytes bounds one tree object, and the tree bytes that one bulk read returns before it stops.
	MaxTreeBytes = 2 << 20
	maxOpenBytes = 256 << 20
	blockSize    = 256 << 10
	maxBlocks    = 4
	// MemoryBudget is the bytes that requests may hold while they work, apart from the bodies that are still being read.
	MemoryBudget = 512 << 20
	blockTimeout = 30 * time.Second
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

type PackRef struct {
	ID    string
	Bytes int64
}

// PackIndex lists the packs of a repo, oldest first.
type PackIndex interface {
	AddPack(ctx context.Context, repo Repo, packID string, bytes int64) error
	Packs(ctx context.Context, repo Repo) ([]PackRef, error)
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
	st    objstore.Store
	index PackIndex

	budget *semaphore.Weighted

	mu        sync.Mutex
	open      map[string]*openPack
	order     []string
	openBytes int
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

func New(st objstore.Store, index PackIndex) *Service {
	return &Service{st: st, index: index, open: map[string]*openPack{}, budget: semaphore.NewWeighted(MemoryBudget)}
}

func mapNotFound(err error) error {
	if errors.Is(err, objstore.ErrNotFound) {
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

func (s *Service) putPack(ctx context.Context, repo Repo, id string, idx, pack []byte) error {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return err
	}
	if _, err := s.st.Create(ctx, prefix+"/"+id+".idx", idx); err != nil {
		return err
	}
	if _, err := s.st.Create(ctx, prefix+"/"+id+".pack", pack); err != nil {
		return err
	}
	return s.index.AddPack(ctx, repo, id, int64(len(pack)))
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

// blockReader reads a pack from the object store in blocks and keeps the
// last few, so one object costs about one range read.
type blockReader struct {
	st   objstore.Store
	key  string
	size int64

	mu     sync.Mutex
	blocks map[int64][]byte
	order  []int64
	// changed runs, with no lock held, after the cached blocks change.
	changed func()
}

func (b *blockReader) cachedBytes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, d := range b.blocks {
		n += len(d)
	}
	return n
}

func (b *blockReader) Size() int64 { return b.size }

// reqReader reads through a blockReader under the context of one request.
type reqReader struct {
	b   *blockReader
	ctx context.Context
}

func (r reqReader) Size() int64 { return r.b.size }

func (r reqReader) ReadAt(p []byte, off int64) (int, error) { return r.b.readAt(r.ctx, p, off) }

func (b *blockReader) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < b.size {
		blk := off / blockSize
		data, err := b.block(ctx, blk)
		if err != nil {
			return n, err
		}
		c := copy(p[n:], data[off-blk*blockSize:])
		n += c
		off += int64(c)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b *blockReader) block(ctx context.Context, i int64) ([]byte, error) {
	b.mu.Lock()
	if d, ok := b.blocks[i]; ok {
		b.mu.Unlock()
		return d, nil
	}
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, blockTimeout)
	defer cancel()
	ctx, sp := tracer.Start(ctx, "vcs.pack.read", trace.WithAttributes(attribute.Int64("offset", i*blockSize), attribute.Int("range_bytes", blockSize)))
	d, err := b.st.GetRange(ctx, b.key, i*blockSize, blockSize)
	statsFrom(ctx).rangeRead(len(d))
	sp.SetAttributes(attribute.Int("bytes", len(d)))
	finishSpan(sp, err)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if cached, ok := b.blocks[i]; ok {
		b.mu.Unlock()
		return cached, nil
	}
	if len(b.blocks) >= maxBlocks {
		delete(b.blocks, b.order[0])
		b.order = b.order[1:]
	}
	b.blocks[i] = d
	b.order = append(b.order, i)
	b.mu.Unlock()
	if b.changed != nil {
		b.changed()
	}
	return d, nil
}

// openPack is a pack kept across requests. A request reads it through
// Bind, so the reads carry the context of the request.
type openPack struct {
	p *gitpack.Pack
	b *blockReader
	// charged and cached are guarded by Service.mu. charged is what the pack
	// adds to Service.openBytes while cached is true.
	charged int
	cached  bool
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
	e.charged = n
	for len(s.order) > 1 && s.openBytes > maxOpenBytes {
		s.evict()
	}
}

func (s *Service) evict() {
	old := s.open[s.order[0]]
	s.openBytes -= old.charged
	old.cached = false
	delete(s.open, s.order[0])
	s.order = s.order[1:]
}

func (s *Service) openPack(ctx context.Context, st objstore.Store, prefix string, ref PackRef) (*openPack, error) {
	key := prefix + "|" + ref.ID
	s.mu.Lock()
	if e, ok := s.open[key]; ok {
		s.mu.Unlock()
		return e, nil
	}
	s.mu.Unlock()
	idx, err := st.Get(ctx, prefix+"/"+ref.ID+".idx")
	if err != nil {
		return nil, mapNotFound(err)
	}
	br := &blockReader{st: st, key: prefix + "/" + ref.ID + ".pack", size: ref.Bytes, blocks: map[int64][]byte{}}
	e := &openPack{b: br}
	br.changed = func() { s.recharge(e) }
	p, err := gitpack.NewPack(idx, reqReader{b: br, ctx: context.Background()})
	if err != nil {
		return nil, err
	}
	e.p = p
	p.OnCacheChange(func() { s.recharge(e) })
	s.mu.Lock()
	if dup, ok := s.open[key]; ok {
		s.mu.Unlock()
		return dup, nil
	}
	s.open[key] = e
	s.order = append(s.order, key)
	e.cached = true
	s.mu.Unlock()
	s.recharge(e)
	return e, nil
}

// view is the packs of one repo as of one request.
type view struct {
	s      *Service
	st     objstore.Store
	prefix string
	refs   []PackRef
}

func (s *Service) view(ctx context.Context, repo Repo) (*view, error) {
	prefix, err := repoPrefix(repo)
	if err != nil {
		return nil, err
	}
	refs, err := s.index.Packs(ctx, repo)
	if err != nil {
		return nil, err
	}
	return &view{s: s, st: s.st, prefix: prefix, refs: refs}, nil
}

// find returns the pack that holds sha, newest pack first.
func (v *view) find(ctx context.Context, sha gitpack.SHA) (*gitpack.Pack, error) {
	st := statsFrom(ctx)
	for i := len(v.refs) - 1; i >= 0; i-- {
		st.packs()
		e, err := v.s.openPack(ctx, v.st, v.prefix, v.refs[i])
		if errors.Is(err, gitpack.ErrCorrupt) {
			return nil, ErrInvalid
		}
		if err != nil {
			return nil, err
		}
		if e.p.Has(sha) {
			return e.p.Bind(reqReader{b: e.b, ctx: ctx}), nil
		}
	}
	return nil, ErrNotFound
}

func (v *view) get(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	p, err := v.find(ctx, sha)
	if err != nil {
		return 0, nil, err
	}
	t, d, depth, err := p.GetDepth(sha)
	statsFrom(ctx).object(len(d), depth)
	switch {
	case errors.Is(err, gitpack.ErrNotFound):
		return 0, nil, ErrNotFound
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

func (s *Service) Blobs(ctx context.Context, repo Repo, shas []gitpack.SHA) ([][]byte, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(shas))
	total := 0
	for i, sha := range shas {
		t, d, err := v.get(ctx, sha)
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
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(shas))
	for i, sha := range shas {
		_, err := v.find(ctx, sha)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		out[i] = err == nil
	}
	return out, nil
}

// Commit returns the tree of a commit and its committer time.
func (s *Service) Commit(ctx context.Context, repo Repo, sha gitpack.SHA) (gitpack.SHA, time.Time, error) {
	t, d, err := s.Get(ctx, repo, sha)
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
// returned reach maxEntries. Blob entries carry their size.
func (s *Service) TreeBulk(ctx context.Context, repo Repo, sha gitpack.SHA, maxEntries int) ([]Tree, error) {
	v, err := s.view(ctx, repo)
	if err != nil {
		return nil, err
	}
	var out []Tree
	queue := []gitpack.SHA{sha}
	total, bytesRead := 0, 0
	for len(queue) > 0 && (len(out) == 0 || total < maxEntries && bytesRead < MaxTreeBytes) {
		cur := queue[0]
		queue = queue[1:]
		t, d, err := v.get(ctx, cur)
		if err != nil {
			return nil, err
		}
		if t != gitpack.Tree {
			return nil, ErrInvalid
		}
		if len(d) > MaxTreeBytes {
			return nil, ErrTooLarge
		}
		bytesRead += len(d)
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
			p, err := v.find(ctx, e.SHA)
			if err != nil {
				return Tree{}, err
			}
			if e.Size, _, err = p.Size(e.SHA); err != nil {
				return Tree{}, ErrNotFound
			}
		}
		tree.Entries = append(tree.Entries, e)
		data = data[nul+21:]
	}
	return tree, nil
}
