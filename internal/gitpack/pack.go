// Package gitpack reads git pack files and v2 pack indexes by ranged reads.
package gitpack

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"slices"
	"sort"
	"sync"
)

type Type uint8

const (
	Commit Type = 1
	Tree   Type = 2
	Blob   Type = 3
	Tag    Type = 4
)

func (t Type) String() string {
	switch t {
	case Commit:
		return "commit"
	case Tree:
		return "tree"
	case Blob:
		return "blob"
	case Tag:
		return "tag"
	}
	return "?"
}

type SHA [20]byte

// ParseType maps a type name to a Type, or 0.
func ParseType(name string) Type {
	for t := Commit; t <= Tag; t++ {
		if t.String() == name {
			return t
		}
	}
	return 0
}

func (s SHA) String() string { return hex.EncodeToString(s[:]) }

func ParseSHA(h string) (SHA, error) {
	var s SHA
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 20 {
		return s, fmt.Errorf("bad sha %q", h)
	}
	copy(s[:], b)
	return s, nil
}

var ErrNotFound = errors.New("object not found")

// ReaderAt is the ranged-read seam over the bytes of one pack.
type ReaderAt interface {
	io.ReaderAt
	Size() int64
}

type Pack struct {
	pack   ReaderAt
	fanout [256]uint32
	names  []byte // n*20 sorted
	offs   []byte // n*4
	big    []byte // 8-byte offsets
	n      int
	idxLen int

	c        *objCache
	cacheAll bool
	ctx      context.Context
	lookup   func(SHA) (int64, bool)
}

// objCache holds resolved objects. Packs made by Bind share it.
type objCache struct {
	mu    sync.Mutex
	m     map[int64]*resolved
	order []int64
	bytes int64
	max   int64
	work  int64
	// changed runs, with mu released, after an object enters the cache.
	changed func()
}

// Bind returns a view of the pack that reads through r and shares its index
// and object cache, so a request can bring its own context to the reads.
func (p *Pack) Bind(r ReaderAt) *Pack {
	q := *p
	q.pack = r
	return &q
}

type resolved struct {
	t     Type
	data  []byte
	depth int
}

const maxCacheBytes = 2 << 20

const (
	idxHead        = 8 + 1024
	MaxObjectBytes = 16 << 20
	maxDeltaDepth  = 512
	// verifyCacheBytes holds the resolved bases while a pack is verified, and
	// maxVerifyWork bounds the bytes that verifying one pack may inflate.
	verifyCacheBytes = 32 << 20
	maxVerifyWork    = 2 << 30
	// VerifyWorkBytes bounds the working memory of Verify: the base cache, the object kept past it, and the delta, its
	// base and its result while one object resolves.
	VerifyWorkBytes = verifyCacheBytes + 4*MaxObjectBytes
)

var (
	ErrCorrupt  = errors.New("gitpack: corrupt pack or index")
	ErrTooLarge = errors.New("gitpack: object too large")
)

// NewPack checks the structure and the checksum of a v2 index before it
// trusts a byte of it.
func NewPack(idx []byte, pack ReaderAt) (*Pack, error) {
	if len(idx) < idxHead+40 || !bytes.Equal(idx[:4], []byte{0xff, 't', 'O', 'c'}) || binary.BigEndian.Uint32(idx[4:8]) != 2 {
		return nil, ErrCorrupt
	}
	if sum := sha1.Sum(idx[:len(idx)-20]); !bytes.Equal(sum[:], idx[len(idx)-20:]) {
		return nil, ErrCorrupt
	}
	p := &Pack{pack: pack, c: &objCache{m: map[int64]*resolved{}, max: maxCacheBytes}}
	for i := range p.fanout {
		p.fanout[i] = binary.BigEndian.Uint32(idx[8+4*i:])
		if i > 0 && p.fanout[i] < p.fanout[i-1] {
			return nil, ErrCorrupt
		}
	}
	n := int64(p.fanout[255])
	if n > int64(len(idx)-idxHead-40)/28 {
		return nil, ErrCorrupt
	}
	p.n = int(n)
	p.idxLen = len(idx)
	o := idxHead
	p.names = idx[o : o+20*p.n]
	o += 24 * p.n
	p.offs = idx[o : o+4*p.n]
	o += 4 * p.n
	p.big = idx[o : len(idx)-40]
	if len(p.big)%8 != 0 {
		return nil, ErrCorrupt
	}
	large := 0
	for i := 0; i < p.n; i++ {
		name := p.names[20*i : 20*i+20]
		bucket := name[0]
		if uint32(i) >= p.fanout[bucket] || (bucket > 0 && uint32(i) < p.fanout[bucket-1]) || (i > 0 && bytes.Compare(p.names[20*i-20:20*i], name) >= 0) {
			return nil, ErrCorrupt
		}
		if off := binary.BigEndian.Uint32(p.offs[4*i:]); off&0x80000000 != 0 {
			large++
			if int(off&0x7fffffff) >= len(p.big)/8 || binary.BigEndian.Uint64(p.big[8*int(off&0x7fffffff):]) >= 1<<62 {
				return nil, ErrCorrupt
			}
		}
	}
	if len(p.big) != 8*large {
		return nil, ErrCorrupt
	}
	return p, nil
}

// NewAt returns a Pack that holds no index. It finds a name through the function that WithLookup sets, so a caller
// that indexes many packs in one structure reads each pack with no index of its own.
func NewAt(pack ReaderAt) *Pack {
	return &Pack{pack: pack, c: &objCache{m: map[int64]*resolved{}, max: maxCacheBytes}}
}

// WithLookup returns a view of the pack that finds a name with f. The view shares the object cache.
func (p *Pack) WithLookup(f func(SHA) (int64, bool)) *Pack {
	q := *p
	q.lookup = f
	return &q
}

// Bytes is the size of the index that the pack keeps in memory.
func (p *Pack) Bytes() int { return p.idxLen }

// CachedBytes is the size of the resolved objects the pack holds.
func (p *Pack) CachedBytes() int {
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	return int(p.c.bytes)
}

// OnCacheChange sets f to run, with no lock held, after an object enters the
// cache. Call it before the pack is shared.
func (p *Pack) OnCacheChange(f func()) { p.c.changed = f }

// Verify checks that pack is the pack that the index describes and that
// every object hashes to its name. It resolves the objects in pack order so
// that each delta chain is read once, and it stops at a budget of inflated
// bytes or when ctx ends.
func (p *Pack) Verify(ctx context.Context, idx, pack []byte) error {
	if len(pack) < 32 || string(pack[:4]) != "PACK" || (binary.BigEndian.Uint32(pack[4:8]) != 2 && binary.BigEndian.Uint32(pack[4:8]) != 3) || int(binary.BigEndian.Uint32(pack[8:12])) != p.n {
		return ErrCorrupt
	}
	sum := sha1.Sum(pack[:len(pack)-20])
	if !bytes.Equal(sum[:], pack[len(pack)-20:]) || !bytes.Equal(sum[:], idx[len(idx)-40:len(idx)-20]) {
		return ErrCorrupt
	}
	type item struct {
		off  int64
		name SHA
	}
	items := make([]item, p.n)
	for i := range items {
		copy(items[i].name[:], p.names[20*i:])
		items[i].off, _ = p.find(items[i].name)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].off < items[j].off })
	p.c.max, p.c.work, p.cacheAll, p.ctx = verifyCacheBytes, 0, true, ctx
	defer func() {
		p.c.mu.Lock()
		defer p.c.mu.Unlock()
		p.c.max, p.c.work, p.cacheAll, p.ctx = maxCacheBytes, 0, false, nil
		for p.c.bytes > p.c.max && len(p.c.order) > 0 {
			o := p.c.order[0]
			p.c.order = p.c.order[1:]
			p.c.bytes -= int64(len(p.c.m[o].data))
			delete(p.c.m, o)
		}
	}()
	for _, it := range items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r, err := p.objAt(it.off, nil)
		if errors.Is(err, ErrTooLarge) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err != nil || Hash(r.t, r.data) != it.name {
			return ErrCorrupt
		}
	}
	return nil
}

func (p *Pack) Count() int { return p.n }

// WholeType returns the type of the object s and its size when the pack stores it whole, and false for a delta or an
// object that the pack lacks. It reads the header of the entry only.
func (p *Pack) WholeType(s SHA) (Type, int64, bool) {
	off, ok := p.find(s)
	if !ok {
		return 0, 0, false
	}
	h, err := p.readHeader(off)
	if err != nil || h.delta != 0 {
		return 0, 0, false
	}
	return h.t, h.size, true
}

// Names returns the names of the objects of the pack, in the order of the index.
func (p *Pack) Names() iter.Seq[SHA] {
	return func(yield func(SHA) bool) {
		for i := range p.n {
			if !yield(SHA(p.names[20*i : 20*i+20])) {
				return
			}
		}
	}
}

func (p *Pack) find(s SHA) (int64, bool) {
	if p.lookup != nil {
		return p.lookup(s)
	}
	i, ok := p.Index(s)
	if !ok {
		return 0, false
	}
	off := binary.BigEndian.Uint32(p.offs[4*i:])
	if off&0x80000000 != 0 {
		return int64(binary.BigEndian.Uint64(p.big[8*int(off&0x7fffffff):])), true
	}
	return int64(off), true
}

// Index returns the position of the object s in the order of the index.
func (p *Pack) Index(s SHA) (int, bool) {
	lo, hi := 0, int(p.fanout[s[0]])
	if s[0] > 0 {
		lo = int(p.fanout[s[0]-1])
	}
	i := lo + sort.Search(hi-lo, func(i int) bool {
		return bytes.Compare(p.names[20*(lo+i):20*(lo+i)+20], s[:]) >= 0
	})
	if i >= hi || !bytes.Equal(p.names[20*i:20*i+20], s[:]) {
		return 0, false
	}
	return i, true
}

// Lookup returns the offset of the object s in the pack.
func (p *Pack) Lookup(s SHA) (int64, bool) { return p.find(s) }

func (p *Pack) Has(s SHA) bool { _, ok := p.find(s); return ok }

type header struct {
	t     Type
	size  int64
	hlen  int
	base  int64 // ofs delta base offset
	bsha  SHA
	delta uint8 // 0 none, 6 ofs, 7 ref
}

func (p *Pack) readHeader(off int64) (header, error) {
	var buf [64]byte
	n, err := p.pack.ReadAt(buf[:], off)
	if err != nil && (n == 0 || !errors.Is(err, io.EOF)) {
		return header{}, err
	}
	return parseHeader(buf[:n], off)
}

// EntryBound is the most bytes that the pack entry at the start of data takes: its header, the base name of a ref
// delta, and its deflate stream even if the stream stores the data. It reports false when data does not hold the
// header.
func EntryBound(data []byte) (int64, bool) {
	h, err := parseHeader(data[:min(len(data), 64)], math.MaxInt64)
	if err != nil {
		return 0, false
	}
	return int64(h.hlen) + deflateBound(h.size), true
}

// deflateBound is the most bytes that a zlib stream of size bytes takes.
func deflateBound(size int64) int64 { return size + 5*(size/16384+1) + 16 }

func parseHeader(buf []byte, off int64) (header, error) {
	n := len(buf)
	i := 0
	next := func() (byte, bool) {
		if i >= n {
			return 0, false
		}
		i++
		return buf[i-1], true
	}
	var h header
	c, ok := next()
	if !ok {
		return h, ErrCorrupt
	}
	typ := (c >> 4) & 7
	h.size = int64(c & 15)
	for shift := uint(4); c&0x80 != 0; shift += 7 {
		if c, ok = next(); !ok || shift > 49 {
			return h, ErrCorrupt
		}
		h.size |= int64(c&0x7f) << shift
	}
	switch typ {
	case 6:
		if c, ok = next(); !ok {
			return h, ErrCorrupt
		}
		v := int64(c & 0x7f)
		for c&0x80 != 0 {
			if c, ok = next(); !ok || v > 1<<40 {
				return h, ErrCorrupt
			}
			v = ((v + 1) << 7) | int64(c&0x7f)
		}
		if v <= 0 || v > off {
			return h, ErrCorrupt
		}
		h.delta, h.base = 6, off-v
	case 7:
		if i+20 > n {
			return h, ErrCorrupt
		}
		copy(h.bsha[:], buf[i:i+20])
		i += 20
		h.delta = 7
	default:
		h.t = Type(typ)
		if h.t < Commit || h.t > Tag {
			return h, ErrCorrupt
		}
	}
	if h.size > MaxObjectBytes {
		return h, ErrTooLarge
	}
	h.hlen = i
	return h, nil
}

func (p *Pack) inflate(off int64, size int64) ([]byte, error) {
	out, err := p.inflateWithin(off, size, min(p.pack.Size()-off, deflateBound(size)))
	if err != nil && off+deflateBound(size) < p.pack.Size() {
		return p.inflateWithin(off, size, p.pack.Size()-off)
	}
	return out, err
}

// inflateWithin inflates the stream at off from the next limit bytes of the pack. A stream that a producer made larger
// than deflateBound fails here, and inflate reads it again without the limit.
func (p *Pack) inflateWithin(off, size, limit int64) ([]byte, error) {
	sr := io.NewSectionReader(p.pack, off, limit)
	zr, err := zlib.NewReader(bufioReader(sr, size))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	out := make([]byte, size)
	if _, err := io.ReadFull(zr, out); err != nil {
		return nil, err
	}
	// The stream must end where the header says, with a valid checksum.
	var extra [1]byte
	if n, err := zr.Read(extra[:]); n > 0 || !errors.Is(err, io.EOF) {
		return nil, ErrCorrupt
	}
	return out, nil
}

// Size returns the object size and type without inflating the whole object.
func (p *Pack) Size(s SHA) (int64, Type, error) {
	off, ok := p.find(s)
	if !ok {
		return 0, 0, ErrNotFound
	}
	return p.sizeAt(off, 0)
}

// Footprint is the bytes that Get of s holds at most for its result: the size of an object that the pack stores whole,
// and MaxObjectBytes for a delta, because its base is inflated first and can be of any size.
func (p *Pack) Footprint(s SHA) (int64, error) {
	off, ok := p.find(s)
	if !ok {
		return 0, ErrNotFound
	}
	h, err := p.readHeader(off)
	if err != nil {
		return 0, err
	}
	if h.delta != 0 {
		return MaxObjectBytes, nil
	}
	return h.size, nil
}

// SizeAt returns the inflated size of the entry at off, whole or delta, without resolving a base.
func (p *Pack) SizeAt(off int64) (int64, error) {
	h, err := p.readHeader(off)
	if err != nil {
		return 0, err
	}
	if h.delta == 0 {
		return h.size, nil
	}
	return p.deltaSize(off, h)
}

func (p *Pack) deltaSize(off int64, h header) (int64, error) {
	sr := io.NewSectionReader(p.pack, off+int64(h.hlen), 2048)
	zr, err := zlib.NewReader(sr)
	if err != nil {
		return 0, err
	}
	var pre [32]byte
	n, _ := io.ReadFull(zr, pre[:])
	_ = zr.Close()
	b := pre[:n]
	_, k := uvarint(b)
	sz, _ := uvarint(b[k:])
	if sz > MaxObjectBytes {
		return 0, ErrTooLarge
	}
	return int64(sz), nil
}

func (p *Pack) sizeAt(off int64, depth int) (int64, Type, error) {
	if depth > maxDeltaDepth {
		return 0, 0, ErrCorrupt
	}
	h, err := p.readHeader(off)
	if err != nil {
		return 0, 0, err
	}
	if h.delta == 0 {
		return h.size, h.t, nil
	}
	sz, err := p.deltaSize(off, h)
	if err != nil {
		return 0, 0, err
	}
	var t Type
	if h.delta == 6 {
		_, t, err = p.sizeAt(h.base, depth+1)
	} else {
		boff, ok := p.find(h.bsha)
		if !ok {
			return 0, 0, ErrNotFound
		}
		_, t, err = p.sizeAt(boff, depth+1)
	}
	return sz, t, err
}

func uvarint(b []byte) (uint64, int) {
	var v uint64
	var s uint
	for i, c := range b {
		if s > 56 {
			return v, len(b)
		}
		v |= uint64(c&0x7f) << s
		s += 7
		if c&0x80 == 0 {
			return v, i + 1
		}
	}
	return v, len(b)
}

func (p *Pack) Get(s SHA) (Type, []byte, error) {
	t, d, _, err := p.GetDepth(s)
	return t, d, err
}

// GetDepth is Get that also returns the length of the delta chain that produced the object; zero is a whole object.
func (p *Pack) GetDepth(s SHA) (Type, []byte, int, error) {
	off, ok := p.find(s)
	if !ok {
		return 0, nil, 0, ErrNotFound
	}
	r, err := p.objAt(off, nil)
	if err != nil {
		return 0, nil, 0, err
	}
	return r.t, r.data, r.depth, nil
}

// spend charges n inflated bytes to the budget of a verification and checks
// its context. A pack that serves reads has no budget.
func (p *Pack) spend(n int64) error {
	if !p.cacheAll {
		return nil
	}
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	if p.c.work += n; p.c.work > maxVerifyWork {
		return ErrTooLarge
	}
	return nil
}

// objAt resolves the object at off. chain holds the offsets of the deltas
// being resolved, which stops a cycle and bounds the depth. The base of a
// delta is resolved before the delta is inflated.
func (p *Pack) objAt(off int64, chain []int64) (*resolved, error) {
	if len(chain) > maxDeltaDepth || slices.Contains(chain, off) {
		return nil, ErrCorrupt
	}
	p.c.mu.Lock()
	if r, ok := p.c.m[off]; ok {
		p.c.mu.Unlock()
		return r, nil
	}
	p.c.mu.Unlock()
	h, err := p.readHeader(off)
	if err != nil {
		return nil, err
	}
	var base *resolved
	if h.delta != 0 {
		boff := h.base
		if h.delta == 7 {
			var ok bool
			if boff, ok = p.find(h.bsha); !ok {
				return nil, ErrNotFound
			}
		}
		if base, err = p.objAt(boff, append(chain, off)); err != nil {
			return nil, err
		}
	}
	if err := p.spend(h.size); err != nil {
		return nil, err
	}
	data, err := p.inflate(off+int64(h.hlen), h.size)
	if err != nil {
		return nil, fmt.Errorf("inflate at %d: %w", off, err)
	}
	r := &resolved{t: h.t, data: data}
	if base != nil {
		_, k := uvarint(data)
		dst, _ := uvarint(data[min(k, len(data)):])
		if err := p.spend(int64(min(dst, MaxObjectBytes+1))); err != nil {
			return nil, err
		}
		out, err := applyDelta(base.data, data)
		if err != nil {
			return nil, err
		}
		if base.depth >= maxDeltaDepth {
			return nil, ErrCorrupt
		}
		r = &resolved{t: base.t, data: out, depth: base.depth + 1}
	}
	p.c.mu.Lock()
	added := false
	if _, ok := p.c.m[off]; !ok && (p.cacheAll || r.t != Blob) { // reads keep bases (trees/commits); blobs are rarely bases of bases
		p.c.m[off] = r
		p.c.order = append(p.c.order, off)
		p.c.bytes += int64(len(r.data))
		keep := 0
		if p.cacheAll {
			keep = 1
		}
		for p.c.bytes > p.c.max && len(p.c.order) > keep {
			o := p.c.order[0]
			p.c.order = p.c.order[1:]
			p.c.bytes -= int64(len(p.c.m[o].data))
			delete(p.c.m, o)
		}
		added = true
	}
	p.c.mu.Unlock()
	if added && p.c.changed != nil {
		p.c.changed()
	}
	return r, nil
}

// deltaResultSize is the size of the result that the delta d makes, at most MaxObjectBytes, or 0 when d does not say.
func deltaResultSize(d []byte) int64 {
	_, k := uvarint(d)
	size, _ := uvarint(d[k:])
	return int64(min(size, MaxObjectBytes))
}

func applyDelta(base, d []byte) ([]byte, error) {
	srcSize, k := uvarint(d)
	if srcSize != uint64(len(base)) {
		return nil, ErrCorrupt
	}
	d = d[k:]
	dstSize, k := uvarint(d)
	if dstSize > MaxObjectBytes {
		return nil, ErrTooLarge
	}
	d = d[k:]
	out := make([]byte, 0, dstSize)
	take := func(n int) ([]byte, bool) {
		if len(d) < n {
			return nil, false
		}
		b := d[:n]
		d = d[n:]
		return b, true
	}
	for len(d) > 0 {
		c := d[0]
		d = d[1:]
		switch {
		case c&0x80 != 0:
			var off, sz uint64
			for i := uint(0); i < 4; i++ {
				if c&(1<<i) != 0 {
					b, ok := take(1)
					if !ok {
						return nil, ErrCorrupt
					}
					off |= uint64(b[0]) << (8 * i)
				}
			}
			for i := uint(0); i < 3; i++ {
				if c&(0x10<<i) != 0 {
					b, ok := take(1)
					if !ok {
						return nil, ErrCorrupt
					}
					sz |= uint64(b[0]) << (8 * i)
				}
			}
			if sz == 0 {
				sz = 0x10000
			}
			if off+sz > uint64(len(base)) || uint64(len(out))+sz > dstSize {
				return nil, ErrCorrupt
			}
			out = append(out, base[off:off+sz]...)
		case c != 0:
			b, ok := take(int(c))
			if !ok || uint64(len(out))+uint64(c) > dstSize {
				return nil, ErrCorrupt
			}
			out = append(out, b...)
		default:
			return nil, ErrCorrupt
		}
	}
	if uint64(len(out)) != dstSize {
		return nil, ErrCorrupt
	}
	return out, nil
}

func Hash(t Type, data []byte) SHA {
	return hashObj(t.String(), data)
}

func bufioReader(r io.Reader, hint int64) io.Reader {
	n := int(hint/2 + 256)
	if n > 64<<10 {
		n = 64 << 10
	}
	if n < 512 {
		n = 512
	}
	return bufio.NewReaderSize(r, n)
}

func hashObj(t string, data []byte) SHA {
	h := sha1.New()
	_, _ = fmt.Fprintf(h, "%s %d\x00", t, len(data))
	_, _ = h.Write(data)
	var s SHA
	copy(s[:], h.Sum(nil))
	return s
}
