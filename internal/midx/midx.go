// Package midx is the layered multi-pack index of a repository: one sorted lookup structure over many packs, the
// manifest that lists the layers of a repository, and the geometric rules that merge layers and compact packs.
package midx

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

const (
	// BlockRecords is the number of records that one ranged read of a layer returns.
	BlockRecords = 256
	// RecordSize is the size of one record: name, pack number and offset.
	RecordSize = 28
	// MaxOffset is the largest pack offset that a record holds.
	MaxOffset  = 1<<32 - 1
	headerSize = 32
	trailer    = sha256.Size
	packEntry  = 40
	bloomBits  = 10
	bloomK     = 7
	magic      = "MTMX"
	version    = 1
)

var ErrCorrupt = errors.New("midx: corrupt layer")

type SHA = [20]byte

// Pack names a pack of a layer: the hex SHA-256 of its bytes, and its size. Key and Idx are the bucket keys of the pack
// file and of its index file, below the prefix of the repository. A manifest written before the keys were recorded has
// neither, and the keys are then derived from the id.
type Pack struct {
	ID    string `json:"id"`
	Bytes int64  `json:"b"`
	Key   string `json:"k,omitempty"`
	Idx   string `json:"i,omitempty"`
}

// FileKey is the bucket key of the pack file.
func (p Pack) FileKey() string {
	if p.Key != "" {
		return p.Key
	}
	return p.ID + ".pack"
}

// IdxKey is the bucket key of the index file of the pack.
func (p Pack) IdxKey() string {
	if p.Idx != "" {
		return p.Idx
	}
	return p.ID + ".idx"
}

// Keyed returns p with its derived keys recorded.
func (p Pack) Keyed() Pack {
	p.Key, p.Idx = p.FileKey(), p.IdxKey()
	return p
}

// Record places one object: the pack that holds it, as an index of the pack list of the layer, and its offset. A layer
// keeps a record for each copy of an object, ordered by name and then by pack number.
type Record struct {
	SHA SHA
	Ord uint32
	Off uint32
	// Size is the stored size of the object (see EncodeSize). The layer file does not hold it: the size file does.
	Size uint32
}

func (r Record) compare(o Record) int {
	if c := bytes.Compare(r.SHA[:], o.SHA[:]); c != 0 {
		return c
	}
	return cmp.Compare(r.Ord, o.Ord)
}

func (r Record) put(b []byte) {
	copy(b, r.SHA[:])
	binary.BigEndian.PutUint32(b[20:], r.Ord)
	binary.BigEndian.PutUint32(b[24:], r.Off)
}

func getRecord(b []byte) Record {
	var r Record
	copy(r.SHA[:], b)
	r.Ord = binary.BigEndian.Uint32(b[20:])
	r.Off = binary.BigEndian.Uint32(b[24:])
	return r
}

// Builder assembles a layer from records in order.
type Builder struct {
	packs []Pack
	recs  []byte
	bloom []byte
	n     int
	last  Record
	sizes *sizeBuf
}

// NewBuilder returns a Builder for a layer over packs with at most maxObjects records.
func NewBuilder(packs []Pack, maxObjects int) *Builder {
	return &Builder{packs: packs, recs: make([]byte, 0, RecordSize*maxObjects), bloom: make([]byte, bloomSize(maxObjects)), sizes: newSizeBuf(maxObjects)}
}

func bloomSize(n int) int { return (n*bloomBits + 7) / 8 }

// Add appends r. A record that equals the last one is dropped. A record before it is an error.
func (b *Builder) Add(r Record) error {
	if int(r.Ord) >= len(b.packs) {
		return ErrCorrupt
	}
	if b.n > 0 {
		switch c := r.compare(b.last); {
		case c == 0:
			if b.sizes.get(b.n-1) == 0 {
				b.sizes.set(b.n-1, r.Size)
			}
			return nil
		case c < 0:
			return ErrCorrupt
		}
	}
	if len(b.recs)+RecordSize > cap(b.recs) {
		return errors.New("midx: more records than the builder holds")
	}
	b.recs = b.recs[:len(b.recs)+RecordSize]
	r.put(b.recs[len(b.recs)-RecordSize:])
	b.last = r
	b.sizes.set(b.n, r.Size)
	b.n++
	return nil
}

// Layer is a finished layer file.
type Layer struct {
	Key  string
	Data []byte
	// Dir is the length of the tail of Data that Parse reads.
	Dir     int
	Objects int
	// Sizes is the size file of the layer, and is nil when the builder knew no size.
	Sizes []byte
}

// Finish returns the layer file. Its key is the SHA-256 of its bytes, so equal layers have one key.
func (b *Builder) Finish() (Layer, error) {
	if b.n == 0 {
		return Layer{}, errors.New("midx: empty layer")
	}
	nb := (b.n + BlockRecords - 1) / BlockRecords
	bloom := b.bloom[:bloomSize(b.n)]
	if len(bloom) != len(b.bloom) {
		bloom = make([]byte, bloomSize(b.n))
	}
	clear(bloom)
	for i := 0; i < b.n; i++ {
		var s SHA
		copy(s[:], b.recs[RecordSize*i:])
		bloomAdd(bloom, &s)
	}
	dir := packEntry*len(b.packs) + RecordSize*nb + len(bloom) + headerSize + trailer
	out := make([]byte, 0, len(b.recs)+dir)
	out = append(out, b.recs...)
	for _, p := range b.packs {
		id, err := hex.DecodeString(p.ID)
		if err != nil || len(id) != 32 {
			return Layer{}, fmt.Errorf("midx: bad pack id %q", p.ID)
		}
		out = append(out, id...)
		out = binary.BigEndian.AppendUint64(out, uint64(p.Bytes))
	}
	for i := 0; i < nb; i++ {
		out = append(out, b.recs[RecordSize*BlockRecords*i:RecordSize*BlockRecords*i+RecordSize]...)
	}
	out = append(out, bloom...)
	out = append(out, magic...)
	out = binary.BigEndian.AppendUint32(out, version)
	out = binary.BigEndian.AppendUint32(out, uint32(len(b.packs)))
	out = binary.BigEndian.AppendUint32(out, BlockRecords)
	out = binary.BigEndian.AppendUint64(out, uint64(b.n))
	out = binary.BigEndian.AppendUint32(out, uint32(len(bloom)))
	out = append(out, bloomK, 0, 0, 0)
	sum := sha256.Sum256(out)
	out = append(out, sum[:]...)
	return Layer{Key: "midx/" + hex.EncodeToString(sum[:]) + ".midx", Data: out, Dir: dir, Objects: b.n, Sizes: b.sizes.finish(b.n)}, nil
}

func bloomAdd(bloom []byte, s *SHA) {
	h1, h2 := bloomHashes(s)
	m := uint64(len(bloom)) * 8
	for i := uint64(0); i < bloomK; i++ {
		bit := (h1 + i*h2) % m
		bloom[bit/8] |= 1 << (bit % 8)
	}
}

func bloomHashes(s *SHA) (uint64, uint64) {
	return binary.BigEndian.Uint64(s[0:8]), binary.BigEndian.Uint64(s[8:16]) | 1
}

// Dir is the part of a layer that a reader keeps in memory: the pack list, the first record of each block, and the
// filter. A lookup reads one block of records from the object store.
type Dir struct {
	Packs []Pack
	N     int
	size  int64
	tail  []byte
	fence []byte
	bloom []byte
}

// Resident is the memory that the Dir holds.
func (d *Dir) Resident() int {
	return len(d.fence) + len(d.bloom) + packEntry*len(d.Packs)
}

// Size is the length of the whole layer file.
func (d *Dir) Size() int64 { return d.size }

// Parse reads the tail of a layer file of size bytes, which is its last dir bytes.
func Parse(tail []byte, size int64) (*Dir, error) {
	if len(tail) < headerSize+trailer || int64(len(tail)) > size {
		return nil, ErrCorrupt
	}
	h := tail[len(tail)-trailer-headerSize : len(tail)-trailer]
	if string(h[:4]) != magic || binary.BigEndian.Uint32(h[4:8]) != version || binary.BigEndian.Uint32(h[12:16]) != BlockRecords || h[28] != bloomK {
		return nil, ErrCorrupt
	}
	np, n, bl := int(binary.BigEndian.Uint32(h[8:12])), binary.BigEndian.Uint64(h[16:24]), int(binary.BigEndian.Uint32(h[24:28]))
	if n == 0 || n > 1<<31 || np == 0 || np > 1<<20 || bl != bloomSize(int(n)) {
		return nil, ErrCorrupt
	}
	nb := (int(n) + BlockRecords - 1) / BlockRecords
	if len(tail) != packEntry*np+RecordSize*nb+bl+headerSize+trailer || size != int64(RecordSize)*int64(n)+int64(len(tail)) {
		return nil, ErrCorrupt
	}
	d := &Dir{N: int(n), size: size, tail: tail}
	for i := 0; i < np; i++ {
		e := tail[packEntry*i:]
		d.Packs = append(d.Packs, Pack{ID: hex.EncodeToString(e[:32]), Bytes: int64(binary.BigEndian.Uint64(e[32:40]))})
	}
	o := packEntry * np
	d.fence = tail[o : o+RecordSize*nb]
	d.bloom = tail[o+RecordSize*nb : o+RecordSize*nb+bl]
	return d, nil
}

// Blocks is the number of blocks of records.
func (d *Dir) Blocks() int { return len(d.fence) / RecordSize }

// BlockRange returns the offset and length of block i in the layer file.
func (d *Dir) BlockRange(i int) (off, n int64) {
	off = int64(i) * BlockRecords * RecordSize
	n = min(int64(d.N)*RecordSize-off, BlockRecords*RecordSize)
	return off, n
}

// MayContain is false when the filter proves that no record has the name s.
func (d *Dir) MayContain(s SHA) bool {
	h1, h2 := bloomHashes(&s)
	m := uint64(len(d.bloom)) * 8
	for i := uint64(0); i < bloomK; i++ {
		bit := (h1 + i*h2) % m
		if d.bloom[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Fetch returns the records of block i.
type Fetch func(ctx context.Context, i int) ([]byte, error)

// Lookup finds a record with name s, in the pack ord, or in any pack when ord is negative. A lookup reads at most one
// block.
func (d *Dir) Lookup(ctx context.Context, s SHA, ord int, fetch Fetch) (Record, bool, error) {
	if !d.MayContain(s) {
		return Record{}, false, nil
	}
	want := Record{SHA: s, Ord: uint32(max(ord, 0))}
	nb := d.Blocks()
	next := sort.Search(nb, func(i int) bool { return getRecord(d.fence[RecordSize*i:]).compare(want) >= 0 })
	if next < nb {
		if r := getRecord(d.fence[RecordSize*next:]); r.SHA == s && (ord < 0 || int(r.Ord) == ord) {
			return r, true, nil
		}
	}
	if next == 0 {
		return Record{}, false, nil
	}
	blk, err := fetch(ctx, next-1)
	if err != nil {
		return Record{}, false, err
	}
	if len(blk)%RecordSize != 0 {
		return Record{}, false, ErrCorrupt
	}
	n := len(blk) / RecordSize
	i := sort.Search(n, func(i int) bool { return getRecord(blk[RecordSize*i:]).compare(want) >= 0 })
	if i < n {
		if r := getRecord(blk[RecordSize*i:]); r.SHA == s && (ord < 0 || int(r.Ord) == ord) {
			return r, true, nil
		}
	}
	return Record{}, false, nil
}

// FetchMany returns the records of the blocks in idx, in the same order.
type FetchMany func(ctx context.Context, idx []int) ([][]byte, error)

// LookupMany is Lookup of every name of shas in any pack. shas are sorted and distinct. It tests the filter and walks
// the block fences once for the whole list, and asks fetch for each needed block once.
func (d *Dir) LookupMany(ctx context.Context, shas []SHA, fetch FetchMany) ([]Record, []bool, error) {
	recs, found, _, err := d.LookupManyPos(ctx, shas, fetch)
	return recs, found, err
}

// LookupManyPos is LookupMany that also returns the position of each record in the layer.
func (d *Dir) LookupManyPos(ctx context.Context, shas []SHA, fetch FetchMany) ([]Record, []bool, []int, error) {
	recs, found, pos := make([]Record, len(shas)), make([]bool, len(shas)), make([]int, len(shas))
	nb := d.Blocks()
	var at, of, blocks []int
	lo := 0
	for i, s := range shas {
		if !d.MayContain(s) {
			continue
		}
		want := Record{SHA: s}
		lo += sort.Search(nb-lo, func(j int) bool { return getRecord(d.fence[RecordSize*(lo+j):]).compare(want) >= 0 })
		if lo < nb {
			if r := getRecord(d.fence[RecordSize*lo:]); r.SHA == s {
				recs[i], found[i], pos[i] = r, true, lo*BlockRecords
				continue
			}
		}
		if lo == 0 {
			continue
		}
		at, of = append(at, i), append(of, lo-1)
		if len(blocks) == 0 || blocks[len(blocks)-1] != lo-1 {
			blocks = append(blocks, lo-1)
		}
	}
	if len(at) == 0 {
		return recs, found, pos, nil
	}
	data, err := fetch(ctx, blocks)
	if err != nil {
		return nil, nil, nil, err
	}
	b := 0
	for k, i := range at {
		want := Record{SHA: shas[i]}
		for blocks[b] < of[k] {
			b++
		}
		blk := data[b]
		if len(blk)%RecordSize != 0 {
			return nil, nil, nil, ErrCorrupt
		}
		n := len(blk) / RecordSize
		j := sort.Search(n, func(j int) bool { return getRecord(blk[RecordSize*j:]).compare(want) >= 0 })
		if j < n {
			if r := getRecord(blk[RecordSize*j:]); r.SHA == shas[i] {
				recs[i], found[i], pos[i] = r, true, of[k]*BlockRecords+j
			}
		}
	}
	return recs, found, pos, nil
}
