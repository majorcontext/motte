package midx

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

const (
	sizesMagic   = "MTSZ"
	sizesVersion = 1
	sizesHeader  = 12
)

var ErrCorruptSizes = errors.New("midx: corrupt size file")

// A size is stored as its value plus one in four bytes, and 0 means that it is not known, so an empty object and an
// object of more than 4 GiB less two bytes are both expressible.
func EncodeSize(n int64) uint32 {
	if n < 0 || n >= math.MaxUint32 {
		return 0
	}
	return uint32(n) + 1
}

// DecodeSize returns the size that v holds, and false when it is not known.
func DecodeSize(v uint32) (int64, bool) { return int64(v) - 1, v != 0 }

func sizeBlocks(n int) int { return (n + BlockRecords - 1) / BlockRecords }

// SizesFileLen is the length of the size file of a layer of n records.
func SizesFileLen(n int) int64 {
	return sizesHeader + 4*int64(n) + 4*int64(sizeBlocks(n)) + sha256.Size
}

func blockCRC(i int, values []byte) uint32 {
	return crc32.Update(crc32.ChecksumIEEE(binary.BigEndian.AppendUint32(nil, uint32(i))), crc32.IEEETable, values)
}

// BuildSizes returns the size file of a layer: a header, the blocks of 256 values in the order of the layer, each with
// a CRC-32 of its number and its values, and the SHA-256 of all of it. It returns nil when no value is known.
func BuildSizes(vals []uint32) []byte {
	b := newSizeBuf(len(vals))
	for i, v := range vals {
		b.set(i, v)
	}
	return b.finish(len(vals))
}

// sizeBuf lays out a size file as values arrive, so a builder holds one buffer of the length of the file.
type sizeBuf struct {
	data  []byte
	known bool
}

func newSizeBuf(n int) *sizeBuf { return &sizeBuf{data: make([]byte, sizesHeader, SizesFileLen(n))} }

func (b *sizeBuf) at(i int) int {
	return sizesHeader + (4*BlockRecords+4)*(i/BlockRecords) + 4*(i%BlockRecords)
}

func (b *sizeBuf) set(i int, v uint32) {
	off := b.at(i)
	if need := off + 8; len(b.data) < need {
		b.data = b.data[:need]
	}
	binary.BigEndian.PutUint32(b.data[off:], v)
	b.known = b.known || v != 0
}

func (b *sizeBuf) get(i int) uint32 { return binary.BigEndian.Uint32(b.data[b.at(i):]) }

func (b *sizeBuf) finish(n int) []byte {
	if !b.known || n == 0 {
		return nil
	}
	out := b.data[:sizesHeader+4*n+4*sizeBlocks(n)]
	copy(out, sizesMagic)
	binary.BigEndian.PutUint32(out[4:], sizesVersion)
	binary.BigEndian.PutUint32(out[8:], uint32(n))
	for i := range sizeBlocks(n) {
		off, l := SizesBlockRange(i, n)
		binary.BigEndian.PutUint32(out[off+l-4:], blockCRC(i, out[off:off+l-4]))
	}
	sum := sha256.Sum256(out)
	return append(out, sum[:]...)
}

// Sizes is a parsed size file.
type Sizes struct {
	n    int
	data []byte
}

// ParseSizes checks a size file of a layer of n records before it trusts a byte of it.
func ParseSizes(b []byte, n int) (*Sizes, error) {
	if int64(len(b)) != SizesFileLen(n) || string(b[:4]) != sizesMagic || binary.BigEndian.Uint32(b[4:8]) != sizesVersion || int(binary.BigEndian.Uint32(b[8:12])) != n {
		return nil, ErrCorruptSizes
	}
	if sum := sha256.Sum256(b[:len(b)-sha256.Size]); !bytes.Equal(sum[:], b[len(b)-sha256.Size:]) {
		return nil, ErrCorruptSizes
	}
	z := &Sizes{n: n, data: b[sizesHeader : len(b)-sha256.Size]}
	for i := range sizeBlocks(n) {
		off, l := SizesBlockRange(i, n)
		if _, err := BlockValues(i, z.data[off-sizesHeader:off-sizesHeader+l]); err != nil {
			return nil, err
		}
	}
	return z, nil
}

// Len is the number of records.
func (s *Sizes) Len() int { return s.n }

// At returns the stored value of record i.
func (s *Sizes) At(i int) uint32 {
	return binary.BigEndian.Uint32(s.data[(4*BlockRecords+4)*(i/BlockRecords)+4*(i%BlockRecords):])
}

// SizesBlockRange returns the offset and length in the size file of block i of a layer of n records, with its CRC. The
// blocks are those of the layer file.
func SizesBlockRange(i, n int) (off, length int64) {
	return sizesHeader + (4*BlockRecords+4)*int64(i), 4*int64(min(BlockRecords, n-i*BlockRecords)) + 4
}

// BlockValues checks the CRC of a block read from the size file and returns its values.
func BlockValues(i int, block []byte) ([]byte, error) {
	if len(block) < 4 || len(block)%4 != 0 || blockCRC(i, block[:len(block)-4]) != binary.BigEndian.Uint32(block[len(block)-4:]) {
		return nil, ErrCorruptSizes
	}
	return block[:len(block)-4], nil
}

// ValueAt returns the stored value at place j of the values of a block.
func ValueAt(values []byte, j int) uint32 { return binary.BigEndian.Uint32(values[4*j:]) }

type sizedIter struct {
	it Iter
	s  *Sizes
	i  int
}

// WithSizes sets the size of each record that it ends from the values of s, by position.
func WithSizes(it Iter, s *Sizes) Iter { return &sizedIter{it: it, s: s} }

func (x *sizedIter) Next() (Record, bool, error) {
	r, ok, err := x.it.Next()
	if err != nil || !ok {
		return r, false, err
	}
	if x.i >= x.s.n {
		return r, false, ErrCorruptSizes
	}
	r.Size = x.s.At(x.i)
	x.i++
	return r, true, nil
}
