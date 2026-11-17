package gitpack

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"hash/crc32"
	"sort"
)

type Object struct {
	Type Type
	Data []byte
}

// BuildSize bounds the bytes of the pack body of objs: zlib stores an incompressible object with 5 bytes of overhead
// for each 64 KiB block, and each object adds a header and a zlib frame of at most 32 bytes.
func BuildSize(objs []Object) int {
	n := 1024
	for _, o := range objs {
		n += len(o.Data) + len(o.Data)/64 + 32
	}
	return n
}

// Build writes objects as a pack without deltas, and its v2 index. The
// returned names are the object names in the order of objs. An object that
// repeats an earlier one is stored once.
func Build(objs []Object) (pack, idx []byte, names []SHA) {
	type entry struct {
		name SHA
		crc  uint32
		off  uint32
	}
	var body bytes.Buffer
	body.Grow(BuildSize(objs))
	var entries []entry
	seen := map[SHA]bool{}
	names = make([]SHA, len(objs))
	zw := zlib.NewWriter(&body)
	for i, o := range objs {
		names[i] = Hash(o.Type, o.Data)
		if seen[names[i]] {
			continue
		}
		seen[names[i]] = true
		start := body.Len()
		size := len(o.Data)
		c := byte(o.Type)<<4 | byte(size&15)
		size >>= 4
		for size > 0 {
			body.WriteByte(c | 0x80)
			c = byte(size & 0x7f)
			size >>= 7
		}
		body.WriteByte(c)
		zw.Reset(&body)
		_, _ = zw.Write(o.Data)
		_ = zw.Close()
		entries = append(entries, entry{name: names[i], crc: crc32.ChecksumIEEE(body.Bytes()[start:]), off: uint32(12 + start)})
	}
	var p bytes.Buffer
	p.Grow(12 + body.Len() + 20)
	p.WriteString("PACK")
	_ = binary.Write(&p, binary.BigEndian, uint32(2))
	_ = binary.Write(&p, binary.BigEndian, uint32(len(entries)))
	p.Write(body.Bytes())
	sum := sha1.Sum(p.Bytes())
	p.Write(sum[:])

	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].name[:], entries[j].name[:]) < 0 })
	var x bytes.Buffer
	x.Write([]byte{0xff, 't', 'O', 'c'})
	_ = binary.Write(&x, binary.BigEndian, uint32(2))
	var fan [256]uint32
	for _, e := range entries {
		for b := int(e.name[0]); b < 256; b++ {
			fan[b]++
		}
	}
	_ = binary.Write(&x, binary.BigEndian, fan)
	for _, e := range entries {
		x.Write(e.name[:])
	}
	for _, e := range entries {
		_ = binary.Write(&x, binary.BigEndian, e.crc)
	}
	for _, e := range entries {
		_ = binary.Write(&x, binary.BigEndian, e.off)
	}
	x.Write(sum[:])
	isum := sha1.Sum(x.Bytes())
	x.Write(isum[:])
	return p.Bytes(), x.Bytes(), names
}
