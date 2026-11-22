package gitpack

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
)

// Span is the extent of one entry of a pack and what its header says.
type Span struct {
	Off, End int64
	// SizeLen is the length of the type and size varint. HdrLen is the length of the whole header, which adds the
	// base of a delta.
	SizeLen, HdrLen int
	// Delta is 0 for a whole entry, 6 for an offset delta and 7 for a reference delta.
	Delta    uint8
	Type     Type
	BaseOff  int64
	BaseName SHA
}

// Scan lists the entries of a checked pack in order, without keeping their data.
func Scan(pack []byte) ([]Span, error) {
	if len(pack) < 32 || string(pack[:4]) != "PACK" {
		return nil, ErrCorrupt
	}
	n := binary.BigEndian.Uint32(pack[8:12])
	r := bytes.NewReader(pack[:len(pack)-20])
	_, _ = r.Seek(12, io.SeekStart)
	pos := func() int64 { return r.Size() - int64(r.Len()) }
	out := make([]Span, 0, min(n, 1<<20))
	for range n {
		s, err := scanEntry(r, pos)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if r.Len() != 0 {
		return nil, ErrCorrupt
	}
	return out, nil
}

func scanEntry(r *bytes.Reader, pos func() int64) (Span, error) {
	s := Span{Off: pos()}
	c, err := r.ReadByte()
	if err != nil {
		return s, ErrCorrupt
	}
	typ := (c >> 4) & 7
	for shift := uint(4); c&0x80 != 0; shift += 7 {
		if c, err = r.ReadByte(); err != nil || shift > 49 {
			return s, ErrCorrupt
		}
	}
	s.SizeLen = int(pos() - s.Off)
	switch typ {
	case 6:
		if c, err = r.ReadByte(); err != nil {
			return s, ErrCorrupt
		}
		v := int64(c & 0x7f)
		for c&0x80 != 0 {
			if c, err = r.ReadByte(); err != nil || v > 1<<40 {
				return s, ErrCorrupt
			}
			v = ((v + 1) << 7) | int64(c&0x7f)
		}
		if v <= 0 || v > s.Off {
			return s, ErrCorrupt
		}
		s.Delta, s.BaseOff = 6, s.Off-v
	case 7:
		if _, err := io.ReadFull(r, s.BaseName[:]); err != nil {
			return s, ErrCorrupt
		}
		s.Delta = 7
	default:
		if s.Type = Type(typ); s.Type < Commit || s.Type > Tag {
			return s, ErrCorrupt
		}
	}
	s.HdrLen = int(pos() - s.Off)
	zr, err := zlib.NewReader(r)
	if err != nil {
		return s, ErrCorrupt
	}
	defer func() { _ = zr.Close() }()
	if _, err := io.Copy(io.Discard, zr); err != nil && !errors.Is(err, io.EOF) {
		return s, ErrCorrupt
	}
	s.End = pos()
	return s, nil
}

// EncodeOfs is the encoded form of the distance to the base of an offset delta.
func EncodeOfs(dist int64) []byte {
	b := []byte{byte(dist & 0x7f)}
	for dist >>= 7; dist > 0; dist >>= 7 {
		dist--
		b = append([]byte{byte(0x80 | dist&0x7f)}, b...)
	}
	return b
}
