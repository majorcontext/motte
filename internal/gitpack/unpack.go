package gitpack

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
)

// MaxUnpackObjects bounds the objects of one pack that Unpack reads.
const MaxUnpackObjects = 100000

// MaxUnpackBytes bounds the inflated bytes that Unpack reads from entries and the bytes of the objects that it
// returns, each.
const MaxUnpackBytes = 64 << 20

type unpacked struct {
	off      int64
	t        Type
	data     []byte
	done     bool
	delta    uint8
	baseOff  int64
	baseName SHA
	name     SHA
}

// Unpack reads the whole pack and returns its objects with every delta resolved. The pack may be thin: external
// resolves the base of a ref delta that the pack does not hold.
func Unpack(ctx context.Context, pack []byte, external func(SHA) (Type, []byte, error)) ([]Object, error) {
	if len(pack) < 32 || string(pack[:4]) != "PACK" || (binary.BigEndian.Uint32(pack[4:8]) != 2 && binary.BigEndian.Uint32(pack[4:8]) != 3) {
		return nil, ErrCorrupt
	}
	body := pack[:len(pack)-20]
	if sum := sha1.Sum(body); !bytes.Equal(sum[:], pack[len(pack)-20:]) {
		return nil, ErrCorrupt
	}
	count := binary.BigEndian.Uint32(pack[8:12])
	if count > MaxUnpackObjects {
		return nil, ErrTooLarge
	}
	r := bytes.NewReader(body)
	_, _ = r.Seek(12, io.SeekStart)
	entries := make([]*unpacked, 0, count)
	byOff := map[int64]*unpacked{}
	var declared int64
	for range count {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		e, err := readEntry(r, int64(len(body))-int64(r.Len()), &declared)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
		byOff[e.off] = e
	}
	if r.Len() != 0 {
		return nil, ErrCorrupt
	}
	return resolveEntries(ctx, entries, byOff, external)
}

func readEntry(r *bytes.Reader, off int64, declared *int64) (*unpacked, error) {
	e := &unpacked{off: off}
	c, err := r.ReadByte()
	if err != nil {
		return nil, ErrCorrupt
	}
	typ := (c >> 4) & 7
	size := int64(c & 15)
	for shift := uint(4); c&0x80 != 0; shift += 7 {
		if c, err = r.ReadByte(); err != nil || shift > 49 {
			return nil, ErrCorrupt
		}
		size |= int64(c&0x7f) << shift
	}
	switch typ {
	case 6:
		if c, err = r.ReadByte(); err != nil {
			return nil, ErrCorrupt
		}
		v := int64(c & 0x7f)
		for c&0x80 != 0 {
			if c, err = r.ReadByte(); err != nil || v > 1<<40 {
				return nil, ErrCorrupt
			}
			v = ((v + 1) << 7) | int64(c&0x7f)
		}
		if v <= 0 || v > off {
			return nil, ErrCorrupt
		}
		e.delta, e.baseOff = 6, off-v
	case 7:
		if _, err := io.ReadFull(r, e.baseName[:]); err != nil {
			return nil, ErrCorrupt
		}
		e.delta = 7
	default:
		if e.t = Type(typ); e.t < Commit || e.t > Tag {
			return nil, ErrCorrupt
		}
	}
	if *declared += size; size > MaxObjectBytes || *declared > MaxUnpackBytes {
		return nil, ErrTooLarge
	}
	zr, err := zlib.NewReader(r)
	if err != nil {
		return nil, ErrCorrupt
	}
	defer func() { _ = zr.Close() }()
	data := make([]byte, size)
	if _, err := io.ReadFull(zr, data); err != nil {
		return nil, ErrCorrupt
	}
	var extra [1]byte
	if n, err := zr.Read(extra[:]); n > 0 || !errors.Is(err, io.EOF) {
		return nil, ErrCorrupt
	}
	e.data = data
	e.done = e.delta == 0
	if e.done {
		e.name = Hash(e.t, e.data)
	}
	return e, nil
}

func resolveEntries(ctx context.Context, entries []*unpacked, byOff map[int64]*unpacked, external func(SHA) (Type, []byte, error)) ([]Object, error) {
	byName := map[SHA]*unpacked{}
	for _, e := range entries {
		if e.done {
			byName[e.name] = e
		}
	}
	var total int64
	apply := func(e *unpacked, baseType Type, base []byte) error {
		out, err := applyDelta(base, e.data)
		if err != nil {
			return err
		}
		if total += int64(len(out)); total > MaxUnpackBytes {
			return ErrTooLarge
		}
		e.t, e.data, e.done = baseType, out, true
		e.name = Hash(e.t, e.data)
		byName[e.name] = e
		return nil
	}
	for useExternal := false; ; {
		progress := false
		for _, e := range entries {
			if e.done {
				continue
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			base := byOff[e.baseOff]
			if e.delta == 7 {
				base = byName[e.baseName]
			}
			switch {
			case base != nil && base.done:
				if err := apply(e, base.t, base.data); err != nil {
					return nil, err
				}
				progress = true
			case e.delta == 7 && base == nil && useExternal:
				t, data, err := external(e.baseName)
				if err != nil {
					return nil, err
				}
				if err := apply(e, t, data); err != nil {
					return nil, err
				}
				progress = true
			}
		}
		if progress {
			continue
		}
		if useExternal {
			break
		}
		useExternal = true
	}
	objs := make([]Object, len(entries))
	for i, e := range entries {
		if !e.done {
			return nil, ErrCorrupt
		}
		objs[i] = Object{Type: e.t, Data: e.data}
	}
	return objs, nil
}
