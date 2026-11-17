package gitpack

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"testing"
)

func packEntry(typ byte, size int, extra, data []byte) []byte {
	var b bytes.Buffer
	c := typ<<4 | byte(size&15)
	size >>= 4
	for size > 0 {
		b.WriteByte(c | 0x80)
		c = byte(size & 0x7f)
		size >>= 7
	}
	b.WriteByte(c)
	b.Write(extra)
	zw := zlib.NewWriter(&b)
	_, _ = zw.Write(data)
	_ = zw.Close()
	return b.Bytes()
}

func packOf(entries ...[]byte) []byte {
	head := binary.BigEndian.AppendUint32([]byte("PACK\x00\x00\x00\x02"), uint32(len(entries)))
	body := append(head, bytes.Join(entries, nil)...)
	sum := sha1.Sum(body)
	return append(body, sum[:]...)
}

func uvar(n int) []byte {
	var b []byte
	for ; n >= 0x80; n >>= 7 {
		b = append(b, byte(n&0x7f)|0x80)
	}
	return append(b, byte(n))
}

func insertDelta(baseLen int, result []byte) []byte {
	d := append(uvar(baseLen), uvar(len(result))...)
	for r := result; len(r) > 0; {
		n := min(len(r), 127)
		d = append(append(d, byte(n)), r[:n]...)
		r = r[n:]
	}
	return d
}

func refDelta(base SHA, baseLen int, result []byte) []byte {
	d := insertDelta(baseLen, result)
	return packEntry(7, len(d), base[:], d)
}

func TestUnpackResolvesAThinChainThatListsTheDependentFirst(t *testing.T) {
	ext := []byte("external base")
	extName := Hash(Blob, ext)
	mid := []byte("middle object")
	midName := Hash(Blob, mid)
	last := []byte("last object")
	pack := packOf(refDelta(midName, len(mid), last), refDelta(extName, len(ext), mid))
	objs, err := Unpack(context.Background(), pack, func(s SHA) (Type, []byte, error) {
		if s != extName {
			return 0, nil, ErrNotFound
		}
		return Blob, ext, nil
	})
	if err != nil || len(objs) != 2 || !bytes.Equal(objs[0].Data, last) || !bytes.Equal(objs[1].Data, mid) {
		t.Fatalf("objects = %v, err = %v", objs, err)
	}
}

func TestUnpackCountsWholeObjectsInTheResolvedTotal(t *testing.T) {
	const n = 15 << 20
	var entries [][]byte
	var names []SHA
	for i := range 3 {
		data := bytes.Repeat([]byte{byte('a' + i)}, n)
		names = append(names, Hash(Blob, data))
		entries = append(entries, packEntry(3, n, nil, data))
	}
	copyAll := append(append(uvar(n), uvar(n)...), 0x80|0x10|0x20|0x40, byte(n&0xff), byte(n>>8&0xff), byte(n>>16&0xff))
	for i := range 2 {
		entries = append(entries, packEntry(7, len(copyAll), names[i][:], copyAll))
	}
	if _, err := Unpack(context.Background(), packOf(entries...), nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("err = %v, want ErrTooLarge", err)
	}
}
