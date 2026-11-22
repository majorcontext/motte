package gitproto

import (
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"io"

	"github.com/majorcontext/motte/internal/gitpack"
)

const prefetchBatch = 256

// writePack streams shas as a pack of whole objects, one object in memory at a time.
func writePack(ctx context.Context, w io.Writer, objs Objects, shas []gitpack.SHA) error {
	h := sha1.New()
	mw := io.MultiWriter(w, h)
	head := []byte("PACK\x00\x00\x00\x02\x00\x00\x00\x00")
	binary.BigEndian.PutUint32(head[8:], uint32(len(shas)))
	if _, err := mw.Write(head); err != nil {
		return err
	}
	zw, err := zlib.NewWriterLevel(mw, zlib.BestSpeed)
	if err != nil {
		return err
	}
	pf, _ := objs.(Prefetcher)
	for i, sha := range shas {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pf != nil && i%prefetchBatch == 0 {
			pf.Prefetch(ctx, shas[i:min(i+prefetchBatch, len(shas))])
		}
		t, data, err := objs.Get(ctx, sha)
		if err != nil {
			return err
		}
		size := len(data)
		c := byte(t)<<4 | byte(size&15)
		size >>= 4
		var hdr []byte
		for size > 0 {
			hdr = append(hdr, c|0x80)
			c = byte(size & 0x7f)
			size >>= 7
		}
		hdr = append(hdr, c)
		if _, err := mw.Write(hdr); err != nil {
			return err
		}
		zw.Reset(mw)
		if _, err := zw.Write(data); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
	}
	_, err = w.Write(h.Sum(nil))
	return err
}
