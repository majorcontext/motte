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

func entryHeader(typ byte, size int64) []byte {
	c := typ<<4 | byte(size&15)
	size >>= 4
	var hdr []byte
	for size > 0 {
		hdr = append(hdr, c|0x80)
		c = byte(size & 0x7f)
		size >>= 7
	}
	return append(hdr, c)
}

// writePack streams shas as a pack of whole objects, one object in memory at a time. When objs is a Copier, an
// object that it can copy goes out as stored, in the order that it chooses.
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
	whole := func(sha gitpack.SHA) error {
		t, data, err := objs.Get(ctx, sha)
		if err != nil {
			return err
		}
		if _, err := mw.Write(entryHeader(byte(t), int64(len(data)))); err != nil {
			return err
		}
		zw.Reset(mw)
		if _, err := zw.Write(data); err != nil {
			return err
		}
		return zw.Close()
	}
	if cp, ok := objs.(Copier); ok {
		err = cp.Copy(ctx, shas, func(sha gitpack.SHA, raw *gitpack.Raw) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if raw == nil {
				return whole(sha)
			}
			if _, err := mw.Write(entryHeader(byte(raw.Type), raw.Size)); err != nil {
				return err
			}
			_, err := mw.Write(raw.Zlib)
			return err
		})
	} else {
		err = writeAhead(ctx, objs, shas, whole)
	}
	if err != nil {
		return err
	}
	_, err = w.Write(h.Sum(nil))
	return err
}

// writeAhead calls write for each of shas in order, reading the next prefetchBatch objects in advance.
func writeAhead(ctx context.Context, objs Objects, shas []gitpack.SHA, write func(gitpack.SHA) error) error {
	pf, _ := objs.(Prefetcher)
	var cur, nxt chan struct{}
	ahead := func(from int) chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			if pf != nil && from < len(shas) {
				pf.Prefetch(ctx, shas[from:min(from+prefetchBatch, len(shas))])
			}
		}()
		return done
	}
	defer func() {
		for _, ch := range []chan struct{}{cur, nxt} {
			if ch != nil {
				<-ch
			}
		}
	}()
	for i, sha := range shas {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i%prefetchBatch == 0 {
			if cur == nil {
				cur = ahead(i)
			}
			nxt = ahead(i + prefetchBatch)
			<-cur
			cur, nxt = nxt, nil
		}
		if err := write(sha); err != nil {
			return err
		}
	}
	return nil
}
