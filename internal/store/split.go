package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
)

const (
	classNone = iota
	classMeta
	classBlob
	classBusy
)

// entryClasses gives each entry of a pack the class of the object that it holds. A delta has the class of its base. A
// delta whose base the pack lacks counts as a blob, so it stays with its bytes.
func entryClasses(spans []gitpack.Span, names map[gitpack.SHA]int64) []int8 {
	at := make(map[int64]int, len(spans))
	for i, sp := range spans {
		at[sp.Off] = i
	}
	cls := make([]int8, len(spans))
	var of func(i int) int8
	of = func(i int) int8 {
		if cls[i] != classNone {
			if cls[i] == classBusy {
				return classBlob
			}
			return cls[i]
		}
		sp := spans[i]
		cls[i] = classBusy
		var c int8 = classBlob
		switch {
		case sp.Delta == 0 && sp.Type != gitpack.Blob:
			c = classMeta
		case sp.Delta == 6:
			if b, ok := at[sp.BaseOff]; ok {
				c = of(b)
			}
		case sp.Delta == 7:
			if off, ok := names[sp.BaseName]; ok {
				if b, ok := at[off]; ok {
					c = of(b)
				}
			}
		}
		cls[i] = c
		return c
	}
	for i := range spans {
		of(i)
	}
	return cls
}

// errInflated reports that the metadata of a run holds more bytes than its packs, so a split cannot keep to the
// memory that the run reserved.
var errInflated = errors.New("store: metadata of the run is too large to split")

// filterIter passes the records that keep accepts, with the record that it returns, and counts the others.
type filterIter struct {
	it      midx.Iter
	in      int
	keep    func(in int, r midx.Record) (midx.Record, bool, error)
	dropped *int
}

func (f filterIter) Next() (midx.Record, bool, error) {
	for {
		r, ok, err := f.it.Next()
		if err != nil || !ok {
			return r, false, err
		}
		out, keep, err := f.keep(f.in, r)
		if err != nil {
			return r, false, err
		}
		if keep {
			return out, true, nil
		}
		*f.dropped++
	}
}

// splitPacks rewrites the packs of r as a pack of metadata and a pack of blobs. The metadata objects are written whole,
// in the order of orderMeta. The blob entries are copied unchanged, except that the distance of an offset delta
// follows the move of its base.
func (s *Service) splitPacks(ctx context.Context, repo Repo, prefix string, r *run) ([]packOut, error) {
	names := make([]map[gitpack.SHA]int64, len(r.packs))
	byOff := make([]map[int64]gitpack.SHA, len(r.packs))
	for u := range r.packs {
		names[u], byOff[u] = map[gitpack.SHA]int64{}, map[int64]gitpack.SHA{}
	}
	ins, err := s.recordIters(ctx, r)
	if err != nil {
		return nil, err
	}
	for in, it := range ins {
		for {
			rec, ok, err := it.Next()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			if int(rec.Ord) >= len(r.ords[in]) {
				return nil, midx.ErrCorrupt
			}
			u := r.ords[in][rec.Ord]
			names[u][rec.SHA], byOff[u][int64(rec.Off)] = int64(rec.Off), rec.SHA
		}
	}
	blob := make([]byte, 12, r.bytes()+16)
	newOff := make([]map[int64]int64, len(r.packs))
	meta := map[gitpack.SHA]bool{}
	var metaObjs []gitpack.Object
	blobCount := 0
	var inflated int64
	for u, p := range r.packs {
		data, err := s.st.Get(ctx, prefix+"/"+p.ID+".pack")
		if err != nil {
			return nil, mapNotFound(err)
		}
		if _, err := packEntries(data, p.Bytes); err != nil {
			return nil, fmt.Errorf("store: pack %s: %w", p.ID, err)
		}
		spans, err := gitpack.Scan(data)
		if err != nil {
			return nil, fmt.Errorf("store: pack %s: %w", p.ID, err)
		}
		cls := entryClasses(spans, names[u])
		pk := gitpack.NewAt(bytesReader(data)).WithLookup(func(n gitpack.SHA) (int64, bool) { off, ok := names[u][n]; return off, ok })
		newOff[u] = map[int64]int64{}
		for i, sp := range spans {
			if cls[i] == classMeta {
				sha, named := byOff[u][sp.Off]
				if !named || meta[sha] {
					continue
				}
				size, _, err := pk.Size(sha)
				if err != nil {
					return nil, fmt.Errorf("store: pack %s: %w", p.ID, err)
				}
				if inflated += size; inflated > r.bytes() {
					return nil, errInflated
				}
				t, obj, err := pk.Get(sha)
				if err != nil {
					return nil, fmt.Errorf("store: pack %s: %w", p.ID, err)
				}
				meta[sha] = true
				metaObjs = append(metaObjs, gitpack.Object{Type: t, Data: obj})
				continue
			}
			self := int64(len(blob))
			newOff[u][sp.Off] = self
			if sp.Delta == 6 {
				base, ok := newOff[u][sp.BaseOff]
				if !ok {
					return nil, fmt.Errorf("store: pack %s: %w", p.ID, gitpack.ErrCorrupt)
				}
				blob = append(blob, data[sp.Off:sp.Off+int64(sp.SizeLen)]...)
				blob = append(blob, gitpack.EncodeOfs(self-base)...)
				blob = append(blob, data[sp.Off+int64(sp.HdrLen):sp.End]...)
			} else {
				blob = append(blob, data[sp.Off:sp.End]...)
			}
			blobCount++
		}
	}
	var outs []packOut
	dropped, seen := 0, 0
	if blobCount > 0 {
		keep := func(in int, rec midx.Record) (midx.Record, bool, error) {
			if meta[rec.SHA] {
				return rec, false, nil
			}
			off, ok := newOff[r.ords[in][rec.Ord]][int64(rec.Off)]
			if !ok {
				return rec, false, midx.ErrCorrupt
			}
			return midx.Record{SHA: rec.SHA, Off: uint32(off)}, true, nil
		}
		fins, err := s.recordIters(ctx, r)
		if err != nil {
			return nil, err
		}
		for i := range fins {
			fins[i] = filterIter{it: fins[i], in: i, keep: keep, dropped: &dropped}
		}
		o, n, err := s.finishPack(ctx, blob, blobCount, midx.ClassBlob, fins, r.objects(), func(_ int, rec midx.Record) (midx.Record, error) { return rec, nil })
		if err != nil {
			return nil, err
		}
		seen += n
		outs = append(outs, o)
	} else {
		dropped = r.objects()
	}
	if len(metaObjs) > 0 {
		ordered := orderMeta(metaObjs)
		pack, idx, names := gitpack.Build(ordered)
		it := idxRecords{gitpack.NewIdxIter(ctx, len(metaObjs), func(_ context.Context, off, n int64) ([]byte, error) {
			if n < 0 {
				return idx[off:], nil
			}
			return idx[off : off+n], nil
		})}
		idSum := sha256.Sum256(pack)
		id := hex.EncodeToString(idSum[:])
		l, n, err := midx.Merge(ctx, []midx.Iter{it}, []midx.Pack{{ID: id, Bytes: int64(len(pack))}}, len(metaObjs), func(_ int, rec midx.Record) (midx.Record, error) { return rec, nil })
		if err != nil {
			return nil, err
		}
		if n != len(metaObjs) {
			return nil, fmt.Errorf("store: compaction wrote %d metadata records of %d", n, len(metaObjs))
		}
		outs = append(outs, packOut{id: id, pack: pack, idx: idx, layer: l, class: midx.ClassMeta, objects: len(metaObjs), graph: s.graphOf(ctx, repo, ordered, names)})
	}
	metaRecords := 0
	for _, o := range outs {
		if o.class == midx.ClassMeta {
			metaRecords = o.objects
		}
	}
	if seen+dropped != r.objects() || dropped < metaRecords {
		return nil, fmt.Errorf("store: compaction read %d records of %d", seen+dropped, r.objects())
	}
	return outs, nil
}
