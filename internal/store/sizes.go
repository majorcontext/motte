package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
)

// sizeBytes is the memory that a merge holds for the size of each record, in the buffer of the builder; a compaction also
// holds the values that it read from the pack.
const sizeBytes = 4

// mergeNeed is the memory that a merge of layers over records reserves. The builder holds the size file that it writes,
// and the merge holds the size files of its inputs while it reads, unless that would pass the whole budget: then it
// reads none, and writes the sizes of no layer.
func mergeNeed(layers []midx.Entry, records int) (need int64, inputSizes bool) {
	need = (2*int64(midx.RecordSize)+sizeBytes)*int64(records) + 4<<20 + 3*graphBytes(layers)
	var sized int64
	for _, e := range layers {
		if e.Sizes != "" {
			sized += int64(e.Objects)
		}
	}
	if withInputs := need + sizeBytes*sized; withInputs <= MemoryBudget {
		return withInputs, true
	}
	return need, false
}

// sizeKey is the key of the size file, or "" when there is none.
func sizeKey(file []byte) string {
	if file == nil {
		return ""
	}
	sum := sha256.Sum256(file)
	return "midx/" + hex.EncodeToString(sum[:]) + ".sz"
}

// storeSizes stores the size file of a layer of n records under key, and keeps it in the layer cache: the tail merge of
// the next write reads it.
func (s *Service) storeSizes(ctx context.Context, prefix, key string, file []byte, n int) error {
	if _, err := s.st.Create(ctx, prefix+"/"+key, file); err != nil {
		return err
	}
	if z, err := midx.ParseSizes(file, n); err == nil {
		s.icache.put("sz|"+prefix+"/"+key, z, cap(file))
	}
	return nil
}

// sizeObject claims, stores and returns the key of the size file of a layer of n records, or "" when there is none.
func (s *Service) sizeObject(ctx context.Context, repo Repo, prefix string, file []byte, n int) (string, error) {
	key := sizeKey(file)
	if key == "" {
		return "", nil
	}
	if err := s.claim(ctx, repo, key); err != nil {
		return "", err
	}
	return key, s.storeSizes(ctx, prefix, key, file, n)
}

// objectSizes returns the stored sizes of objs in the order of their sorted names, which is the order of the index of
// the pack that holds them.
func objectSizes(objs []gitpack.Object, names []gitpack.SHA) []uint32 {
	by := make(map[gitpack.SHA]uint32, len(objs))
	for i, o := range objs {
		by[names[i]] = midx.EncodeSize(int64(len(o.Data)))
	}
	sorted := make([]gitpack.SHA, 0, len(by))
	for n := range by {
		sorted = append(sorted, n)
	}
	slices.SortFunc(sorted, func(a, b gitpack.SHA) int { return slices.Compare(a[:], b[:]) })
	out := make([]uint32, len(sorted))
	for i, n := range sorted {
		out[i] = by[n]
	}
	return out
}

// packSizes returns the stored sizes of the objects of a verified pack in the order of its index. An entry whose size
// cannot be read has none.
func packSizes(p *gitpack.Pack) []uint32 {
	out := make([]uint32, 0, p.Count())
	for name := range p.Names() {
		var v uint32
		if off, ok := p.Lookup(name); ok {
			if n, err := p.SizeAt(off); err == nil {
				v = midx.EncodeSize(n)
			}
		}
		out = append(out, v)
	}
	return out
}

// layerSizes returns the stored sizes of the records of a layer written over one pack, read from the pack.
func layerSizes(l midx.Layer, pack []byte) ([]uint32, error) {
	d, err := midx.Parse(l.Data[len(l.Data)-l.Dir:], int64(len(l.Data)))
	if err != nil {
		return nil, err
	}
	pk := gitpack.NewAt(bytesReader(pack))
	it := midx.NewIter(context.Background(), d, func(_ context.Context, off, n int64) ([]byte, error) { return l.Data[off : off+n], nil })
	out := make([]uint32, 0, l.Objects)
	for {
		r, ok, err := it.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		var v uint32
		if n, err := pk.SizeAt(int64(r.Off)); err == nil {
			v = midx.EncodeSize(n)
		}
		out = append(out, v)
	}
}

// sizesWhole loads the whole size file of layer e through the layer cache. It returns nil when the layer has none, or
// its file is missing or corrupt: a size that is not known is read from the pack.
func (s *Service) sizesWhole(ctx context.Context, prefix string, e midx.Entry) *midx.Sizes {
	if e.Sizes == "" {
		return nil
	}
	key := prefix + "/" + e.Sizes
	ck := "sz|" + key
	if z, ok := s.icache.get(ck); ok {
		s.ihits.Add(1)
		return z.(*midx.Sizes)
	}
	ch := s.loads.DoChan(ck, func() (any, error) {
		s.imisses.Add(1)
		load, cancel := context.WithTimeout(context.WithoutCancel(ctx), layerLoadTimeout)
		defer cancel()
		n := midx.SizesFileLen(e.Objects) + 1
		if readBudgetFactor*n > s.readBudget {
			return nil, ErrNotFound
		}
		if err := s.rflight.Acquire(load, readBudgetFactor*n); err != nil {
			return nil, err
		}
		defer s.rflight.Release(readBudgetFactor * n)
		data, err := s.st.GetRange(load, key, 0, n)
		if err != nil {
			return nil, err
		}
		s.idxBytes.Add(int64(len(data)))
		z, err := midx.ParseSizes(data, e.Objects)
		if err != nil {
			return nil, err
		}
		s.icache.put(ck, z, cap(data))
		return z, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil
		}
		return r.Val.(*midx.Sizes)
	case <-ctx.Done():
		return nil
	}
}

// sizeBlock returns the values of block i of the size file of layer e, or nil when they cannot be read.
func (s *Service) sizeBlock(ctx context.Context, prefix string, e midx.Entry, i int) []byte {
	key := prefix + "/" + e.Sizes
	k := blockKey{"sz|" + key, i}
	if b, ok := s.icache.get(k); ok {
		s.ihits.Add(1)
		return b.([]byte)
	}
	s.imisses.Add(1)
	off, n := midx.SizesBlockRange(i, e.Objects)
	ctx, cancel := context.WithTimeout(ctx, blockTimeout)
	defer cancel()
	b, err := s.st.GetRange(ctx, key, off, n)
	if err != nil || int64(len(b)) != n {
		return nil
	}
	b, err = midx.BlockValues(i, b)
	if err != nil {
		return nil
	}
	s.idxBytes.Add(n)
	s.icache.put(k, b, cap(b))
	return b
}

// storedSizes returns the stored size of each record of layer i at the positions pos, 0 where it is not known.
func (v *view) storedSizes(ctx context.Context, e midx.Entry, pos []int) []uint32 {
	out := make([]uint32, len(pos))
	if e.Sizes == "" || len(pos) == 0 {
		return out
	}
	if e.IsIndex() {
		if z := v.s.sizesWhole(ctx, v.prefix, e); z != nil {
			for k, p := range pos {
				if p < z.Len() {
					out[k] = z.At(p)
				}
			}
		}
		return out
	}
	at := map[int]int{}
	var blocks []int
	for _, p := range pos {
		if b := p / midx.BlockRecords; at[b] == 0 {
			blocks = append(blocks, b)
			at[b] = len(blocks)
		}
	}
	data := make([][]byte, len(blocks))
	var g errgroup.Group
	g.SetLimit(readWorkers)
	for k, b := range blocks {
		g.Go(func() error { data[k] = v.s.sizeBlock(ctx, v.prefix, e, b); return nil })
	}
	_ = g.Wait()
	for k, p := range pos {
		blk := data[at[p/midx.BlockRecords]-1]
		if j := p % midx.BlockRecords; 4*j+4 <= len(blk) {
			out[k] = midx.ValueAt(blk, j)
		}
	}
	return out
}

// indexedSizes returns the sizes that the size files of the layers hold for shas, which are sorted and distinct. A name
// that the result lacks has no stored size, and the caller reads it from its pack.
func (v *view) indexedSizes(ctx context.Context, shas []gitpack.SHA) map[gitpack.SHA]int64 {
	out := make(map[gitpack.SHA]int64, len(shas))
	pending := slices.Clone(shas)
	for i := len(v.layers) - 1; i >= 0 && len(pending) > 0; i-- {
		h, err := v.handle(ctx, i)
		if err != nil {
			return out
		}
		found, pos := make([]bool, len(pending)), make([]int, len(pending))
		if h.idx != nil {
			for j, sha := range pending {
				pos[j], found[j] = h.idx.Index(sha)
			}
		} else {
			names := make([]midx.SHA, len(pending))
			for j, sha := range pending {
				names[j] = sha
			}
			_, f, p, err := h.dir.LookupManyPos(ctx, names, func(ctx context.Context, idx []int) ([][]byte, error) {
				return v.s.blocks(ctx, h, idx)
			})
			if err != nil {
				return out
			}
			found, pos = f, p
		}
		keep := pending[:0]
		var hit []gitpack.SHA
		var at []int
		for j, sha := range pending {
			if found[j] {
				hit, at = append(hit, sha), append(at, pos[j])
			} else {
				keep = append(keep, sha)
			}
		}
		pending = keep
		for k, val := range v.storedSizes(ctx, v.layers[i], at) {
			if n, ok := midx.DecodeSize(val); ok {
				out[hit[k]] = n
			}
		}
	}
	return out
}
