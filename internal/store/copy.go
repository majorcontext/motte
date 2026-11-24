package store

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	"golang.org/x/sync/semaphore"

	"github.com/majorcontext/motte/internal/midx"

	"github.com/majorcontext/motte/internal/gitpack"
)

// placeBatch is the number of names that Copy places at once, so the index blocks that one placement holds stay within
// placeBatch blocks.
const placeBatch = 2048

// prefetchBatch is the number of objects that Copy reads in advance for the objects that it cannot copy.
const prefetchBatch = 256

// copyRun is one read of Copy: the range of a pack and the objects that start in it, in offset order.
type copyRun struct {
	pack       uint32
	start, end int64
	objs       []copyPlace
}

// copyPlace is the place of one object for Copy: the pack as an index into the list of packs of the call.
type copyPlace struct {
	sha  gitpack.SHA
	pack uint32
	off  int64
}

// packRef names a pack of a Copy: its id and size, and whether it holds metadata. The plan holds these and no open pack,
// whose caches the pack cache may evict.
type packRef struct {
	pk   midx.Pack
	meta bool
}

// Copy calls fn once for each of shas, with the stored pack entry of the object when a writer can copy it as it is,
// and with nil for an object that it cannot: a delta, an object that the commit and tree cache holds, and an object
// that no pack of the reader holds. It calls fn for the copyable objects first, in the physical order of the packs,
// and reads each pack once from front to back in runs of at most maxRun, reading the next runs while fn runs on the
// current one, with at most ahead bytes of runs read or being read.
// before is called with the size of an entry before the entry is read on its own. The entry that fn gets is valid
// until fn returns.
func (r *Reader) Copy(ctx context.Context, shas []gitpack.SHA, ahead int64, before func(size int64) error, fn func(sha gitpack.SHA, raw *gitpack.Raw) error) error {
	cold, fall := make([]gitpack.SHA, 0, len(shas)), make([]gitpack.SHA, 0, len(shas))
	for _, sha := range shas {
		if _, _, ok := r.v.s.cached(r.v.prefix, sha, r.v.before); ok {
			fall = append(fall, sha)
		} else {
			cold = append(cold, sha)
		}
	}
	slices.SortFunc(cold, func(a, b gitpack.SHA) int { return bytes.Compare(a[:], b[:]) })
	r.v.ahead.reset()
	packs := make([]packRef, 0, r.Packs())
	at := make(map[string]uint32, r.Packs())
	found := make([]copyPlace, 0, len(cold))
	for batch := range slices.Chunk(cold, placeBatch) {
		f, m, err := r.v.placeNamed(ctx, batch, false)
		if err != nil {
			return err
		}
		for _, p := range f {
			i, ok := at[p.pk.ID]
			if !ok {
				i = uint32(len(packs))
				packs, at[p.pk.ID] = append(packs, packRef{p.pk, p.meta}), i
			}
			found = append(found, copyPlace{p.sha, i, p.off})
		}
		fall = append(fall, m...)
	}
	slices.SortFunc(found, func(a, b copyPlace) int {
		if c := strings.Compare(packs[a.pack].pk.ID, packs[b.pack].pk.ID); c != 0 {
			return c
		}
		return int(a.off - b.off)
	})
	runs := planCopy(found, packs)
	if err := r.copyRuns(ctx, runs, packs, max(ahead, maxRun), before, fn, &fall); err != nil {
		return err
	}
	return r.copyRest(ctx, fall, fn)
}

// copyRest calls fn with nil for each of shas, reading the next prefetchBatch objects in advance while fn runs on the
// current ones.
func (r *Reader) copyRest(ctx context.Context, shas []gitpack.SHA, fn func(gitpack.SHA, *gitpack.Raw) error) error {
	ahead := func(from int) chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			if from < len(shas) {
				r.Prefetch(ctx, shas[from:min(from+prefetchBatch, len(shas))])
			}
		}()
		return done
	}
	cur := ahead(0)
	for i := 0; i < len(shas); i += prefetchBatch {
		nxt := ahead(i + prefetchBatch)
		<-cur
		for _, sha := range shas[i:min(i+prefetchBatch, len(shas))] {
			if err := fn(sha, nil); err != nil {
				<-nxt
				return err
			}
		}
		cur = nxt
	}
	return nil
}

type copied struct {
	data []byte
	err  error
}

// copyRuns reads runs in order, up to ahead bytes of them at once, and copies each in turn.
func (r *Reader) copyRuns(ctx context.Context, runs []copyRun, packs []packRef, ahead int64, before func(size int64) error, fn func(gitpack.SHA, *gitpack.Raw) error, fallback *[]gitpack.SHA) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	flight := semaphore.NewWeighted(ahead)
	queue := make(chan chan copied, len(runs))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(queue)
		for _, run := range runs {
			ch := make(chan copied, 1)
			if err := flight.Acquire(ctx, run.end-run.start); err != nil {
				ch <- copied{err: err}
				queue <- ch
				return
			}
			queue <- ch
			go func() {
				d, err := r.pack(packs[run.pack]).b.read(ctx, run.start, run.end)
				if err == nil && int64(len(d)) != run.end-run.start {
					err = errors.New("store: short read of a pack run")
				}
				ch <- copied{data: d, err: err}
			}()
		}
	}()
	defer func() {
		cancel()
		for ch := range queue {
			<-ch
		}
		<-done
	}()
	for _, run := range runs {
		c := <-<-queue
		if c.err == nil {
			c.err = r.copyRun(ctx, run, packs[run.pack], c.data, before, fn, fallback)
		}
		if c.err != nil {
			return c.err
		}
		flight.Release(run.end - run.start)
	}
	return nil
}

// pack opens the pack of ref. A caller keeps it only while it reads.
func (r *Reader) pack(ref packRef) *openPack { return r.v.s.openPack(r.v.prefix, ref.pk, ref.meta) }

func (r *Reader) copyRun(ctx context.Context, run copyRun, ref packRef, d []byte, before func(size int64) error, fn func(gitpack.SHA, *gitpack.Raw) error, fallback *[]gitpack.SHA) error {
	for _, o := range run.objs {
		raw, _, need, err := gitpack.RawEntry(d[o.off-run.start:])
		if need > 0 {
			if before != nil {
				if err := before(raw.Size); err != nil {
					return err
				}
			}
			end := min(o.off+need, ref.pk.Bytes)
			var whole []byte
			if whole, err = r.pack(ref).b.read(ctx, o.off, end); err != nil {
				return err
			}
			raw, _, _, err = gitpack.RawEntry(whole)
		}
		if err != nil || raw.Delta || raw.Zlib == nil {
			*fallback = append(*fallback, o.sha)
			continue
		}
		statsFrom(ctx).object(int(raw.Size), 0)
		if err := fn(o.sha, &raw); err != nil {
			return err
		}
	}
	return nil
}

// planCopy cuts the objects, sorted by pack and offset, into runs: objects less than spanGap apart join a run of at
// most maxRun, and a run ends minRead bytes after its last object, where the header and most of a small object lie.
func planCopy(found []copyPlace, packs []packRef) []copyRun {
	cut := func(i int) (j int, run copyRun) {
		size := packs[found[i].pack].pk.Bytes
		start := found[i].off
		end := min(start+minRead, size)
		for j = i + 1; j < len(found) && found[j].pack == found[i].pack && found[j].off <= end+spanGap && min(found[j].off+minRead, size)-start <= maxRun; j++ {
			end = max(end, min(found[j].off+minRead, size))
		}
		return j, copyRun{found[i].pack, start, end, found[i:j]}
	}
	n := 0
	for i := 0; i < len(found); n++ {
		i, _ = cut(i)
	}
	runs := make([]copyRun, 0, n)
	for i := 0; i < len(found); {
		var run copyRun
		i, run = cut(i)
		runs = append(runs, run)
	}
	return runs
}
