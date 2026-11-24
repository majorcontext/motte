package store

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
)

// copyRun is one read of Copy: the range of a pack and the objects that start in it, in offset order.
type copyRun struct {
	e          *openPack
	start, end int64
	objs       []placed
}

// Copy calls fn once for each of shas, with the stored pack entry of the object when a writer can copy it as it is,
// and with nil for an object that it cannot: a delta, an object that the commit and tree cache holds, and an object
// that no pack of the reader holds. It calls fn for the copyable objects first, in the physical order of the packs,
// and reads each pack once from front to back in runs of at most maxRun, the next while fn runs on the current one.
// before is called with the size of an entry before the entry is read on its own. The entry that fn gets is valid
// until fn returns.
func (r *Reader) Copy(ctx context.Context, shas []gitpack.SHA, before func(size int64) error, fn func(sha gitpack.SHA, raw *gitpack.Raw) error) error {
	var cold, nils []gitpack.SHA
	for _, sha := range shas {
		if _, _, ok := r.v.s.cached(r.v.prefix, sha, r.v.before); ok {
			nils = append(nils, sha)
		} else {
			cold = append(cold, sha)
		}
	}
	slices.SortFunc(cold, func(a, b gitpack.SHA) int { return bytes.Compare(a[:], b[:]) })
	found, missing, err := r.v.placeNamed(ctx, cold)
	if err != nil {
		return err
	}
	slices.SortFunc(found, func(a, b placed) int {
		if c := strings.Compare(a.e.b.key, b.e.b.key); c != 0 {
			return c
		}
		return int(a.off - b.off)
	})
	runs := planCopy(found)
	read := func(i int) chan copied {
		ch := make(chan copied, 1)
		go func() {
			run := runs[i]
			n := run.end - run.start
			if err := r.flight.Acquire(ctx, n); err != nil {
				ch <- copied{err: err}
				return
			}
			d, err := run.e.b.read(ctx, run.start, run.end)
			if err == nil && int64(len(d)) != n {
				err = errors.New("store: short read of a pack run")
			}
			ch <- copied{data: d, held: true, err: err}
		}()
		return ch
	}
	var next chan copied
	if len(runs) > 0 {
		next = read(0)
	}
	for i, run := range runs {
		c := <-next
		next = nil
		if i+1 < len(runs) && c.err == nil {
			next = read(i + 1)
		}
		if c.err == nil {
			c.err = r.copyRun(ctx, run, c.data, before, fn, &nils)
		}
		if c.held {
			r.flight.Release(run.end - run.start)
		}
		if c.err != nil {
			if next != nil {
				if n := <-next; n.held {
					r.flight.Release(runs[i+1].end - runs[i+1].start)
				}
			}
			return c.err
		}
	}
	for _, sha := range append(nils, missing...) {
		if err := fn(sha, nil); err != nil {
			return err
		}
	}
	return nil
}

type copied struct {
	data []byte
	held bool
	err  error
}

func (r *Reader) copyRun(ctx context.Context, run copyRun, d []byte, before func(size int64) error, fn func(gitpack.SHA, *gitpack.Raw) error, fallback *[]gitpack.SHA) error {
	for _, o := range run.objs {
		raw, _, need, err := gitpack.RawEntry(d[o.off-run.start:])
		if need > 0 {
			if before != nil {
				if err := before(raw.Size); err != nil {
					return err
				}
			}
			end := min(o.off+need, run.e.b.size)
			var whole []byte
			if whole, err = run.e.b.read(ctx, o.off, end); err != nil {
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
func planCopy(found []placed) []copyRun {
	var runs []copyRun
	for i := 0; i < len(found); {
		e, start := found[i].e, found[i].off
		end := min(start+minRead, e.b.size)
		j := i + 1
		for ; j < len(found) && found[j].e == e && found[j].off <= end+spanGap && min(found[j].off+minRead, e.b.size)-start <= maxRun; j++ {
			end = max(end, min(found[j].off+minRead, e.b.size))
		}
		runs = append(runs, copyRun{e, start, end, found[i:j]})
		i = j
	}
	return runs
}
