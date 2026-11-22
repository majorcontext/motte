package land

import (
	"context"
	"errors"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/linemerge"
)

var errNoMerge = errors.New("land: the path cannot merge")

func pathOf(n *node) string {
	if n.leaf == nil {
		return ""
	}
	return n.leaf.Path
}

// canMerge sets the paths that apply merges, and reports whether every clashing path is one that the change modifies,
// and nothing else, in each commit that touches it.
func (b *Batch) canMerge(hits []string, set *pathSet, changes [][]Change) bool {
	b.merging = make(map[string]struct{}, len(hits))
	for _, h := range hits {
		if _, own := set.seen[h]; !own {
			return false
		}
		b.merging[h] = struct{}{}
	}
	for _, cs := range changes {
		for _, ch := range cs {
			if _, hit := b.merging[ch.Path]; hit && (ch.Old == nil || ch.New == nil) {
				return false
			}
		}
	}
	return true
}

func regular(mode uint32) bool { return mode == 0o100644 || mode == 0o100755 }

// mergeLeaf returns the entry that a change of one regular file makes on cur, the entry of the running tip, when the
// tip changed the file too. It merges the lines of the base blob, the tip blob and the change blob, and returns
// errNoMerge when it cannot.
func (b *Batch) mergeLeaf(ctx context.Context, cur Entry, had bool, ch *Change) (Entry, error) {
	if ch == nil || ch.Old == nil || ch.New == nil || !had || ch.Old.Mode != ch.New.Mode || cur.Mode != ch.Old.Mode || !regular(cur.Mode) {
		return Entry{}, errNoMerge
	}
	if cur.SHA == ch.Old.SHA {
		return Entry{Name: cur.Name, Mode: cur.Mode, SHA: ch.New.SHA}, nil
	}
	var blobs [3][]byte
	for i, sha := range [3]gitpack.SHA{ch.Old.SHA, cur.SHA, ch.New.SHA} {
		data, err := b.mergeBlob(ctx, sha)
		if errors.Is(err, linemerge.ErrTooLarge) {
			err = errNoMerge
		}
		if err != nil {
			return Entry{}, err
		}
		blobs[i] = data
	}
	merged, err := linemerge.Merge(blobs[0], blobs[1], blobs[2])
	if err != nil {
		return Entry{}, errNoMerge
	}
	return Entry{Name: cur.Name, Mode: cur.Mode, SHA: b.put(gitpack.Blob, merged)}, nil
}

// mergeBlob reads a blob that is small enough to merge. The batch counts three times its size, for the working
// memory of the merge.
func (b *Batch) mergeBlob(ctx context.Context, sha gitpack.SHA) ([]byte, error) {
	if i, ok := b.byID[sha]; ok {
		if b.objs[i].Type != gitpack.Blob {
			return nil, errNoMerge
		}
		return b.objs[i].Data, nil
	}
	t, data, err := b.src.Get(ctx, sha, func(size int64) error {
		if size > linemerge.MaxBytes {
			return linemerge.ErrTooLarge
		}
		b.loaded += 3 * size
		return b.grow(int64(b.bytes) + b.loaded)
	})
	if err != nil {
		return nil, err
	}
	if t != gitpack.Blob {
		return nil, errNoMerge
	}
	return data, nil
}
