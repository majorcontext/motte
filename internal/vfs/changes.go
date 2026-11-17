//go:build linux

package vfs

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
)

// ChangeEntry is one file whose content in the view differs from the base tree.
// The SHAs are git blob names; an absent side has no SHA and mode 0.
type ChangeEntry struct {
	Path    string `json:"path"`
	Status  string `json:"status"`
	OldSHA  string `json:"old_sha,omitempty"`
	OldMode uint32 `json:"old_mode,omitempty"`
	OldSize int64  `json:"old_size,omitempty"`
	NewSHA  string `json:"new_sha,omitempty"`
	NewMode uint32 `json:"new_mode,omitempty"`
	Size    int64  `json:"size,omitempty"`
}

// baseEntry finds the entry at p in the tree rt and fails when a subtree read fails.
func (v *VFS) baseEntry(ctx context.Context, rt *Tree, p string) (Entry, bool, error) {
	t := rt
	comps := strings.Split(p, "/")
	for i, c := range comps {
		e, ok := t.Lookup(c)
		if !ok || i == len(comps)-1 {
			return e, ok, nil
		}
		if !e.IsDir() {
			return Entry{}, false, nil
		}
		nt, err := v.c.Tree(ctx, e.SHA)
		if err != nil {
			return Entry{}, false, err
		}
		t = nt
	}
	return Entry{}, false, nil
}

func (v *VFS) baseLeaves(ctx context.Context, sha gitpack.SHA, prefix string, out map[string]Entry) error {
	t, err := v.c.Tree(ctx, sha)
	if err != nil {
		return err
	}
	for _, e := range t.Entries {
		p := join(prefix, e.Name)
		if e.IsDir() {
			if err := v.baseLeaves(ctx, e.SHA, p, out); err != nil {
				return err
			}
			continue
		}
		out[p] = e
	}
	return nil
}

func shaText(s gitpack.SHA) string {
	if s == (gitpack.SHA{}) {
		return ""
	}
	return s.String()
}

func diffEntries(olds, news map[string]Entry) []ChangeEntry {
	var out []ChangeEntry
	for p, o := range olds {
		n, ok := news[p]
		old := ChangeEntry{Path: p, OldSHA: o.SHA.String(), OldMode: o.Mode, OldSize: o.Size}
		switch {
		case !ok:
			old.Status = "deleted"
			out = append(out, old)
		case n.SHA != o.SHA || n.Mode != o.Mode:
			old.Status, old.NewSHA, old.NewMode, old.Size = "modified", shaText(n.SHA), n.Mode, n.Size
			out = append(out, old)
		}
	}
	for p, n := range news {
		if _, ok := olds[p]; !ok {
			out = append(out, ChangeEntry{Path: p, Status: "added", NewSHA: shaText(n.SHA), NewMode: n.Mode, Size: n.Size})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// maxHashedFile bounds the hash of one file; the shim hashes a larger file
// itself when it must compare it.
const maxHashedFile = 2 << 20

// maxHashedTotal bounds the bytes that one changes call hashes; past it the
// shim hashes the files it must compare.
const maxHashedTotal = 64 << 20

// maxBlobBatch matches the byte limit of one blob batch of the store.
const maxBlobBatch = 16 << 20

// getBlobs answers getbatch: the data of each blob, in request order. A blob
// larger than maxSize, when set, is listed in Large with no data. A batch over
// maxBlobBatch fails as too_large, so the caller splits it.
func (v *VFS) getBlobs(ctx context.Context, names []string, maxSize int64) ctlResp {
	if len(names) > maxCtlObjects {
		return ctlResp{Err: fmt.Sprintf("vfs: more than %d objects in one request", maxCtlObjects)}
	}
	shas := make([]gitpack.SHA, len(names))
	for i, n := range names {
		s, err := gitpack.ParseSHA(n)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		shas[i] = s
	}
	blobs := make([][]byte, len(shas))
	var missing []gitpack.SHA
	var at []int
	var r blobReply
	for i, s := range shas {
		if !v.c.hasBlob(s) {
			missing, at = append(missing, s), append(at, i)
			continue
		}
		st, err := os.Stat(v.c.blobPath(s))
		if err == nil && r.skip(i, st.Size(), maxSize) {
			continue
		}
		if err == nil && r.total > maxBlobBatch {
			return tooLarge
		}
		if err == nil {
			blobs[i], err = os.ReadFile(v.c.blobPath(s))
		}
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
	}
	if len(missing) > 0 {
		got, err := v.cfg.Store.Blobs(ctx, missing)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		if len(got) != len(missing) {
			return ctlResp{Err: fmt.Sprintf("vfs: store answered %d of %d blobs", len(got), len(missing))}
		}
		for j, i := range at {
			if !r.skip(i, int64(len(got[j])), maxSize) {
				blobs[i] = got[j]
			}
		}
	}
	if r.total > maxBlobBatch {
		return tooLarge
	}
	data := make([]string, len(blobs))
	for i, b := range blobs {
		data[i] = base64.StdEncoding.EncodeToString(b)
	}
	return ctlResp{Datas: data, Large: r.large}
}

var tooLarge = ctlResp{Err: "vfs: getbatch passes the batch byte limit: vcs_object_too_large"}

type blobReply struct {
	total int64
	large []int
}

// skip records blob i of size n as large when it passes maxSize, and counts
// it toward the batch otherwise.
func (r *blobReply) skip(i int, n, maxSize int64) bool {
	if maxSize > 0 && n > maxSize {
		r.large = append(r.large, i)
		return true
	}
	r.total += n
	return false
}
