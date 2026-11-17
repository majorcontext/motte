package ctl

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/majorcontext/motte/internal/gitpack"
)

// maxBlobBatch matches the byte limit of one blob batch of the store.
const maxBlobBatch = 16 << 20

var tooLarge = newFailure(CodeObjectTooLarge, "ctl: getbatch passes the batch byte limit")

// getBlobs answers getbatch: the data of each blob, in request order. A blob larger than maxSize, when set, is
// listed in Large with no data. A batch over maxBlobBatch fails as too large, so the caller splits it.
func (s *Server) getBlobs(ctx context.Context, names []string, maxSize int64) any {
	shas, bad := parseSHAs(names)
	if bad != nil {
		return *bad
	}
	blobs := make([][]byte, len(shas))
	var missing []gitpack.SHA
	var at []int
	r := blobReply{large: []int{}}
	for i, sha := range shas {
		if !s.Cache.HasBlob(sha) {
			missing, at = append(missing, sha), append(at, i)
			continue
		}
		st, err := os.Stat(s.Cache.BlobPath(sha))
		if err == nil && r.skip(i, st.Size(), maxSize) {
			continue
		}
		if err == nil && r.total > maxBlobBatch {
			return tooLarge
		}
		if err == nil {
			blobs[i], err = os.ReadFile(s.Cache.BlobPath(sha))
		}
		if err != nil {
			return internal(err)
		}
	}
	if len(missing) > 0 {
		got, err := s.Store.Blobs(ctx, missing)
		if err != nil {
			return storeFail(err)
		}
		if len(got) != len(missing) {
			return newFailure(CodeStoreUnavailable, fmt.Sprintf("ctl: store answered %d of %d blobs", len(got), len(missing)))
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
	return datasAnswer{data, r.large}
}

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
