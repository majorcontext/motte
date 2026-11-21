// Package bucket is the object store seam of the store service. One Store is
// one bucket of immutable objects. Implementations: Google Cloud Storage
// (bucket/gcs), an S3-compatible service (bucket/s3) and a filesystem
// directory (bucket/fs). The conformance suite is bucket/buckettest.
package bucket

import (
	"context"
	"errors"
)

// ErrNotFound is a read of a key that no object holds.
var ErrNotFound = errors.New("bucket: object not found")

type Store interface {
	// Create writes data when key is absent and reports whether it wrote. A
	// stored object is never replaced. Of concurrent creators of one key,
	// exactly one gets created true. A later read of the key sees the whole
	// object.
	Create(ctx context.Context, key string, data []byte) (created bool, err error)
	// Get returns the object, or ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
	// GetRange returns up to n bytes from off, fewer at the end of the
	// object, and none when off is at or past the end. A negative n reads to
	// the end. A missing key is ErrNotFound.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
	// Delete removes the object. A missing key is not an error. Only the
	// removal of a retired file calls it.
	Delete(ctx context.Context, key string) error
}
