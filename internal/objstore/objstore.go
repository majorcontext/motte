// Package objstore is the object store seam of the store service. One Store
// is one bucket. Implementations: Google Cloud Storage (internal/objstore/gcs)
// and a filesystem directory (internal/objstore/fs).
package objstore

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("objstore: object not found")

type Store interface {
	// Create writes data when key is absent and reports whether it wrote. A
	// stored object is never replaced or deleted.
	Create(ctx context.Context, key string, data []byte) (created bool, err error)
	Get(ctx context.Context, key string) ([]byte, error)
	// GetRange returns up to n bytes from off, fewer at the end of the object.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
}
