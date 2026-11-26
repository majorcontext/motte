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

// ErrChanged is a Delete of an object whose version is not the one named.
var ErrChanged = errors.New("bucket: object changed")

// ConditionalDeleter is an optional method of a Store. ConditionalDelete reports whether Delete tests the version
// atomically, so an object that is stored again is never removed by a Delete of the older version.
type ConditionalDeleter interface {
	ConditionalDelete() bool
}

// DeletesConditionally reports whether Delete of s tests the version. A Store that does not declare it answers false,
// so a wrapper must forward the method.
func DeletesConditionally(s Store) bool {
	c, ok := s.(ConditionalDeleter)
	return ok && c.ConditionalDelete()
}

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
	// Version returns an opaque name of the object as it is now, or
	// ErrNotFound. An object that is deleted and stored again has another
	// version, even with the same bytes, where the backend can tell (see
	// Delete).
	Version(ctx context.Context, key string) (string, error)
	// Delete removes the object when its version is version, as Version
	// returned it. A missing key is not an error. An object of another
	// version stays, and Delete returns ErrChanged. A backend that cannot
	// test the version atomically removes the object whatever its version:
	// bucket/s3 does, because an ETag is the same for the same bytes. Only
	// the removal of a retired file calls it.
	Delete(ctx context.Context, key, version string) error
}
