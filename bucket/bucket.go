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

// Caps are the properties of a Store that a caller may depend on.
type Caps struct {
	// ConditionalDelete: Delete tests the version atomically, so an object that is stored again is never removed by a
	// Delete of the older version.
	ConditionalDelete bool
	// Hedged: range reads have a duplicate on a second connection.
	Hedged bool
}

// Capable is an optional method of a Store, which declares its own Caps.
type Capable interface {
	Caps() Caps
}

// Wrapper is a Store that adds behavior to another one. CapsOf follows Unwrap through a Store that declares no Caps.
type Wrapper interface {
	Unwrap() Store
}

// CapsOf returns the Caps of s. It walks the wrappers of s through Unwrap: Hedged is true when any layer declares it,
// and ConditionalDelete is the answer of the outermost layer that declares Caps. A layer that declares no Caps is
// transparent, and a Store with no declaring layer has no capability.
func CapsOf(s Store) Caps {
	var caps Caps
	declared := false
	for s != nil {
		if c, ok := s.(Capable); ok {
			got := c.Caps()
			caps.Hedged = caps.Hedged || got.Hedged
			if !declared {
				caps.ConditionalDelete, declared = got.ConditionalDelete, true
			}
		}
		w, ok := s.(Wrapper)
		if !ok {
			break
		}
		s = w.Unwrap()
	}
	return caps
}

// DeletesConditionally reports whether Delete of s tests the version.
func DeletesConditionally(s Store) bool { return CapsOf(s).ConditionalDelete }

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
