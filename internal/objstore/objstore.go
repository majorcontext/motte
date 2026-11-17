// Package objstore is the object store seam of the store service. One Store
// or Bucket is one bucket. Implementations: Google Cloud Storage
// (internal/objstore/gcs), a filesystem directory (internal/objstore/fs) and
// Memory, a Store only.
package objstore

import (
	"context"
	"errors"
	"io"
	"sync"
)

var ErrNotFound = errors.New("objstore: object not found")

type Store interface {
	// Create writes data when key is absent and reports whether it wrote. A
	// stored object is never replaced.
	Create(ctx context.Context, key string, data []byte) (created bool, err error)
	Get(ctx context.Context, key string) ([]byte, error)
	// GetRange returns up to n bytes from off, fewer at the end of the object.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
}

// Attrs describes one stored object; MD5 holds raw bytes.
type Attrs struct {
	Size        int64
	ContentType string
	Metadata    map[string]string
	MD5         []byte
	Generation  int64
}

// Bucket is the whole object API of one bucket.
type Bucket interface {
	Store
	// Attrs answers ErrNotFound for a missing object.
	Attrs(ctx context.Context, key string) (Attrs, error)
	// Open reads one generation of key (0: the live one); ErrNotFound if
	// missing. Its Attrs hold Size, ContentType and Generation only.
	Open(ctx context.Context, key string, generation int64) (io.ReadCloser, Attrs, error)
	List(ctx context.Context, prefix string) ([]string, error)
	// Delete removes the live object. A missing object is not an error.
	Delete(ctx context.Context, key string) error
	// Copy copies one generation of Source (0: the live one) and returns the
	// generation it wrote; ErrNotFound for a missing source.
	Copy(ctx context.Context, c Copy) (int64, error)
}

// Copy copies one object generation; Metadata replaces that of the source.
type Copy struct {
	Source      string
	Generation  int64
	Destination string
	Metadata    map[string]string
	CreateOnly  bool
}

// Buckets opens a bucket by name.
type Buckets interface {
	Bucket(name string) Bucket
}

// Memory is an in-memory Store.
type Memory struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMemory() *Memory { return &Memory{data: map[string][]byte{}} }

func (m *Memory) Create(_ context.Context, key string, data []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		return false, nil
	}
	m.data[key] = append([]byte(nil), data...)
	return true, nil
}

func (m *Memory) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), d...), nil
}

func (m *Memory) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	d, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if off >= int64(len(d)) {
		return nil, nil
	}
	if n < 0 || n > int64(len(d))-off {
		return d[off:], nil
	}
	return d[off : off+n], nil
}

// Keys lists the stored keys.
func (m *Memory) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}
