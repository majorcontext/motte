// Package mem is a bucket.Store that keeps its objects in memory.
package mem

import (
	"bytes"
	"context"
	"strconv"
	"sync"

	"github.com/majorcontext/motte/internal/bucket"
)

type object struct {
	data    []byte
	version int
}

// Store is an empty in-memory bucket.
type Store struct {
	mu   sync.Mutex
	next int
	objs map[string]object
}

// New returns an empty Store.
func New() *Store { return &Store{objs: map[string]object{}} }

func (*Store) Caps() bucket.Caps { return bucket.Caps{ConditionalDelete: true} }

func (s *Store) Create(_ context.Context, key string, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objs[key]; ok {
		return false, nil
	}
	s.next++
	s.objs[key] = object{bytes.Clone(data), s.next}
	return true, nil
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	return s.GetRange(ctx, key, 0, -1)
}

func (s *Store) GetRange(_ context.Context, key string, off, n int64) ([]byte, error) {
	s.mu.Lock()
	o, ok := s.objs[key]
	s.mu.Unlock()
	if !ok {
		return nil, bucket.ErrNotFound
	}
	end := int64(len(o.data))
	if n >= 0 {
		end = min(end, off+n)
	}
	if off >= end {
		return nil, nil
	}
	return bytes.Clone(o.data[off:end]), nil
}

func (s *Store) Version(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objs[key]
	if !ok {
		return "", bucket.ErrNotFound
	}
	return strconv.Itoa(o.version), nil
}

func (s *Store) Delete(_ context.Context, key, version string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.objs[key]; ok {
		if strconv.Itoa(o.version) != version {
			return bucket.ErrChanged
		}
		delete(s.objs, key)
	}
	return nil
}
