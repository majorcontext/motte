package store

import (
	"context"

	"github.com/majorcontext/motte/api"
)

// RefStore reads and swaps the refs of the native repo that the grant names. A lost swap is
// client.ErrGenerationConflict, and client.CurrentRef returns the stored ref.
type RefStore interface {
	Refs(ctx context.Context) ([]api.Ref, error)
	Ref(ctx context.Context, name string) (api.Ref, error)
	SwapRef(ctx context.Context, name string, expect int64, commit string) (api.Ref, error)
	DeleteRef(ctx context.Context, name string, expect int64) error
}

func (s *HTTPStore) Refs(ctx context.Context) ([]api.Ref, error) {
	ctx, cl := s.begin(ctx, "vcs.client.refs.list", "/v1/vcs/refs")
	refs, err := s.c().Refs(ctx)
	return refs, cl.end(ctx, len(refs), err)
}

func (s *HTTPStore) Ref(ctx context.Context, name string) (api.Ref, error) {
	ctx, cl := s.begin(ctx, "vcs.client.ref.get", "/v1/vcs/refs/{name}")
	ref, err := s.c().Ref(ctx, name)
	return ref, cl.end(ctx, 1, err)
}

func (s *HTTPStore) SwapRef(ctx context.Context, name string, expect int64, commit string) (api.Ref, error) {
	ctx, cl := s.begin(ctx, "vcs.client.ref.swap", "/v1/vcs/refs/{name}")
	ref, err := s.c().SwapRef(ctx, name, expect, commit)
	return ref, cl.end(ctx, 1, err)
}

func (s *HTTPStore) DeleteRef(ctx context.Context, name string, expect int64) error {
	ctx, cl := s.begin(ctx, "vcs.client.ref.delete", "/v1/vcs/refs/{name}")
	return cl.end(ctx, 1, s.c().DeleteRef(ctx, name, expect))
}
