package store

import (
	"context"
	"errors"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
)

// OpHeadsStore reads and swaps the op heads of the workspace that the grant names.
type OpHeadsStore interface {
	OpHeads(ctx context.Context) (api.OpHeads, error)
	SwapOpHeads(ctx context.Context, expect int64, heads []string) (api.OpHeads, error)
}

// OpHeadsBudget bounds one store call, because the shim waits on it while jj
// holds its op-heads lock.
const OpHeadsBudget = 2 * time.Second

// OpObjectStore keeps the operations and views of the op log of the workspace that the grant names.
type OpObjectStore interface {
	PutOp(ctx context.Context, kind, id string, data []byte) error
	GetOp(ctx context.Context, kind, id string) ([]byte, error)
}

func (s *HTTPStore) PutOp(ctx context.Context, kind, id string, data []byte) error {
	ctx, cl := s.begin(ctx, "vcs.client.op.put", "/v1/vcs/ops/{kind}/{object_id}")
	return cl.end(ctx, 1, s.c().PutOp(ctx, kind, id, data))
}

func (s *HTTPStore) GetOp(ctx context.Context, kind, id string) ([]byte, error) {
	ctx, cl := s.begin(ctx, "vcs.client.op.get", "/v1/vcs/ops/{kind}/{object_id}")
	data, err := s.c().Op(ctx, kind, id)
	return data, cl.end(ctx, 1, err)
}

func (s *HTTPStore) PutWorkspace(ctx context.Context, op, commit string, generation int64) error {
	ctx, cl := s.begin(ctx, "vcs.client.workspace.put", "/v1/vcs/workspace")
	return cl.end(ctx, 0, s.c().SetWorkspace(ctx, op, commit, generation))
}

// OpHeads reads the op heads. A workspace that has none yet is generation 0 and no heads.
func (s *HTTPStore) OpHeads(ctx context.Context) (api.OpHeads, error) {
	ctx, cl := s.begin(ctx, "vcs.client.op_heads.get", "/v1/vcs/op-heads")
	out, err := s.c().OpHeads(ctx)
	if errors.Is(err, client.ErrNotFound) {
		out, err = api.OpHeads{Heads: []string{}}, nil
	}
	return out, cl.end(ctx, 0, err)
}

func (s *HTTPStore) SwapOpHeads(ctx context.Context, expect int64, heads []string) (api.OpHeads, error) {
	ctx, cl := s.begin(ctx, "vcs.client.op_heads.swap", "/v1/vcs/op-heads")
	out, err := s.c().SwapOpHeads(ctx, expect, heads)
	return out, cl.end(ctx, len(heads), err)
}
