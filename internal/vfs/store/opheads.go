package store

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/majorcontext/motte/api"
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
	cctx, cl := s.begin(ctx, "vcs.client.op.put", "/v1/vcs/ops/{kind}/{object_id}")
	err := s.do(cctx, cl, http.MethodPut, "/v1/vcs/ops/"+url.PathEscape(kind)+"/"+url.PathEscape(id), api.OpObject{Data: base64.StdEncoding.EncodeToString(data)}, nil)
	cl.end(cctx, 1, err)
	return err
}

func (s *HTTPStore) GetOp(ctx context.Context, kind, id string) ([]byte, error) {
	var out api.OpObject
	cctx, cl := s.begin(ctx, "vcs.client.op.get", "/v1/vcs/ops/{kind}/{object_id}")
	err := s.do(cctx, cl, http.MethodGet, "/v1/vcs/ops/"+url.PathEscape(kind)+"/"+url.PathEscape(id), nil, &out)
	cl.end(cctx, 1, err)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(out.Data)
}

func (s *HTTPStore) PutWorkspace(ctx context.Context, op, commit string, generation int64) error {
	cctx, cl := s.begin(ctx, "vcs.client.workspace.put", "/v1/vcs/workspace")
	err := s.do(cctx, cl, http.MethodPut, "/v1/vcs/workspace", api.SetWorkspaceRequest{Op: op, Commit: commit, Generation: generation}, nil)
	cl.end(cctx, 0, err)
	return err
}

func (s *HTTPStore) OpHeads(ctx context.Context) (api.OpHeads, error) {
	var out api.OpHeads
	cctx, cl := s.begin(ctx, "vcs.client.op_heads.get", "/v1/vcs/op-heads")
	err := s.do(cctx, cl, http.MethodGet, "/v1/vcs/op-heads", nil, &out)
	if cl.status == http.StatusNotFound && err != nil && strings.Contains(err.Error(), "vcs_not_found") {
		err, out = nil, api.OpHeads{Heads: []string{}}
	}
	cl.end(cctx, 0, err)
	return out, err
}

func (s *HTTPStore) SwapOpHeads(ctx context.Context, expect int64, heads []string) (api.OpHeads, error) {
	var out api.OpHeads
	cctx, cl := s.begin(ctx, "vcs.client.op_heads.swap", "/v1/vcs/op-heads")
	err := s.do(cctx, cl, http.MethodPut, "/v1/vcs/op-heads", api.SwapOpHeadsRequest{ExpectGeneration: expect, Heads: heads}, &out)
	cl.end(cctx, len(heads), err)
	return out, err
}
