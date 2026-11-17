package store

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/api"
)

// RefStore reads and swaps the refs of the native repo that the grant names.
type RefStore interface {
	Refs(ctx context.Context) ([]api.Ref, error)
	Ref(ctx context.Context, name string) (api.Ref, error)
	SwapRef(ctx context.Context, name string, expect int64, commit string) (api.Ref, error)
	DeleteRef(ctx context.Context, name string, expect int64) error
}

// RefConflictError is a lost ref swap or delete. Ref is the stored ref, nil when none exists.
type RefConflictError struct {
	Message string
	Ref     *api.Ref
}

func (e *RefConflictError) Error() string { return ConflictCode + ": " + e.Message }

const ConflictCode = "vcs_generation_conflict"

func refPath(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return "/v1/vcs/refs/" + strings.Join(parts, "/")
}

func (s *HTTPStore) Refs(ctx context.Context) ([]api.Ref, error) {
	var out api.Refs
	cctx, cl := s.begin(ctx, "vcs.client.refs.list", "/v1/vcs/refs")
	err := s.do(cctx, cl, http.MethodGet, "/v1/vcs/refs", nil, &out)
	cl.end(cctx, len(out.Refs), err)
	return out.Refs, err
}

func (s *HTTPStore) Ref(ctx context.Context, name string) (api.Ref, error) {
	var out api.Ref
	cctx, cl := s.begin(ctx, "vcs.client.ref.get", "/v1/vcs/refs/{name}")
	err := s.do(cctx, cl, http.MethodGet, refPath(name), nil, &out)
	cl.end(cctx, 1, err)
	return out, err
}

func (s *HTTPStore) SwapRef(ctx context.Context, name string, expect int64, commit string) (api.Ref, error) {
	var out api.Ref
	cctx, cl := s.begin(ctx, "vcs.client.ref.swap", "/v1/vcs/refs/{name}")
	err := s.do(cctx, cl, http.MethodPut, refPath(name), api.SwapRefRequest{Expect: expect, Commit: commit}, &out)
	cl.end(cctx, 1, err)
	return out, refConflict(err)
}

func (s *HTTPStore) DeleteRef(ctx context.Context, name string, expect int64) error {
	cctx, cl := s.begin(ctx, "vcs.client.ref.delete", "/v1/vcs/refs/{name}")
	err := s.do(cctx, cl, http.MethodDelete, refPath(name)+"?expect="+strconv.FormatInt(expect, 10), nil, nil)
	cl.end(cctx, 1, err)
	return refConflict(err)
}

// refConflict turns the 409 vcs_generation_conflict answer into a *RefConflictError; any other error stays.
func refConflict(err error) error {
	var se *statusError
	if !errors.As(err, &se) || se.status != http.StatusConflict {
		return err
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Details struct {
				Ref *api.Ref `json:"ref"`
			} `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(se.body, &body) != nil || body.Error.Code != ConflictCode {
		return err
	}
	return &RefConflictError{Message: body.Error.Message, Ref: body.Error.Details.Ref}
}

// statusError is a non-2xx store answer, with its body for a caller that reads the error envelope.
type statusError struct {
	error
	status int
	body   []byte
}

func (e *statusError) Unwrap() error { return e.error }
