package vfs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/refname"
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

func (e *RefConflictError) Error() string { return conflictCode + ": " + e.Message }

const conflictCode = "vcs_generation_conflict"

var commitHex = regexp.MustCompile(`^[0-9a-f]{40}$`)

// ServeRefs answers the shim ops refs and swapref. The error carries the store code; a *RefConflictError also
// carries the stored ref, so the shim that loses the race can retry from the winner.
func ServeRefs(ctx context.Context, s RefStore, op, name string, expect int64, commit string, del bool) (refs []api.Ref, ref *api.Ref, err error) {
	if op == "refs" && name == "" {
		ctx, cancel := context.WithTimeout(ctx, opHeadsBudget)
		defer cancel()
		refs, err = s.Refs(ctx)
		return refs, nil, err
	}
	if name == "" || expect < 0 {
		return nil, nil, errors.New("refs: a ref needs a name and a non-negative expect")
	}
	if !refname.Valid(name) {
		return nil, nil, errors.New("refs: invalid ref name")
	}
	if op == "swapref" && !del && !commitHex.MatchString(commit) {
		return nil, nil, errors.New("refs: commit must be 40 lowercase hex digits")
	}
	if del && expect < 1 {
		return nil, nil, errors.New("refs: a delete needs the stored generation in expect")
	}
	ctx, cancel := context.WithTimeout(ctx, opHeadsBudget)
	defer cancel()
	var r api.Ref
	switch {
	case op == "refs":
		r, err = s.Ref(ctx, name)
	case del:
		return nil, nil, s.DeleteRef(ctx, name, expect)
	default:
		r, err = s.SwapRef(ctx, name, expect, commit)
	}
	if err != nil {
		return nil, nil, err
	}
	return nil, &r, nil
}

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
	if json.Unmarshal(se.body, &body) != nil || body.Error.Code != conflictCode {
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
