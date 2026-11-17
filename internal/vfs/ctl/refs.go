package ctl

import (
	"context"
	"errors"
	"regexp"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/refname"
	"github.com/majorcontext/motte/internal/vfs/store"
)

var commitHex = regexp.MustCompile(`^[0-9a-f]{40}$`)

// serveRefs answers the shim ops refs and swapref. The error carries the store code; a *RefConflictError also
// carries the stored ref, so the shim that loses the race can retry from the winner.
func serveRefs(ctx context.Context, s store.RefStore, op, name string, expect int64, commit string, del bool) (refs []api.Ref, ref *api.Ref, err error) {
	if op == "refs" && name == "" {
		ctx, cancel := context.WithTimeout(ctx, store.OpHeadsBudget)
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
	ctx, cancel := context.WithTimeout(ctx, store.OpHeadsBudget)
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
