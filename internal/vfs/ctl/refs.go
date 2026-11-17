package ctl

import (
	"context"
	"regexp"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/refname"
	"github.com/majorcontext/motte/internal/vfs/store"
)

var commitHex = regexp.MustCompile(`^[0-9a-f]{40}$`)

// serveOpHeads answers opheads: a read without heads, and a compare-and-swap on the generation expect with them.
func serveOpHeads(ctx context.Context, s store.OpHeadsStore, rq request) any {
	ctx, cancel := context.WithTimeout(ctx, store.OpHeadsBudget)
	defer cancel()
	var h api.OpHeads
	var err error
	if len(rq.Heads) == 0 {
		h, err = s.OpHeads(ctx)
	} else {
		h, err = s.SwapOpHeads(ctx, rq.Expect, rq.Heads)
	}
	if err != nil {
		return storeFail(err)
	}
	return opHeadsAnswer{h.Generation, nonNil(h.Heads)}
}

// serveRefs answers refs and swapref. A lost swap carries the stored ref, so the shim that loses the race can retry
// from the winner.
func serveRefs(ctx context.Context, s store.RefStore, rq request) any {
	ctx, cancel := context.WithTimeout(ctx, store.OpHeadsBudget)
	defer cancel()
	if rq.Op == "refs" && rq.Name == "" {
		refs, err := s.Refs(ctx)
		if err != nil {
			return storeFail(err)
		}
		return refsAnswer{nonNil(refs)}
	}
	switch {
	case rq.Name == "" || rq.Expect < 0:
		return invalid("refs: a ref needs a name and a non-negative expect")
	case !refname.Valid(rq.Name):
		return invalid("refs: invalid ref name")
	case rq.Op == "swapref" && !rq.Delete && !commitHex.MatchString(rq.Commit):
		return invalid("refs: commit must be 40 lowercase hex digits")
	case rq.Delete && rq.Expect < 1:
		return invalid("refs: a delete needs the stored generation in expect")
	}
	var r api.Ref
	var err error
	switch {
	case rq.Op == "refs":
		r, err = s.Ref(ctx, rq.Name)
	case rq.Delete:
		if err = s.DeleteRef(ctx, rq.Name, rq.Expect); err != nil {
			return storeFail(err)
		}
		return empty{}
	default:
		r, err = s.SwapRef(ctx, rq.Name, rq.Expect, rq.Commit)
	}
	if err != nil {
		return storeFail(err)
	}
	return refAnswer{r}
}
