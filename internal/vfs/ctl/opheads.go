package ctl

import (
	"context"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/vfs/store"
)

// ServeOpHeads answers the shim op opheads: a read without heads, and a
// compare-and-swap on the generation expect with them. The error carries the
// store code, so a shim that loses the race sees vcs_generation_conflict.
func ServeOpHeads(ctx context.Context, s store.OpHeadsStore, expect int64, heads []string) (api.OpHeads, error) {
	ctx, cancel := context.WithTimeout(ctx, store.OpHeadsBudget)
	defer cancel()
	if len(heads) == 0 {
		return s.OpHeads(ctx)
	}
	return s.SwapOpHeads(ctx, expect, heads)
}
