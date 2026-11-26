package land

import (
	"context"

	"github.com/majorcontext/motte/internal/gitpack"
)

// warmBytes bounds the object bytes that Warm reads.
const warmBytes = 4 << 20

// Warm reads the commits of base..head and the trees that each commit changes, which a land of the range reads again,
// so that the leader finds them near. It reads at most warmBytes and returns the first error.
func Warm(ctx context.Context, src Source, base, head gitpack.SHA) error {
	b := NewBatch(src, func(retained int64) error {
		if retained > warmBytes {
			return ErrTooLarge
		}
		return nil
	}, base, nil, Options{})
	baseCommit, err := b.commit(ctx, base)
	if err != nil {
		return err
	}
	chain, err := b.walk(ctx, base, head)
	if err != nil {
		return err
	}
	_, _, err = b.scan(ctx, baseCommit, chain, false)
	return err
}
