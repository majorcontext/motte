package mem

import (
	"testing"

	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/bucket/buckettest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	buckettest.Run(t, func(*testing.T) bucket.Store { return New() }, buckettest.Caps{ConditionalDelete: true})
}
