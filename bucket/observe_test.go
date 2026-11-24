package bucket_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/majorcontext/motte/bucket"
)

type failing struct {
	bucket.Store
	err error
}

func (f failing) GetRange(context.Context, string, int64, int64) ([]byte, error) { return nil, f.err }
func (f failing) Get(context.Context, string) ([]byte, error)                    { return nil, f.err }

func TestObserveClassifiesFailedReads(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		err  error
		kind string
		ok   bool
	}{
		{"deadline", fmt.Errorf("bucket: read: %w", context.DeadlineExceeded), bucket.KindTimeout, true},
		{"5xx", fmt.Errorf("bucket: read: %w", bucket.WithStatus(errors.New("boom"), 503)), bucket.Kind5xx, true},
		{"4xx", fmt.Errorf("bucket: read: %w", bucket.WithStatus(errors.New("denied"), 403)), bucket.KindOther, true},
		{"other", errors.New("reset"), bucket.KindOther, true},
		{"cancelled", context.Canceled, "", false},
		{"missing", bucket.ErrNotFound, "", false},
	} {
		var op string
		var got error
		s := bucket.Observe(failing{err: c.err}, func(o string, _ time.Duration, err error) { op, got = o, err })
		_, err := s.GetRange(context.Background(), "k", 0, 1)
		kind, ok := bucket.ErrorKind(got)
		if op != bucket.OpGetRange || err != c.err || kind != c.kind || ok != c.ok {
			t.Errorf("%s: op %q, err %v, kind %q, ok %v; want %q, %v", c.name, op, err, kind, ok, c.kind, c.ok)
		}
	}
}
