package s3

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/quartz"
)

const ms = time.Millisecond

func conflictServer(t *testing.T, conflicts int) (*atomic.Int32, string) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(n.Add(1)) <= conflicts {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`<Error><Code>ConditionalRequestConflict</Code><Message>in flight</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	return &n, srv.URL
}

func TestCreateBoundsTheRetryOfAConflict(t *testing.T) {
	for name, c := range map[string]struct {
		conflicts   int
		created     bool
		failed      bool
		requests    int32
		wantBackoff []time.Duration
	}{
		"succeeds after conflicts": {3, true, false, 4, []time.Duration{20 * ms, 40 * ms, 80 * ms}},
		"fails after the bound":    {100, false, true, 8, []time.Duration{20 * ms, 40 * ms, 80 * ms, 160 * ms, 320 * ms, 640 * ms, time.Second}},
	} {
		t.Run(name, func(t *testing.T) {
			n, url := conflictServer(t, c.conflicts)
			ctx := t.Context()
			client, err := NewClient(ctx, Options{Endpoint: url, Region: "us-east-1", PathStyle: true})
			if err != nil {
				t.Fatal(err)
			}
			mock := quartz.NewMock(t)
			trap := mock.Trap().NewTimer()
			defer trap.Close()
			b := s3Bucket{client, "b", mock}
			type result struct {
				ok  bool
				err error
			}
			done := make(chan result, 1)
			go func() {
				ok, err := b.Create(ctx, "k", []byte("x"))
				done <- result{ok, err}
			}()
			for i, base := range c.wantBackoff {
				call := trap.MustWait(ctx)
				if call.Duration < base/2 || call.Duration > base {
					t.Errorf("wait %d = %v, want between %v and %v", i, call.Duration, base/2, base)
				}
				call.MustRelease(ctx)
				mock.Advance(call.Duration).MustWait(ctx)
			}
			r := <-done
			if r.ok != c.created || (r.err != nil) != c.failed || n.Load() != c.requests {
				t.Errorf("create = %v, %v after %d requests; want %v, failed %v after %d", r.ok, r.err, n.Load(), c.created, c.failed, c.requests)
			}
		})
	}
}
