package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	return &n, srv.URL
}

func TestCreateBoundsTheRetryOfAConflict(t *testing.T) {
	t.Parallel()
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
			t.Parallel()
			n, url := conflictServer(t, c.conflicts)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := s3.New(s3.Options{
				BaseEndpoint: aws.String(url), Region: "us-east-1", UsePathStyle: true,
				Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""),
			})
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
				cancel()
			}()
			for i, base := range c.wantBackoff {
				call := trap.MustWait(ctx)
				if call.Duration < base/2 || call.Duration > base {
					t.Errorf("wait %d = %v, want between %v and %v", i, call.Duration, base/2, base)
				}
				call.MustRelease(ctx)
				mock.Advance(call.Duration).MustWait(ctx)
			}
			extra := make(chan struct{})
			go func() {
				if _, err := trap.Wait(ctx); err == nil {
					close(extra)
				}
			}()
			var r result
			select {
			case r = <-done:
			case <-extra:
				t.Fatal("create waited after the last expected attempt")
			}
			if r.ok != c.created || (r.err != nil) != c.failed || n.Load() != c.requests {
				t.Errorf("create = %v, %v after %d requests; want %v, failed %v after %d", r.ok, r.err, n.Load(), c.created, c.failed, c.requests)
			}
		})
	}
}
