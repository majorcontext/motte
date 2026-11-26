package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/testserver"
)

// memBucket keeps objects in memory, so a benchmark measures the land and not the sync of a disk.
type memBucket struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func (b *memBucket) Create(_ context.Context, key string, data []byte) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.m[key]; ok {
		return false, nil
	}
	b.m[key] = append([]byte(nil), data...)
	return true, nil
}

func (b *memBucket) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if d, ok := b.m[key]; ok {
		return d, nil
	}
	return nil, bucket.ErrNotFound
}

func (b *memBucket) Version(ctx context.Context, key string) (string, error) {
	if _, err := b.Get(ctx, key); err != nil {
		return "", err
	}
	return "1", nil
}

func (b *memBucket) Delete(_ context.Context, key, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.m, key)
	return nil
}

func (b *memBucket) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	d, err := b.Get(ctx, key)
	if err != nil || off >= int64(len(d)) {
		return nil, err
	}
	if n < 0 || off+n > int64(len(d)) {
		n = int64(len(d)) - off
	}
	return d[off : off+n], nil
}

// BenchmarkLand reports lands per second for C agents that each land small changes on one ref from a stale base. The
// objects live on a filesystem bucket (fs, which syncs each pack) or in memory (mem). Run it once for each case:
// go test -run '^$' -bench Land -benchtime 1x ./internal/server
func BenchmarkLand(b *testing.B) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, store := range []string{"fs", "mem"} {
			for _, c := range []int{10, 100, 1000} {
				b.Run(fmt.Sprintf("%s/%s/C=%d", backend, store, c), func(b *testing.B) { benchLand(b, backend, store, c) })
			}
		}
	}
}

func benchLand(b *testing.B, backend, store string, agents int) {
	var ts *testserver.Server
	if backend == "sqlite" {
		ts = testserver.StartLocal(b)
	} else {
		ts = testserver.Start(b, testserver.WithPostgres(""))
	}
	if store == "mem" {
		srv := httptest.NewServer(server.New(server.Config{Meta: ts.Meta, Bucket: &memBucket{m: map[string][]byte{}}, Verifier: ts.Verifier}).Handler())
		b.Cleanup(srv.Close)
		ts = &testserver.Server{URL: srv.URL, Meta: ts.Meta, Verifier: ts.Verifier, Operator: ts.Operator, Node: ts.Node, Expire: ts.Expire}
	}
	e := &env{Server: ts, url: ts.URL, cp: ts.Operator, node: ts.Node}
	l := &landEnv{env: e, repo: repoText}
	e.t = &testing.T{}
	total := max(2000, 3*agents)
	perAgent := total / agents
	var o objSet
	l.root = o.commit(nil, "", "root")
	heads := make([]string, agents*perAgent)
	for i := range heads {
		heads[i] = o.commit(map[string]string{fmt.Sprint("dir", i%16, "/file", i): "x"}, "", fmt.Sprint("land ", i), l.root)
		if len(o.objs) >= 6000 {
			o.upload(b, e, repoText)
		}
	}
	o.upload(b, e, repoText)
	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: agents, MaxConnsPerHost: 0}}
	merge := client.New(ts.URL, client.StaticGrant(ts.MergeGrant(b, repoText)), client.WithHTTPClient(hc), client.WithBudget(0))
	if _, err := merge.SwapRef(context.Background(), "main", 0, l.root); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	for a := range agents {
		wg.Go(func() {
			for i := range perAgent {
				if _, err := merge.Land(context.Background(), repoText, api.LandRequest{Ref: "main", Base: l.root, Head: heads[a*perAgent+i]}); err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.ReportMetric(float64(len(heads))/time.Since(start).Seconds(), "lands/s")
}
