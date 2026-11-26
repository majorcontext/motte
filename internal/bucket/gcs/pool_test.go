package gcs_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/majorcontext/motte/internal/bucket/gcs"
)

type countingTransport struct{ n atomic.Int64 }

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return http.DefaultTransport.RoundTrip(r)
}

func TestAPoolReadsThroughEveryClientAndWritesThroughTheFirst(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("data"))
			return
		}
		_, _ = w.Write([]byte(`{"name":"k"}`))
	}))
	defer srv.Close()
	var counts [3]*countingTransport
	var clients []*storage.Client
	for i := range counts {
		counts[i] = &countingTransport{}
		c, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: counts[i]}))
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
	}
	b, err := gcs.NewPool(clients, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Create(context.Background(), "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := counts[0].n.Load(); got != 1 || counts[1].n.Load() != 0 || counts[2].n.Load() != 0 {
		t.Fatalf("a write used the clients %d, %d, %d times, want 1, 0, 0", got, counts[1].n.Load(), counts[2].n.Load())
	}
	for range 6 {
		if _, err := b.GetRange(context.Background(), "k", 0, 4); err != nil {
			t.Fatal(err)
		}
	}
	if a, b, c := counts[0].n.Load(), counts[1].n.Load(), counts[2].n.Load(); a != 3 || b != 2 || c != 2 {
		t.Errorf("6 reads and 1 write used the clients %d, %d, %d times, want 3, 2, 2", a, b, c)
	}
	if _, err := gcs.NewPool(nil, "bucket"); err == nil {
		t.Error("an empty pool was accepted")
	}
}
