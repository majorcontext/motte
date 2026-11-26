package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/quartz"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/bucket"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/midx"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/testserver"
)

// idxBucket holds every read of a pack index until gate closes, or fails it while fail is set.
type idxBucket struct {
	bucket.Store
	gate    chan struct{}
	entered chan struct{}
	reads   atomic.Int32
	fail    atomic.Bool
}

func (b *idxBucket) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, ".idx") {
		b.reads.Add(1)
		select {
		case b.entered <- struct{}{}:
		default:
		}
		if b.fail.Load() {
			return nil, errors.New("bucket down")
		}
		select {
		case <-b.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.Store.Get(ctx, key)
}

// coldReplica is a second replica on the state of a store that holds one imported repo, which has no repos row. It is cold until Warm returns.
func coldReplica(t *testing.T, clk quartz.Clock) (*idxBucket, *server.Server, func(context.Context) error, func() int) {
	t.Helper()
	ts := testserver.StartLocal(t)
	pack, idx, names := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: []byte("hello")}})
	sum := sha256.Sum256(pack)
	if err := ts.Client(ts.ImportGrant(t, repoText)).ImportPack(context.Background(), hex.EncodeToString(sum[:]), idx, pack); err != nil {
		t.Fatal(err)
	}
	b := &idxBucket{Store: ts.Bucket, gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	srv := server.New(server.Config{Meta: ts.Meta, Bucket: b, Verifier: ts.Verifier, Warm: true, Clock: clk})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	ready := func() int {
		res, err := http.Get(hs.URL + "/ready")
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		return res.StatusCode
	}
	read := client.New(hs.URL, client.StaticGrant(ts.ReadGrant(t, repoText)))
	return b, srv, func(ctx context.Context) error { _, err := read.Object(ctx, names[0].String()); return err }, ready
}

func TestReadyWaitsForPackIndexesAndFirstReadFetchesNone(t *testing.T) {
	t.Parallel()
	b, srv, read, ready := coldReplica(t, nil)
	done := make(chan struct{})
	go func() { srv.Warm(t.Context()); close(done) }()
	<-b.entered
	if got := ready(); got != http.StatusServiceUnavailable {
		t.Fatalf("ready while an index is unread = %d, want 503", got)
	}
	close(b.gate)
	<-done
	if got := ready(); got != http.StatusOK {
		t.Fatalf("ready after warm-up = %d, want 200", got)
	}
	before := b.reads.Load()
	if err := read(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := b.reads.Load(); got != before {
		t.Errorf("first read fetched %d pack indexes, want 0", got-before)
	}
}

func TestWarmFailureKeepsTheReplicaNotReadyUntilARetrySucceeds(t *testing.T) {
	t.Parallel()
	clk := quartz.NewMock(t)
	retry := clk.Trap().NewTimer()
	defer retry.Close()
	b, srv, _, ready := coldReplica(t, clk)
	b.fail.Store(true)
	done := make(chan struct{})
	go func() { srv.Warm(t.Context()); close(done) }()
	retry.MustWait(t.Context()).MustRelease(t.Context())
	if got := ready(); got != http.StatusServiceUnavailable {
		t.Fatalf("ready after a failed warm-up = %d, want 503", got)
	}
	b.fail.Store(false)
	close(b.gate)
	clk.Advance(5 * 1e9).MustWait(t.Context())
	<-done
	if got := ready(); got != http.StatusOK {
		t.Fatalf("ready after the retry = %d, want 200", got)
	}
}

func TestWarmAddsAListedPackThatNoLayerNamesAndLogsTheCount(t *testing.T) {
	t.Parallel()
	ts := testserver.StartLocal(t)
	pack, idx, _ := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: []byte("listed")}})
	sum := sha256.Sum256(pack)
	id := hex.EncodeToString(sum[:])
	key := "repos/gh/" + base64.RawURLEncoding.EncodeToString([]byte("acme/widgets")) + "/" + id
	for k, v := range map[string][]byte{key + ".idx": idx, key + ".pack": pack} {
		if _, err := ts.Bucket.Create(t.Context(), k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := ts.Meta.AddPack(t.Context(), repoText, id, int64(len(pack))); err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	srv := server.New(server.Config{Meta: ts.Meta, Bucket: ts.Bucket, Verifier: ts.Verifier, Warm: true, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	srv.Warm(t.Context())
	row, err := ts.Meta.Manifest(t.Context(), repoText)
	if err != nil {
		t.Fatal(err)
	}
	if m, _ := midx.DecodeManifest(row.Body); len(m.Layers) != 1 {
		t.Errorf("manifest has %d layers after Warm, want the 1 of the listed pack", len(m.Layers))
	}
	if !strings.Contains(logs.String(), "layers_added=1") || !strings.Contains(logs.String(), "failed=0") {
		t.Errorf("log %q lacks layers_added=1 and failed=0", logs.String())
	}
}
