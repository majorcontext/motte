package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/internal/testhook"
	"github.com/majorcontext/motte/testserver"
)

func TestRangeCacheBytesZeroTurnsTheCacheOff(t *testing.T) {
	t.Parallel()
	ts := testserver.Start(t)
	pack, idx, names := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: []byte("cached or not")}})
	sum := sha256.Sum256(pack)
	if err := ts.Client(ts.ImportGrant(t, repoText)).ImportPack(context.Background(), hex.EncodeToString(sum[:]), idx, pack); err != nil {
		t.Fatal(err)
	}
	for size, wantCached := range map[int64]bool{0: false, 64 << 20: true} {
		srv := server.New(server.Config{Meta: testhook.Of(ts).Meta, Bucket: testhook.Of(ts).Bucket, Verifier: testhook.Of(ts).Verifier, RangeCacheBytes: size})
		hs := func() string { hs := httptest.NewServer(srv.Handler()); t.Cleanup(hs.Close); return hs.URL }()
		read := client.New(hs, client.StaticGrant(ts.ReadGrant(t, repoText)))
		if _, err := read.Object(t.Context(), names[0].String()); err != nil {
			t.Fatal(err)
		}
		res, err := http.Get(hs + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		m := regexp.MustCompile(`motte_range_cache_bytes (\d+)`).FindSubmatch(body)
		if m == nil {
			t.Fatal("no motte_range_cache_bytes metric")
		}
		if n, _ := strconv.Atoi(string(m[1])); (n > 0) != wantCached {
			t.Errorf("RangeCacheBytes %d: cache holds %d bytes, want cached = %v", size, n, wantCached)
		}
	}
}
