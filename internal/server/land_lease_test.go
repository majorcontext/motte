package server_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/meta/sqlite"
)

const batchLease = 6 * time.Second

// slowBucket runs hook before each write of an object.
type slowBucket struct {
	bucket.Store
	hook *atomic.Pointer[func(context.Context)]
}

func (b slowBucket) Unwrap() bucket.Store { return b.Store }

func (b slowBucket) Create(ctx context.Context, key string, data []byte) (bool, error) {
	if h := b.hook.Load(); h != nil {
		(*h)(ctx)
	}
	return b.Store.Create(ctx, key, data)
}

// leaseBubble is a server whose writes of objects run a hook, with the commits root, c1 and c2, and main at c1.
type leaseBubble struct {
	t          *testing.T
	srv        *server.Server
	meta       *sqlite.Store
	hook       *atomic.Pointer[func(context.Context)]
	merge      *client.Client
	root, c1   string
	c2         string
	stopServer func()
}

func newLeaseBubble(t *testing.T) *leaseBubble {
	t.Helper()
	ctx := context.Background()
	db, err := sqlite.Open(ctx, sqlite.Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	b := &leaseBubble{t: t, meta: db, hook: new(atomic.Pointer[func(context.Context)])}
	b.srv = server.New(server.Config{
		Meta: db, Bucket: slowBucket{root.Bucket("motte"), b.hook}, Events: server.EventsConfig{Instance: "a"},
		Lands:    server.LandsConfig{Lease: batchLease, Poll: time.Second, Timeout: 30 * time.Second},
		Verifier: grant.NewVerifier(grant.Key{Kid: "cp", Public: pub, Scopes: []grant.Scope{grant.ScopeImport, grant.ScopeMerge}}),
	})
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = b.srv.Run(run); close(done) }()
	b.stopServer = func() { cancel(); <-done; b.srv.Close() }
	mint := func(s grant.Scope) string {
		tok, err := grant.Signer{Kid: "cp", Key: key}.Mint(grant.Claims{Scope: s, Repo: repoText, Exp: time.Now().Add(time.Hour).Unix()})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	var o objSet
	b.root = o.commit(nil, "", "root")
	b.c1 = o.commit(map[string]string{"f1": "1"}, "", "one", b.root)
	b.c2 = o.commit(map[string]string{"f2": "2"}, "", "two", b.root)
	pack, idx, _ := gitpack.Build(o.objs)
	sum := sha256.Sum256(pack)
	if err := importPack(b.srv.Handler(), mint(grant.ScopeImport), hex.EncodeToString(sum[:]), idx, pack); err != nil {
		t.Fatal(err)
	}
	h := b.srv.Handler()
	b.merge = client.New("http://motte.invalid", client.StaticGrant(mint(grant.ScopeMerge)), client.WithHTTPClient(&http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Result(), nil
	})}))
	if _, err := b.merge.SwapRef(ctx, "main", 0, b.root); err != nil {
		t.Fatal(err)
	}
	if _, err := b.merge.Land(ctx, repoText, api.LandRequest{Ref: "main", Base: b.root, Head: b.c1}); err != nil {
		t.Fatal(err)
	}
	return b
}

// importPack uses a socket: the import route needs a connection that a recorder lacks.
func importPack(h http.Handler, tok, id string, idx, pack []byte) error {
	ts := httptest.NewServer(h)
	defer ts.Close()
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	return client.New(ts.URL, client.StaticGrant(tok), client.WithHTTPClient(hc)).ImportPack(context.Background(), id, idx, pack)
}

func (b *leaseBubble) tip() string {
	r, err := b.merge.Ref(context.Background(), "main")
	if err != nil {
		b.t.Fatal(err)
	}
	return r.Commit
}

func TestABatchLongerThanTheLeaseStillLands(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newLeaseBubble(t)
		var once atomic.Bool
		slow := func(context.Context) {
			if once.CompareAndSwap(false, true) {
				time.Sleep(4 * batchLease)
				now := time.Now()
				if _, err := b.meta.AcquireLandLease(context.Background(), repoText, "main", "probe", now, now.Add(time.Second)); err == nil {
					t.Error("the lease ended during the batch")
				}
			}
		}
		b.hook.Store(&slow)
		got, err := b.merge.Land(context.Background(), repoText, api.LandRequest{Ref: "main", Base: b.root, Head: b.c2})
		if err != nil || got.Tip != b.tip() || got.Tip == b.c1 {
			t.Errorf("land = %+v, %v, want the merge on main: the lease outlasts a batch that renews it", got, err)
		}
		b.stopServer()
	})
}

func TestABatchThatLostItsLeaseMovesNoRef(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newLeaseBubble(t)
		ctx := context.Background()
		var taken atomic.Bool
		slow := func(context.Context) {
			if taken.CompareAndSwap(false, true) {
				_ = b.meta.ReleaseLandLease(ctx, repoText, "main", "a")
				if _, err := b.meta.AcquireLandLease(ctx, repoText, "main", "thief", time.Now(), time.Now().Add(time.Hour)); err != nil {
					t.Error(err)
				}
			}
			time.Sleep(4 * batchLease)
		}
		b.hook.Store(&slow)
		_, err := b.merge.Land(ctx, repoText, api.LandRequest{Ref: "main", Base: b.root, Head: b.c2})
		if !errors.Is(err, client.ErrLandTimeout) || b.tip() != b.c1 {
			t.Errorf("land = %v, main = %s, want a timeout and main at %s: a lost lease moves no ref", err, b.tip(), b.c1)
		}
		b.stopServer()
	})
}
