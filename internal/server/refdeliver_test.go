package server_test

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/bucket/fs"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/meta"
	"github.com/majorcontext/motte/meta/sqlite"
)

const secret = "0123456789abcdef"

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// receivers records the deliveries by URL path and answers a status that a test sets.
type receivers struct {
	mu     sync.Mutex
	t      *testing.T
	seqs   map[string][]int64
	status int
	fail   int
}

func (r *receivers) RoundTrip(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	var ev api.Event
	_ = json.Unmarshal(body, &ev)
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Header.Get(api.HeaderSignature) != "sha256="+hex.EncodeToString(mac.Sum(nil)) || req.Header.Get(api.HeaderSequence) != strconv.FormatInt(ev.Seq, 10) {
		r.t.Errorf("delivery of seq %d has the signature %q and sequence %q", ev.Seq, req.Header.Get(api.HeaderSignature), req.Header.Get(api.HeaderSequence))
	}
	status := http.StatusNoContent
	if r.fail > 0 {
		r.fail--
		status = http.StatusInternalServerError
	} else {
		r.seqs[req.URL.Path] = append(r.seqs[req.URL.Path], ev.Seq)
	}
	return &http.Response{StatusCode: status, Body: http.NoBody, Header: http.Header{}}, nil
}

func (r *receivers) got(path string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprint(r.seqs[path])
}

type bubble struct {
	t     *testing.T
	store meta.Store
	rcv   *receivers
	keyP  ed25519.PublicKey
	key   ed25519.PrivateKey
}

func newBubble(t *testing.T) *bubble {
	t.Helper()
	db, err := sqlite.Open(context.Background(), sqlite.Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	return &bubble{t: t, store: db, rcv: &receivers{t: t, seqs: map[string][]int64{}}, keyP: pub, key: key}
}

// replica runs a server on the shared store until the returned stop func is called.
func (b *bubble) replica(name string, cfg server.EventsConfig) (stop func(), handler http.Handler) {
	root, err := fs.New(b.t.TempDir())
	if err != nil {
		b.t.Fatal(err)
	}
	cfg.Instance, cfg.HTTP = name, &http.Client{Transport: b.rcv}
	srv := server.New(server.Config{
		Meta: b.store, Bucket: root.Bucket("motte"), Events: cfg,
		Verifier: grant.NewVerifier(grant.Key{Kid: "cp", Public: b.keyP, Scopes: []grant.Scope{grant.ScopeOperator, grant.ScopeMerge}}),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	return func() { cancel(); <-done }, srv.Handler()
}

func (b *bubble) client(h http.Handler, c grant.Claims) *client.Client {
	c.Exp = time.Now().Add(1000 * 24 * time.Hour).Unix()
	tok, err := grant.Signer{Kid: "cp", Key: b.key}.Mint(c)
	if err != nil {
		b.t.Fatal(err)
	}
	return client.New("http://motte.invalid", client.StaticGrant(tok), client.WithHTTPClient(&http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Result(), nil
	})}))
}

func TestEventsAreDeliveredSignedInOrderAndOnceAcrossReplicas(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBubble(t)
		cfg := server.EventsConfig{Poll: 5 * time.Second}
		stopA, handler := b.replica("a", cfg)
		stopB, _ := b.replica("b", cfg)
		op := b.client(handler, grant.Claims{Scope: grant.ScopeOperator})
		repoA, repoB := b.client(handler, grant.Claims{Scope: grant.ScopeMerge, Repo: repoText}), b.client(handler, grant.Claims{Scope: grant.ScopeMerge, Repo: "gh:acme/other"})
		ctx := context.Background()
		for _, path := range []string{"all", "filtered"} {
			req := api.CreateSubscriptionRequest{URL: "https://recv.invalid/" + path, Secret: secret}
			if path == "filtered" {
				req.Repo = "gh:acme/other"
			}
			if _, err := op.CreateSubscription(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
		write := func(c *client.Client, name string) {
			if _, err := c.SwapRef(ctx, name, 0, commitA); err != nil {
				t.Fatal(err)
			}
		}
		write(repoA, "a1")
		write(repoB, "b1")
		write(repoA, "a2")
		time.Sleep(20 * time.Second)
		if got := b.rcv.got("/all"); got != "[1 2 3]" {
			t.Errorf("deliveries = %s, want each event once, in order, with two replicas running", got)
		}
		if got := b.rcv.got("/filtered"); got != "[2]" {
			t.Errorf("filtered deliveries = %s, want only the event of its repo", got)
		}

		b.rcv.fail = 3
		write(repoA, "a3")
		write(repoA, "a4")
		time.Sleep(2 * time.Minute)
		if got := b.rcv.got("/all"); got != "[1 2 3 4 5]" || b.rcv.fail != 0 {
			t.Errorf("deliveries = %s after refusals (left %d), want 4 and 5 once each, in order, after retries", got, b.rcv.fail)
		}

		stopA()
		stopB()
		write(repoA, "a5")
		stopC, _ := b.replica("c", cfg)
		time.Sleep(3 * time.Minute)
		if got := b.rcv.got("/all"); got != "[1 2 3 4 5 6]" {
			t.Errorf("deliveries = %s after a failover, want 6 once: the cursor survives and the new replica takes the expired lease", got)
		}
		stopC()
	})
}

func TestOldEventsArePrunedAndAnOlderCursorIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBubble(t)
		stop, handler := b.replica("a", server.EventsConfig{Retention: 24 * time.Hour, Poll: 30 * time.Minute})
		defer stop()
		op, merge := b.client(handler, grant.Claims{Scope: grant.ScopeOperator}), b.client(handler, grant.Claims{Scope: grant.ScopeMerge, Repo: repoText})
		ctx := context.Background()
		if _, err := merge.SwapRef(ctx, "a", 0, commitA); err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Hour)
		if _, err := merge.SwapRef(ctx, "b", 0, commitA); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Hour)
		_, err := op.Events(ctx, 0, 0)
		refuses(t, err, client.ErrEventsPruned, 410)
		if evs, err := op.Events(ctx, 1, 0); err != nil || len(evs) != 1 || evs[0].Ref != "b" {
			t.Errorf("events from the floor = %+v, %v, want the one newer event", evs, err)
		}
	})
}
