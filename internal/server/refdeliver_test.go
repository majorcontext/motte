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
	"sort"
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
	evs    map[string][]api.Event
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
	if req.Header.Get(api.HeaderSignature) != "sha256="+hex.EncodeToString(mac.Sum(nil)) || req.Header.Get(api.HeaderSequence) != strconv.FormatInt(ev.Seq, 10) || req.Header.Get(api.HeaderPartition) != strconv.Itoa(ev.Partition) {
		r.t.Errorf("delivery of %d/%d has the signature %q, partition %q and sequence %q", ev.Partition, ev.Seq, req.Header.Get(api.HeaderSignature), req.Header.Get(api.HeaderPartition), req.Header.Get(api.HeaderSequence))
	}
	status := http.StatusNoContent
	if r.fail > 0 {
		r.fail--
		status = http.StatusInternalServerError
	} else {
		r.evs[req.URL.Path] = append(r.evs[req.URL.Path], ev)
	}
	return &http.Response{StatusCode: status, Body: http.NoBody, Header: http.Header{}}, nil
}

// got returns the sorted ref names that a receiver got. It reports a repeat, and a partition whose events did not
// arrive in order with no gap.
func (r *receivers) got(path string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := map[int]int64{}
	var refs []string
	for _, e := range r.evs[path] {
		if last[e.Partition] != 0 && e.Seq != last[e.Partition]+1 {
			r.t.Errorf("%s: partition %d delivered seq %d after %d", path, e.Partition, e.Seq, last[e.Partition])
		}
		last[e.Partition] = e.Seq
		refs = append(refs, e.Ref)
	}
	sort.Strings(refs)
	return fmt.Sprint(refs)
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
	return &bubble{t: t, store: db, rcv: &receivers{t: t, evs: map[string][]api.Event{}}, keyP: pub, key: key}
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
	t.Parallel()
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
		first, err := repoA.SwapRef(ctx, "a1", 0, commitA)
		if err != nil {
			t.Fatal(err)
		}
		second, err := repoA.SwapRef(ctx, "a1", first.Generation, commitB)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repoA.SwapRef(ctx, "a1", second.Generation, commitA); err != nil {
			t.Fatal(err)
		}
		write(repoB, "b1")
		write(repoA, "a2")
		time.Sleep(20 * time.Second)
		if got := b.rcv.got("/all"); got != "[a1 a1 a1 a2 b1]" {
			t.Errorf("deliveries = %s, want each event once, in order, with two replicas running", got)
		}
		if got := b.rcv.got("/filtered"); got != "[b1]" {
			t.Errorf("filtered deliveries = %s, want only the event of its repo", got)
		}

		b.rcv.fail = 3
		write(repoA, "a3")
		write(repoA, "a4")
		time.Sleep(2 * time.Minute)
		if got := b.rcv.got("/all"); got != "[a1 a1 a1 a2 a3 a4 b1]" || b.rcv.fail != 0 {
			t.Errorf("deliveries = %s after refusals (left %d), want a3 and a4 once each after retries", got, b.rcv.fail)
		}

		stopA()
		stopB()
		write(repoA, "a5")
		stopC, _ := b.replica("c", cfg)
		time.Sleep(3 * time.Minute)
		if got := b.rcv.got("/all"); got != "[a1 a1 a1 a2 a3 a4 a5 b1]" {
			t.Errorf("deliveries = %s after a failover, want a5 once: the cursor survives and the new replica takes the expired lease", got)
		}
		stopC()
	})
}

func TestOldEventsArePrunedAndAnOlderCursorIsRefused(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newBubble(t)
		stop, handler := b.replica("a", server.EventsConfig{Retention: 24 * time.Hour, Poll: 30 * time.Minute})
		defer stop()
		op, merge := b.client(handler, grant.Claims{Scope: grant.ScopeOperator}), b.client(handler, grant.Claims{Scope: grant.ScopeMerge, Repo: repoText})
		ctx := context.Background()
		if _, err := merge.SwapRef(ctx, "a", 0, commitA); err != nil {
			t.Fatal(err)
		}
		_, older, err := op.Events(ctx, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Hour)
		if _, err := merge.SwapRef(ctx, "b", 0, commitA); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Hour)
		_, _, err = op.Events(ctx, "", 0)
		refuses(t, err, client.ErrEventsPruned, 410)
		if evs, _, err := op.Events(ctx, older, 0); err != nil || len(evs) != 1 || evs[0].Ref != "b" {
			t.Errorf("events from the floor = %+v, %v, want the one newer event", evs, err)
		}
	})
}

func TestAnEventOfThisReplicaIsDeliveredBeforeThePoll(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newBubble(t)
		stop, handler := b.replica("a", server.EventsConfig{Poll: time.Hour})
		defer stop()
		op, merge := b.client(handler, grant.Claims{Scope: grant.ScopeOperator}), b.client(handler, grant.Claims{Scope: grant.ScopeMerge, Repo: repoText})
		ctx := context.Background()
		if _, err := op.CreateSubscription(ctx, api.CreateSubscriptionRequest{URL: "https://recv.invalid/all", Secret: secret}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		start := time.Now()
		ref, err := merge.SwapRef(ctx, "a", 0, commitA)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := merge.SwapRef(ctx, "a", ref.Generation, commitB); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if got, waited := b.rcv.got("/all"), time.Since(start); got != "[a a]" || waited != 0 {
			t.Errorf("deliveries = %s after %v of fake time, want both at once, with no wait for the hourly poll", got, waited)
		}
	})
}
