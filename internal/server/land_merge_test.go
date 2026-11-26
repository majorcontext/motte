package server_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/server"
)

func tenLines(edits map[int]string) string {
	var b strings.Builder
	for i := 1; i <= 10; i++ {
		if e, ok := edits[i]; ok {
			b.WriteString(e + "\n")
		} else {
			fmt.Fprintf(&b, "line %d\n", i)
		}
	}
	return b.String()
}

func landMergesLinesThatTwoLandsChangeApart(t *testing.T, l *landEnv) {
	var o objSet
	with := func(f string) map[string]string { return map[string]string{"f": f, "g": "1"} }
	start := o.commit(with(tenLines(nil)), "", "start", l.root)
	tip := o.commit(with(tenLines(map[int]string{2: "tip"})), "", "tip", start)
	apart := o.commit(with(tenLines(map[int]string{8: "mine"})), "", "apart", start)
	same := o.commit(with(tenLines(map[int]string{2: "mine"})), "", "same", start)
	o.upload(t, l.env, l.repo)
	for _, c := range []string{start, tip} {
		if _, err := l.land(l.merge, "main", l.root, c); err != nil {
			t.Fatal(err)
		}
	}
	both := o.tree(with(tenLines(map[int]string{2: "tip", 8: "mine"})))
	if r, err := l.landExpecting(start, apart, both); err != nil || len(r.Commits) != 1 || l.treeOf(r.Tip) != both {
		t.Fatalf("land of other lines = %+v, %v, want the tree with both edits", r, err)
	}
	at := l.tip("main")
	_, err := l.land(l.merge, "main", start, same)
	refuses(t, err, client.ErrLandConflict, 409)
	if got := client.ConflictPaths(err); !slices.Equal(got, []string{"f"}) || l.tip("main") != at {
		t.Errorf("same lines: paths = %v, main = %+v, want [f] and %+v", got, l.tip("main"), at)
	}
}

func TestLandResultStaysUntilTheCallerAcknowledgesIt(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		ctx := context.Background()
		var o objSet
		c1 := o.commit(map[string]string{"a": "1"}, "", "c1", l.root)
		o.upload(t, l.env, l.repo)
		req := api.LandRequest{ID: "ack-land-1", Ref: "main", Base: l.root, Head: c1}
		first, err := l.merge.Land(ctx, l.repo, req)
		again, err2 := l.merge.Land(ctx, l.repo, req)
		if err != nil || err2 != nil || again.Tip != first.Tip || again.Generation != first.Generation {
			t.Fatalf("repeat = %+v, %v, %v; want the first result %+v", again, err, err2, first)
		}
		refuses(t, l.reader.AckLand(ctx, l.repo, req.ID), client.ErrRepoForbidden, 403)
		refuses(t, l.merge.AckLand(ctx, "gh:acme/other", req.ID), client.ErrRepoForbidden, 403)
		if err := l.merge.AckLand(ctx, l.repo, req.ID); err != nil {
			t.Fatal(err)
		}
		refuses(t, l.merge.AckLand(ctx, l.repo, req.ID), client.ErrNotFound, 404)
	})
}

func TestARequestIgnoresTheOutcomeOfAnEarlierLandWithItsId(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		ctx := context.Background()
		now := time.Now()
		if err := l.Meta.AcquireLandLease(ctx, l.repo, "main", "other-replica", now, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		replica := server.New(server.Config{Meta: l.Meta, Bucket: l.Bucket, Verifier: l.Verifier, Events: server.EventsConfig{Instance: "replica-4"},
			Lands: server.LandsConfig{Lease: time.Hour, Poll: time.Millisecond, Timeout: 50 * time.Millisecond}})
		ts := httptest.NewServer(replica.Handler())
		defer ts.Close()
		var o objSet
		c1 := o.commit(map[string]string{"a": "1"}, "", "c1", l.root)
		o.upload(t, l.env, l.repo)
		done := make(chan error, 1)
		go func() {
			_, err := client.New(ts.URL, client.StaticGrant(l.MergeGrant(t, l.repo))).Land(ctx, l.repo, api.LandRequest{ID: "reused-id-1", Ref: "main", Base: l.root, Head: c1})
			done <- err
		}()
		for {
			select {
			case err := <-done:
				refuses(t, err, client.ErrLandTimeout, 504)
				return
			default:
				server.TellOldLand(replica, l.repo, "reused-id-1")
			}
		}
	})
}
