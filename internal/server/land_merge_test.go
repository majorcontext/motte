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

func TestCloseEndsALeaderThatWaitsForAnotherLease(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		ctx := context.Background()
		now := time.Now()
		if err := l.Meta.AcquireLandLease(ctx, l.repo, "main", "other-replica", now, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		replica := server.New(server.Config{Meta: l.Meta, Bucket: l.Bucket, Verifier: l.Verifier, Events: server.EventsConfig{Instance: "replica-3"},
			Lands: server.LandsConfig{Lease: time.Hour, Poll: time.Millisecond, Timeout: 50 * time.Millisecond}})
		ts := httptest.NewServer(replica.Handler())
		defer ts.Close()
		var o objSet
		c1 := o.commit(map[string]string{"a": "1"}, "", "c1", l.root)
		o.upload(t, l.env, l.repo)
		_, err := client.New(ts.URL, client.StaticGrant(l.MergeGrant(t, l.repo))).Land(ctx, l.repo, api.LandRequest{Ref: "main", Base: l.root, Head: c1})
		refuses(t, err, client.ErrLandTimeout, 504)
		closed := make(chan struct{})
		go func() { replica.Close(); close(closed) }()
		<-closed
	})
}

func TestLandIgnoresAPathThatTheTargetAddedAndRemoved(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		adds := o.commit(map[string]string{"x": "1"}, "", "add x", l.root)
		removes := o.commit(nil, "", "remove x", adds)
		mine := o.commit(map[string]string{"x": "1", "y": "2"}, "", "mine", l.root)
		o.upload(t, l.env, l.repo)
		land := func(base, head string, want []string) {
			if r, err := l.land(l.merge, "main", base, head); err != nil || want != nil && !slices.Equal(l.files(r.Tip), want) {
				t.Fatalf("land of %s = %+v, %v, want %v", head, r, err, want)
			}
		}
		land(l.root, adds, nil)
		land(adds, removes, nil)
		land(l.root, mine, []string{"x", "y"})
		start := l.tip("main").Commit
		asFile := o.commit(map[string]string{"x": "1", "y": "2", "d": "1"}, "", "d as a file", start)
		asDir := o.commit(map[string]string{"x": "1", "y": "2", "d/y": "1"}, "", "d as a directory", asFile)
		beside := o.commit(map[string]string{"x": "1", "y": "2", "d/z": "1"}, "", "beside", start)
		o.upload(t, l.env, l.repo)
		land(start, asFile, nil)
		land(asFile, asDir, nil)
		land(start, beside, []string{"d/", "x", "y"})
	})
}
