package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
)

func TestProtectedRefsAdmitOnlyAMergeGrant(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.want(e.claim("ws1", 1, repoText), 200, "")
	op, node, merge := e.Client(e.operator()), e.Client(e.nodeGrant("ws1", 1)), e.Client(e.MergeGrant(t, repoText))
	main, err := node.SwapRef(ctx, "main", 0, commitA)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := node.SwapRef(ctx, "release/1", 0, commitA)
	if err != nil {
		t.Fatal(err)
	}
	if err := op.SetProtectedRefs(ctx, repoText, []string{"main", "release/*"}); err != nil {
		t.Fatal(err)
	}
	if got, err := op.ProtectedRefs(ctx, repoText); err != nil || fmt.Sprint(got) != "[main release/*]" {
		t.Fatalf("patterns = %v, %v", got, err)
	}
	_, err = node.SwapRef(ctx, "main", main.Generation, commitB)
	refuses(t, err, client.ErrRefProtected, 403)
	_, err = node.SwapRef(ctx, "release/2", 0, commitB)
	refuses(t, err, client.ErrRefProtected, 403)
	refuses(t, node.DeleteRef(ctx, "release/1", rel.Generation), client.ErrRefProtected, 403)
	if _, err := node.SwapRef(ctx, "feature", 0, commitB); err != nil {
		t.Errorf("an unprotected ref: %v", err)
	}
	if got, err := node.Ref(ctx, "main"); err != nil || got != main {
		t.Fatalf("main = %v, %v, want it unchanged", got, err)
	}

	_, err = merge.SwapRef(ctx, "main", main.Generation+100, commitB)
	refuses(t, err, client.ErrGenerationConflict, 409)
	if cur, ok := client.CurrentRef(err); !ok || cur != main {
		t.Errorf("conflict carries %v, want %v", cur, main)
	}
	moved, err := merge.SwapRef(ctx, "main", main.Generation, commitB)
	if err != nil || moved.Commit != commitB || moved.Generation <= main.Generation {
		t.Fatalf("merge swap = %v, %v", moved, err)
	}
	if _, err := merge.SwapRef(ctx, "release/2", 0, commitB); err != nil {
		t.Errorf("merge create: %v", err)
	}
	if err := merge.DeleteRef(ctx, "release/1", rel.Generation); err != nil {
		t.Errorf("merge delete: %v", err)
	}
	if err := op.SetProtectedRefs(ctx, repoText, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := node.SwapRef(ctx, "main", moved.Generation, commitA); err != nil {
		t.Errorf("after the list is cleared: %v", err)
	}
}

func TestProtectedRefsRouteRules(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.want(e.claim("ws1", 1, repoText), 200, "")
	op := e.Client(e.operator())
	for _, bad := range [][]string{{""}, {"a b"}, {"../x"}, {"a/**/b"}, make([]string, 101)} {
		refuses(t, op.SetProtectedRefs(ctx, repoText, bad), client.ErrInvalidBody, 400)
	}
	refuses(t, op.SetProtectedRefs(ctx, "no-colon", []string{"main"}), client.ErrInvalidBody, 400)
	for name, c := range map[string]*client.Client{"node": e.Client(e.nodeGrant("ws1", 1)), "merge": e.Client(e.MergeGrant(t, repoText)), "read": e.Client(e.readGrant(repoText))} {
		refuses(t, c.SetProtectedRefs(ctx, repoText, []string{"main"}), client.ErrUnauthorized, 401)
		_, err := c.ProtectedRefs(ctx, repoText)
		if !strings.Contains(fmt.Sprint(err), "401") {
			t.Errorf("%s grant read the list: %v", name, err)
		}
	}
	if got, err := op.ProtectedRefs(ctx, repoText); err != nil || len(got) != 0 {
		t.Errorf("a repo with none = %v, %v", got, err)
	}
}

func TestMergeGrantStaysInItsRepoAndCheckedKey(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	other := e.Client(e.MergeGrant(t, "gh:acme/other"))
	if _, err := other.SwapRef(ctx, "main", 0, commitA); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Client(e.readGrant(repoText)).Ref(ctx, "main"); err == nil {
		t.Error("a merge grant of one repo wrote a ref of another")
	}
	if got, err := e.Client(e.MergeGrant(t, "gh:acme/other")).Ref(ctx, "main"); err != nil || got.Commit != commitA {
		t.Errorf("a merge grant reads its refs: %v, %v", got, err)
	}
	forged := e.mint(e.node, grant.Claims{Scope: grant.ScopeMerge, Repo: repoText})
	_, err := e.Client(forged).SwapRef(ctx, "main", 0, commitA)
	refuses(t, err, client.ErrUnauthorized, 401)
}

func TestMainOfANewRepoIsNotProtected(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	op := e.Client(e.operator())
	path, err := op.CreateRepo(ctx, "w", "o1")
	if err != nil {
		t.Fatal(err)
	}
	native := "native:" + path
	if _, err := op.Claim(ctx, "ws1", api.ClaimRequest{Epoch: 1, Owner: "o1", Repo: native}); err != nil {
		t.Fatal(err)
	}
	node := e.Client(e.NodeGrant(t, "ws1", 1, native))
	main, err := node.Ref(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.SwapRef(ctx, "main", main.Generation, commitA); err != nil {
		t.Errorf("node write to main of a new repo: %v", err)
	}
}

func TestRefEventsRecordEveryWrite(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	op := e.Client(e.operator())
	path, err := op.CreateRepo(ctx, "w", "o1")
	if err != nil {
		t.Fatal(err)
	}
	native := "native:" + path
	if _, err := op.Claim(ctx, "ws1", api.ClaimRequest{Epoch: 1, Owner: "o1", Repo: native}); err != nil {
		t.Fatal(err)
	}
	node, merge := e.Client(e.NodeGrant(t, "ws1", 1, native)), e.Client(e.MergeGrant(t, native))
	made, err := node.SwapRef(ctx, "x", 0, commitA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.SwapRef(ctx, "x", 0, commitB); err == nil {
		t.Fatal("a create over a ref succeeded")
	}
	moved, err := merge.SwapRef(ctx, "x", made.Generation, commitB)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.DeleteRef(ctx, "x", moved.Generation); err != nil {
		t.Fatal(err)
	}
	evs, err := op.Events(ctx, 0, 0)
	if err != nil || len(evs) != 4 {
		t.Fatalf("events = %+v, %v, want 4: the failed create has none", evs, err)
	}
	main, _ := node.Ref(ctx, "main")
	want := []api.Event{
		{Repo: native, Ref: "main", New: main.Commit, Kind: "operator"},
		{Repo: native, Ref: "x", New: commitA, Generation: made.Generation, Kind: "node"},
		{Repo: native, Ref: "x", Old: commitA, New: commitB, Generation: moved.Generation, Kind: "merge"},
		{Repo: native, Ref: "x", Old: commitB, Generation: moved.Generation, Kind: "node"},
	}
	for i, w := range want {
		g := evs[i]
		if g.Time == "" || g.Generation <= 0 || i > 0 && g.Seq <= evs[i-1].Seq {
			t.Errorf("event %d = %+v: want a time, a generation and a rising seq", i, g)
		}
		g.Seq, g.Time = 0, ""
		if i == 0 {
			g.Generation = 0
		}
		if g != w {
			t.Errorf("event %d = %+v, want %+v", i, g, w)
		}
	}
	page, err := op.Events(ctx, evs[0].Seq, 2)
	if err != nil || len(page) != 2 || page[0].Seq != evs[1].Seq || page[1].Seq != evs[2].Seq {
		t.Errorf("page = %+v, %v, want events 1 and 2", page, err)
	}
	if raw, _ := json.Marshal(evs); strings.Contains(string(raw), "mg1.") {
		t.Error("an event carries a grant")
	}
}

func TestEventRouteRules(t *testing.T) {
	e := newEnv(t)
	for _, q := range []string{"after=-1", "after=x", "limit=0", "limit=1001", "limit=x"} {
		e.want(e.call("GET", "/v1/events?"+q, e.operator(), nil), 400, "invalid_body")
	}
	e.want(e.claim("ws1", 1, repoText), 200, "")
	for _, tok := range []string{e.nodeGrant("ws1", 1), e.MergeGrant(t, repoText), e.readGrant(repoText)} {
		e.want(e.call("GET", "/v1/events", tok, nil), 401, "unauthorized")
	}
}

func TestSubscriptionRoutes(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	op := e.Client(e.operator())
	for _, bad := range []api.CreateSubscriptionRequest{
		{URL: "ftp://x.invalid/h", Secret: strings.Repeat("s", 16)},
		{URL: "https:///h", Secret: strings.Repeat("s", 16)},
		{URL: "https://x.invalid/h", Secret: "short"},
		{URL: "https://x.invalid/h", Secret: strings.Repeat("s", 16), Repo: "no-colon"},
	} {
		_, err := op.CreateSubscription(ctx, bad)
		refuses(t, err, client.ErrInvalidBody, 400)
	}
	merge := e.Client(e.MergeGrant(t, repoText))
	if _, err := merge.SwapRef(ctx, "a", 0, commitA); err != nil {
		t.Fatal(err)
	}
	sub, err := op.CreateSubscription(ctx, api.CreateSubscriptionRequest{URL: "https://x.invalid/h", Secret: strings.Repeat("s", 16), Repo: repoText})
	if err != nil || sub.ID == "" || sub.Cursor != 1 || sub.Repo != repoText {
		t.Fatalf("create = %+v, %v, want the cursor at the newest event", sub, err)
	}
	list, err := op.Subscriptions(ctx)
	if raw, _ := json.Marshal(list); err != nil || len(list) != 1 || list[0] != sub || strings.Contains(string(raw), "sssss") {
		t.Fatalf("list = %+v, %v: want the subscription and no secret", list, err)
	}
	if err := op.DeleteSubscription(ctx, sub.ID); err != nil {
		t.Fatal(err)
	}
	refuses(t, op.DeleteSubscription(ctx, sub.ID), client.ErrNotFound, 404)
	_, err = e.Client(e.MergeGrant(t, repoText)).CreateSubscription(ctx, api.CreateSubscriptionRequest{URL: "https://x.invalid/h", Secret: strings.Repeat("s", 16)})
	refuses(t, err, client.ErrUnauthorized, 401)
}
