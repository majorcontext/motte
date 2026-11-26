package server_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
)

func TestProtectedRefsAdmitOnlyAMergeGrant(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.want(e.claim("ws1", 1, repoText), 200, "")
	op := e.Client(e.operator())
	for _, bad := range [][]string{{""}, {"a b"}, {"../x"}, {"a/**/b"}, make([]string, 101)} {
		refuses(t, op.SetProtectedRefs(ctx, repoText, bad), client.ErrInvalidBody, 400)
	}
	refuses(t, op.SetProtectedRefs(ctx, "no-colon", []string{"main"}), client.ErrInvalidBody, 400)
	for name, c := range map[string]*client.Client{"node": e.Client(e.nodeGrant("ws1", 1)), "merge": e.Client(e.MergeGrant(t, repoText)), "read": e.Client(e.readGrant(repoText))} {
		refuses(t, c.SetProtectedRefs(ctx, repoText, []string{"main"}), client.ErrRepoForbidden, 403)
		_, err := c.ProtectedRefs(ctx, repoText)
		if !strings.Contains(fmt.Sprint(err), "403") {
			t.Errorf("%s grant read the list: %v", name, err)
		}
	}
	if got, err := op.ProtectedRefs(ctx, repoText); err != nil || len(got) != 0 {
		t.Errorf("a repo with none = %v, %v", got, err)
	}
}

func TestMergeGrantStaysInItsRepoAndCheckedKey(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	evs, cursor, err := op.Events(ctx, "", 0)
	if err != nil || len(evs) != 4 {
		t.Fatalf("events = %+v, %v, want 4: the failed create has none", evs, err)
	}
	main, _ := node.Ref(ctx, "main")
	want := map[string][]api.Event{
		"main": {{Repo: native, Ref: "main", New: main.Commit, Kind: "operator"}},
		"x": {
			{Repo: native, Ref: "x", New: commitA, Generation: made.Generation, Kind: "node"},
			{Repo: native, Ref: "x", Old: commitA, New: commitB, Generation: moved.Generation, Kind: "merge"},
			{Repo: native, Ref: "x", Old: commitB, Generation: moved.Generation, Kind: "node"},
		},
	}
	got := map[string][]api.Event{}
	for _, g := range evs {
		if g.Time == "" || g.Generation <= 0 || g.Seq <= 0 || g.Partition < 0 || g.Partition >= 64 {
			t.Errorf("event %+v: want a time, a generation, a seq and a partition", g)
		}
		got[g.Ref] = append(got[g.Ref], g)
	}
	for ref, ws := range want {
		for i, w := range ws {
			g := got[ref][i]
			if i > 0 && (g.Partition != got[ref][0].Partition || g.Seq <= got[ref][i-1].Seq) {
				t.Errorf("%s event %d = %+v: want the partition of the ref and a rising seq", ref, i, g)
			}
			g.Seq, g.Time, g.Partition = 0, "", 0
			if ref == "main" {
				g.Generation = 0
			}
			if g != w {
				t.Errorf("%s event %d = %+v, want %+v", ref, i, g, w)
			}
		}
	}
	if more, next, err := op.Events(ctx, cursor, 0); err != nil || len(more) != 0 || next != cursor {
		t.Errorf("after the cursor = %+v, %q, %v, want nothing and the same cursor", more, next, err)
	}
	if _, err := merge.SwapRef(ctx, "y", 0, commitA); err != nil {
		t.Fatal(err)
	}
	if more, _, err := op.Events(ctx, cursor, 0); err != nil || len(more) != 1 || more[0].Ref != "y" {
		t.Errorf("after the cursor = %+v, %v, want the one new event", more, err)
	}
	var seen []api.Event
	for cur := ""; len(seen) < 5; {
		page, next, err := op.Events(ctx, cur, 2)
		if err != nil || len(page) == 0 || len(page) > 2 {
			t.Fatalf("page = %+v, %v", page, err)
		}
		seen, cur = append(seen, page...), next
	}
	if len(seen) != 5 {
		t.Errorf("pages of 2 gave %d events, want 5 with no repeat", len(seen))
	}
	if raw, _ := json.Marshal(evs); strings.Contains(string(raw), "mg1.") {
		t.Error("an event carries a grant")
	}
}

func TestEventRouteRules(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ahead := base64.RawURLEncoding.EncodeToString([]byte("1" + strings.Repeat(",0", 63)))
	for _, q := range []string{"cursor=x", "cursor=MSwy", "cursor=" + ahead, "limit=0", "limit=1001", "limit=x"} {
		e.want(e.call("GET", "/v1/events?"+q, e.operator(), nil), 400, "invalid_body")
	}
	e.want(e.claim("ws1", 1, repoText), 200, "")
	for _, tok := range []string{e.nodeGrant("ws1", 1), e.MergeGrant(t, repoText)} {
		e.want(e.call("GET", "/v1/events", tok, nil), 403, "vcs_repo_forbidden")
	}
	e.want(e.call("GET", "/v1/events", e.readGrant(repoText), nil), 200, "")
}

func TestSubscriptionRoutes(t *testing.T) {
	t.Parallel()
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
	_, head, _ := op.Events(ctx, "", 0)
	if err != nil || sub.ID == "" || sub.Cursor != head || sub.Repo != repoText {
		t.Fatalf("create = %+v, %v, want a cursor at the newest events", sub, err)
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
	refuses(t, err, client.ErrRepoForbidden, 403)
}
