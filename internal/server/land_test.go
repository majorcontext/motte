package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/meta"
)

const signature = "gpgsig -----BEGIN SIGNATURE-----\n sig\n -----END SIGNATURE-----\n"

func TestLandFastForwardsThenRebasesAndMapsTheCommits(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		c1 := o.commit(map[string]string{"f1": "1"}, "change-id kkkk\n"+signature, "one", l.root)
		c2 := o.commit(map[string]string{"f2": "2"}, "change-id llll\n"+signature, "two", l.root)
		o.upload(t, l.env, l.repo)

		r1, err := l.land(l.merge, "main", l.root, c1)
		if err != nil || !r1.FastForward || r1.Tip != c1 || len(r1.Commits) != 0 {
			t.Fatalf("fast forward = %+v, %v", r1, err)
		}
		if !strings.Contains(l.commitText(c1), "gpgsig") {
			t.Error("a fast forward changed the commit")
		}
		before := time.Now().Unix()
		r2, err := l.land(l.merge, "main", l.root, c2)
		if err != nil || r2.FastForward || len(r2.Commits) != 1 || r2.Commits[0].Old != c2 || r2.Tip != r2.Commits[0].New || r2.Tip == c2 || r2.Generation <= r1.Generation {
			t.Fatalf("rebase = %+v, %v", r2, err)
		}
		if got := l.tip("main"); got.Commit != r2.Tip || got.Generation != r2.Generation {
			t.Fatalf("main = %+v, want %+v", got, r2)
		}
		text := l.commitText(r2.Tip)
		for _, want := range []string{"parent " + c1 + "\n", "author Ada <ada@example.com> 1600000000 +0100\n", "committer Grace <grace@example.com> ", " +0000\n", "change-id llll\n", "\ntwo\n"} {
			if !strings.Contains(text, want) {
				t.Errorf("rebased commit lacks %q:\n%s", want, text)
			}
		}
		if strings.Contains(text, "gpgsig") || strings.Contains(text, "BEGIN") {
			t.Errorf("rebased commit keeps its signature:\n%s", text)
		}
		var stamp int64
		line := text[strings.Index(text, "committer"):]
		if _, err := fmt.Sscanf(line[strings.Index(line, ">")+2:], "%d", &stamp); err != nil || stamp < before || stamp > time.Now().Unix() {
			t.Errorf("committer time = %d, want the land time from %d", stamp, before)
		}
		if got := l.files(r2.Tip); !slices.Equal(got, []string{"f1", "f2"}) {
			t.Errorf("files = %v, want both changes", got)
		}
		evs, _, err := l.Client(l.operator()).Events(context.Background(), "", 1000)
		if err != nil {
			t.Fatal(err)
		}
		var news []string
		for _, e := range evs {
			if e.Ref == "main" && e.Kind == "merge" {
				news = append(news, e.New)
			}
		}
		if !slices.Equal(news, []string{l.root, c1, r2.Tip}) {
			t.Errorf("merge events of main end at %v, want the root, %s and %s", news, c1, r2.Tip)
		}
	})
}

func TestLandConflictNamesTheClashingPathsAndMovesNothing(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		start := o.commit(map[string]string{"a": "1", "b": "1", "d/x": "1"}, "", "start", l.root)
		tip := o.commit(map[string]string{"a": "2", "b": "1", "d/x": "2"}, "", "tip", start)
		clash := o.commit(map[string]string{"a": "3", "b": "1", "d/x": "1"}, "", "edit a", start)
		gone := o.commit(map[string]string{"b": "1", "d/x": "1"}, "", "delete a", start)
		renamed := o.commit(map[string]string{"a": "1", "c": "1", "d/x": "3"}, "", "rename b, edit d/x", start)
		dirOverFile := o.commit(map[string]string{"a/y": "1", "b": "1", "d/x": "1"}, "", "a becomes a directory", start)
		clean := o.commit(map[string]string{"a": "1", "b": "9", "d/x": "1"}, "", "edit b", start)
		o.upload(t, l.env, l.repo)
		if _, err := l.land(l.merge, "main", l.root, start); err != nil {
			t.Fatal(err)
		}
		if _, err := l.land(l.merge, "main", start, tip); err != nil {
			t.Fatal(err)
		}
		at := l.tip("main")
		for name, c := range map[string]struct {
			head  string
			paths []string
		}{"both edit a": {clash, []string{"a"}}, "delete against edit": {gone, []string{"a"}}, "rename and edit": {renamed, []string{"d/x"}}, "file becomes directory": {dirOverFile, []string{"a"}}} {
			_, err := l.land(l.merge, "main", start, c.head)
			refuses(t, err, client.ErrLandConflict, 409)
			if got := client.ConflictPaths(err); !slices.Equal(got, c.paths) {
				t.Errorf("%s: paths = %v, want %v", name, got, c.paths)
			}
		}
		if got := l.tip("main"); got != at {
			t.Errorf("main = %+v after the conflicts, want %+v", got, at)
		}
		if r, err := l.land(l.merge, "main", start, clean); err != nil || len(r.Commits) != 1 {
			t.Errorf("a change of another file = %+v, %v", r, err)
		}
	})
}

func TestLandBatchRefusesALaterLandThatClashesWithAnEarlierOne(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		files := func(m map[string]string) map[string]string { return m }
		x1 := o.commit(files(map[string]string{"f1": "1"}), "", "x1", l.root)
		x2 := o.commit(files(map[string]string{"f1": "2"}), "", "x2", l.root)
		x3 := o.commit(files(map[string]string{"f3": "3"}), "", "x3", l.root)
		x4 := o.commit(files(map[string]string{"f4": "4"}), "", "x4", l.root)
		o.upload(t, l.env, l.repo)
		ids := map[string]string{x1: "land-one-aaaa", x2: "land-two-bbbb", x3: "land-three-c", x4: "land-four-dd"}
		for _, head := range []string{x1, x2, x3} {
			_, err := l.Meta.EnqueueLand(context.Background(), meta.LandRequest{ID: ids[head], Repo: l.repo, Ref: "main", Base: l.root, Head: head, Kind: meta.WriterMerge}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
		}
		last, err := l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: ids[x4], Ref: "main", Base: l.root, Head: x4})
		if err != nil {
			t.Fatal(err)
		}
		results := map[string]api.LandResponse{}
		for _, head := range []string{x1, x2, x3} {
			r, err := l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: ids[head], Ref: "main", Base: l.root, Head: head})
			if head == x2 {
				refuses(t, err, client.ErrLandConflict, 409)
				if got := client.ConflictPaths(err); !slices.Equal(got, []string{"f1"}) {
					t.Errorf("paths = %v, want [f1]", got)
				}
				continue
			}
			if err != nil {
				t.Fatalf("land %s: %v", ids[head], err)
			}
			results[head] = r
		}
		if r1, r3 := results[x1], results[x3]; r1.Generation != last.Generation || r3.Generation != last.Generation {
			t.Errorf("generations = %d, %d, %d, want one ref move for the batch", r1.Generation, r3.Generation, last.Generation)
		}
		if got := l.files(last.Tip); !slices.Equal(got, []string{"f1", "f3", "f4"}) {
			t.Errorf("files = %v", got)
		}
		evs, _, _ := l.Client(l.operator()).Events(context.Background(), "", 1000)
		var main int
		for _, e := range evs {
			if e.Ref == "main" {
				main++
			}
		}
		if main != 2 {
			t.Errorf("main has %d events, want the create and one for the batch", main)
		}
	})
}

func TestLandScopesProtectionAndTheRepositoryOfTheGrant(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		var heads []string
		for i := range 4 {
			heads = append(heads, o.commit(map[string]string{fmt.Sprint("f", i): "1"}, "", fmt.Sprint("c", i), l.root))
		}
		o.upload(t, l.env, l.repo)
		if _, err := l.merge.SwapRef(context.Background(), "feature", 0, l.root); err != nil {
			t.Fatal(err)
		}
		if err := l.Client(l.operator()).SetProtectedRefs(context.Background(), l.repo, []string{"main"}); err != nil {
			t.Fatal(err)
		}
		_, err := l.land(l.write, "main", l.root, heads[0])
		refuses(t, err, client.ErrRefProtected, 403)
		for name, c := range map[string]*client.Client{"node": l.Client(l.nodeGrant("ws1", 1)), "read": l.reader, "operator": l.Client(l.operator())} {
			if _, err := l.land(c, "feature", l.root, heads[0]); !strings.Contains(fmt.Sprint(err), "401") {
				t.Errorf("%s grant: %v, want 401", name, err)
			}
		}
		_, err = l.merge.Land(context.Background(), "gh:acme/other", api.LandRequest{Ref: "main", Base: l.root, Head: heads[0]})
		refuses(t, err, client.ErrRepoForbidden, 403)
		if got := l.tip("main").Commit; got != l.root {
			t.Fatalf("main = %s after refusals", got)
		}
		if r, err := l.land(l.write, "feature", l.root, heads[1]); err != nil || !r.FastForward {
			t.Errorf("a write grant on an unprotected ref = %+v, %v", r, err)
		}
		if r, err := l.land(l.merge, "main", l.root, heads[2]); err != nil || !r.FastForward {
			t.Errorf("a merge grant on a protected ref = %+v, %v", r, err)
		}
		evs, _, _ := l.Client(l.operator()).Events(context.Background(), "", 1000)
		kinds := map[string]string{}
		for _, e := range evs {
			if e.New == heads[1] || e.New == heads[2] {
				kinds[e.Ref] = e.Kind
			}
		}
		if kinds["feature"] != "write" || kinds["main"] != "merge" {
			t.Errorf("event kinds = %v", kinds)
		}
	})
}

func TestLandRefusesARangeThatCannotLand(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		one := o.commit(map[string]string{"a": "1"}, "", "one", l.root)
		two := o.commit(map[string]string{"b": "1"}, "", "two", l.root)
		merged := o.commit(map[string]string{"a": "1", "b": "1"}, "", "merge", one, two)
		stranger := o.commit(map[string]string{"z": "1"}, "", "stranger")
		o.upload(t, l.env, l.repo)
		missing := strings.Repeat("ab", 20)
		for name, c := range map[string]struct {
			ref, base, head string
			sentinel        error
			status          int
		}{
			"merge in range":   {"main", l.root, merged, client.ErrLandInvalid, 400},
			"unrelated base":   {"main", stranger, one, client.ErrLandInvalid, 400},
			"base is head":     {"main", one, one, client.ErrLandInvalid, 400},
			"head not stored":  {"main", l.root, missing, client.ErrNotFound, 404},
			"base not stored":  {"main", missing, one, client.ErrNotFound, 404},
			"ref absent":       {"nothing", l.root, one, client.ErrNotFound, 404},
			"head not a sha":   {"main", l.root, "main", client.ErrInvalidBody, 400},
			"ref name invalid": {"a//b", l.root, one, client.ErrInvalidBody, 400},
		} {
			_, err := l.land(l.merge, c.ref, c.base, c.head)
			t.Log(name, err)
			refuses(t, err, c.sentinel, c.status)
		}
		_, err := l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: "bad id", Ref: "main", Base: l.root, Head: one})
		refuses(t, err, client.ErrInvalidBody, 400)
		if got := l.tip("main").Commit; got != l.root {
			t.Errorf("main = %s, want it unmoved", got)
		}
	})
}

func TestLandRepeatOfAnIdReturnsTheFirstResult(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		a := o.commit(map[string]string{"a": "1"}, "", "a", l.root)
		b := o.commit(map[string]string{"b": "1"}, "", "b", l.root)
		o.upload(t, l.env, l.repo)
		req := api.LandRequest{ID: "once-only-1", Ref: "main", Base: l.root, Head: a}
		first, err := l.merge.Land(context.Background(), l.repo, req)
		if err != nil {
			t.Fatal(err)
		}
		again, err := l.merge.Land(context.Background(), l.repo, req)
		if err != nil || fmt.Sprint(again) != fmt.Sprint(first) {
			t.Errorf("repeat = %+v, %v, want %+v", again, err, first)
		}
		req.Head = b
		_, err = l.merge.Land(context.Background(), l.repo, req)
		refuses(t, err, client.ErrInvalidBody, 400)
		if got := l.tip("main"); got.Generation != first.Generation {
			t.Errorf("main = %+v, want it to move once", got)
		}
	})
}

func TestLandManyConcurrentAgentsLandOnceEachInALinearHistory(t *testing.T) {
	const agents = 60
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		heads := make([]string, agents)
		for i := range heads {
			heads[i] = o.commit(map[string]string{fmt.Sprint("dir", i%7, "/file", i): "x"}, "", fmt.Sprint("agent ", i), l.root)
		}
		o.upload(t, l.env, l.repo)
		var wg sync.WaitGroup
		results := make([]api.LandResponse, agents)
		errs := make([]error, agents)
		for i := range agents {
			wg.Go(func() { results[i], errs[i] = l.land(l.merge, "main", l.root, heads[i]) })
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("agent %d: %v", i, err)
			}
		}
		seen := map[string]bool{}
		tip := l.tip("main").Commit
		for cur, n := tip, 0; cur != l.root; n++ {
			text := l.commitText(cur)
			parents := parentsOf(text)
			if len(parents) != 1 || n > agents {
				t.Fatalf("commit %s has parents %v after %d commits: the history is not linear", cur, parents, n)
			}
			subject := text[strings.LastIndex(text, "\n\n")+2:]
			if seen[subject] {
				t.Fatalf("%q landed twice", subject)
			}
			seen[subject], cur = true, parents[0]
		}
		if len(seen) != agents {
			t.Errorf("%d agents in the history, want %d", len(seen), agents)
		}
		if got := l.files(tip); len(got) != 7 {
			t.Errorf("root has %v, want the 7 directories", got)
		}
	})
}

func TestLandRecoversAClaimedItemAndLeadsAcrossReplicas(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		cfg := server.Config{Meta: l.Meta, Bucket: l.Bucket, Verifier: l.Verifier, Events: server.EventsConfig{Instance: "replica-2"},
			Lands: server.LandsConfig{Lease: 300 * time.Millisecond, Poll: 5 * time.Millisecond}}
		second := httptest.NewServer(server.New(cfg).Handler())
		t.Cleanup(second.Close)
		other := client.New(second.URL, client.StaticGrant(l.MergeGrant(t, l.repo)))

		var o objSet
		lost := o.commit(map[string]string{"lost": "1"}, "", "lost", l.root)
		fresh := o.commit(map[string]string{"fresh": "1"}, "", "fresh", l.root)
		o.upload(t, l.env, l.repo)
		ctx := context.Background()
		now := time.Now()
		if _, err := l.Meta.EnqueueLand(ctx, meta.LandRequest{ID: "claimed-then-lost", Repo: l.repo, Ref: "main", Base: l.root, Head: lost, Kind: meta.WriterMerge}, now); err != nil {
			t.Fatal(err)
		}
		if err := l.Meta.AcquireLandLease(ctx, l.repo, "main", "replica-dead", now, now.Add(cfg.Lands.Lease)); err != nil {
			t.Fatal(err)
		}
		if items, err := l.Meta.ClaimLands(ctx, l.repo, "main", "replica-dead", now, 10); err != nil || len(items) != 1 {
			t.Fatalf("claim = %v, %v", items, err)
		}
		if _, err := other.Land(ctx, l.repo, api.LandRequest{Ref: "main", Base: l.root, Head: fresh}); err != nil {
			t.Fatal(err)
		}
		r, err := other.Land(ctx, l.repo, api.LandRequest{ID: "claimed-then-lost", Ref: "main", Base: l.root, Head: lost})
		if err != nil || r.Tip == "" {
			t.Fatalf("the claimed item = %+v, %v, want it landed by the next leader", r, err)
		}
		if got := l.files(l.tip("main").Commit); !slices.Equal(got, []string{"fresh", "lost"}) {
			t.Errorf("files = %v, want each change once", got)
		}

		const each = 10
		heads := make([]string, 2*each)
		for i := range heads {
			heads[i] = o.commit(map[string]string{fmt.Sprint("n", i): "1"}, "", fmt.Sprint("n", i), l.root)
		}
		o.upload(t, l.env, l.repo)
		var wg sync.WaitGroup
		errs := make([]error, len(heads))
		for i, head := range heads {
			c := l.merge
			if i%2 == 1 {
				c = other
			}
			wg.Go(func() { _, errs[i] = c.Land(ctx, l.repo, api.LandRequest{Ref: "main", Base: l.root, Head: head}) })
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Errorf("land %d: %v", i, err)
			}
		}
		if got := l.files(l.tip("main").Commit); len(got) != 2+2*each {
			t.Errorf("%d files, want %d", len(got), 2+2*each)
		}
	})
}

func TestLandNoopOnATipOfTheSameBatchGetsTheBatchGeneration(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		x := o.commit(map[string]string{"x": "1"}, "", "x", l.root)
		y := o.commit(map[string]string{"y": "1"}, "", "y", l.root)
		o.upload(t, l.env, l.repo)
		for id, head := range map[string]string{"land-first-aa": x, "land-again-bb": x} {
			if _, err := l.Meta.EnqueueLand(context.Background(), meta.LandRequest{ID: id, Repo: l.repo, Ref: "main", Base: l.root, Head: head, Kind: meta.WriterMerge}, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		last, err := l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: "land-third-cc", Ref: "main", Base: l.root, Head: y})
		if err != nil {
			t.Fatal(err)
		}
		again, err := l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: "land-again-bb", Ref: "main", Base: l.root, Head: x})
		if err != nil || again.Generation != last.Generation {
			t.Fatalf("a repeat of a tip of the batch = %+v, %v, want generation %d", again, err, last.Generation)
		}
	})
}

// failFinish refuses the first calls of FinishLands.
type failFinish struct {
	meta.Store
	left atomic.Int32
}

func (f *failFinish) FinishLands(ctx context.Context, repo, ref, owner string, now time.Time, r []meta.LandDone) error {
	if f.left.Add(-1) >= 0 {
		return errors.New("store is down")
	}
	return f.Store.FinishLands(ctx, repo, ref, owner, now, r)
}

func TestLandKeepsOneTerminalResultWhenTheFailureIsNotRecorded(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		store := &failFinish{Store: l.Meta}
		store.left.Store(4)
		srv := httptest.NewServer(server.New(server.Config{Meta: store, Bucket: l.Bucket, Verifier: l.Verifier,
			Lands: server.LandsConfig{Poll: 2 * time.Millisecond}}).Handler())
		t.Cleanup(srv.Close)
		c := client.New(srv.URL, client.StaticGrant(l.MergeGrant(t, l.repo)))
		_, err := c.Land(context.Background(), l.repo, api.LandRequest{Ref: "main", Base: l.root, Head: l.root})
		refuses(t, err, client.ErrLandInvalid, 400)
	})
}

// failProtected refuses the second call of ProtectedRefs, which is the one of the leader.
type failProtected struct {
	meta.Store
	calls atomic.Int32
}

func (f *failProtected) ProtectedRefs(ctx context.Context, repo string) ([]string, error) {
	if f.calls.Add(1) == 2 {
		return nil, errors.New("store is down")
	}
	return f.Store.ProtectedRefs(ctx, repo)
}

func TestLandRetriesWhenTheProtectedListCannotBeRead(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		head := o.commit(map[string]string{"a": "1"}, "", "a", l.root)
		o.upload(t, l.env, l.repo)
		if _, err := l.merge.SwapRef(context.Background(), "feature", 0, l.root); err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(server.New(server.Config{Meta: &failProtected{Store: l.Meta}, Bucket: l.Bucket, Verifier: l.Verifier,
			Lands: server.LandsConfig{Poll: 2 * time.Millisecond}}).Handler())
		t.Cleanup(srv.Close)
		c := client.New(srv.URL, client.StaticGrant(l.WriteGrant(t, l.repo)))
		if _, err := c.Land(context.Background(), l.repo, api.LandRequest{Ref: "feature", Base: l.root, Head: head}); err != nil {
			t.Fatalf("a backend error stored a result: %v", err)
		}
	})
}

func TestLandLeaderRestartsAfterItLosesTheLeaseToAnotherOwner(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		heads := make([]string, 20)
		for i := range heads {
			heads[i] = o.commit(map[string]string{fmt.Sprint("f", i): "1"}, "", fmt.Sprint("c", i), l.root)
		}
		o.upload(t, l.env, l.repo)
		now := time.Now()
		if err := l.Meta.AcquireLandLease(context.Background(), l.repo, "main", "other-owner", now, now.Add(100*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(server.New(server.Config{Meta: l.Meta, Bucket: l.Bucket, Verifier: l.Verifier,
			Lands: server.LandsConfig{Poll: time.Millisecond, Timeout: 5 * time.Second}}).Handler())
		t.Cleanup(srv.Close)
		c := client.New(srv.URL, client.StaticGrant(l.MergeGrant(t, l.repo)))
		var wg sync.WaitGroup
		errs := make([]error, len(heads))
		for i, h := range heads {
			wg.Go(func() {
				_, errs[i] = c.Land(context.Background(), l.repo, api.LandRequest{Ref: "main", Base: l.root, Head: h})
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Errorf("land %d: %v", i, err)
			}
		}
	})
}
