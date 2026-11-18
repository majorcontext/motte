package server_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/meta"
)

var wrongTree = strings.Repeat("1", 40)

func (l *landEnv) landExpecting(base, head, tree string) (api.LandResponse, error) {
	return l.merge.Land(context.Background(), l.repo, api.LandRequest{Ref: "main", Base: base, Head: head, ExpectTree: tree})
}

func (l *landEnv) treeOf(commit string) string {
	l.t.Helper()
	c, err := l.reader.Commit(context.Background(), commit)
	if err != nil {
		l.t.Fatal(err)
	}
	return c.Tree
}

func TestLandExpectTreeRefusesAStaleRebaseAndNamesTheTreeThatLandsWithoutIt(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		c1 := o.commit(map[string]string{"f1": "1"}, "", "one", l.root)
		c2 := o.commit(map[string]string{"f2": "2"}, "", "two", l.root)
		c3 := o.commit(map[string]string{"f3": "3"}, "", "three", l.root)
		o.upload(t, l.env, l.repo)
		if _, err := l.land(l.merge, "main", l.root, c1); err != nil {
			t.Fatal(err)
		}
		at := l.tip("main")

		_, err := l.landExpecting(l.root, c2, wrongTree)
		refuses(t, err, client.ErrLandStale, 409)
		tree := client.StaleTree(err)
		if got := l.tip("main"); got != at {
			t.Errorf("main = %+v after a stale land, want %+v", got, at)
		}
		r, err := l.land(l.merge, "main", l.root, c2)
		if err != nil || l.treeOf(r.Tip) != tree {
			t.Errorf("tree of the land without expect_tree = %q, %v, want the stale tree %q", l.treeOf(r.Tip), err, tree)
		}

		_, err = l.landExpecting(l.root, c3, wrongTree)
		refuses(t, err, client.ErrLandStale, 409)
		r, err = l.landExpecting(l.root, c3, client.StaleTree(err))
		if err != nil || len(r.Commits) != 1 {
			t.Errorf("land with the matching expect_tree = %+v, %v", r, err)
		}
	})
}

func TestLandExpectTreeComparesTheTreeOfHeadOnAFastForwardAndTheTreeOfTheTipOnAHeadThatIsTheTip(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		c1 := o.commit(map[string]string{"f1": "1"}, "", "one", l.root)
		c2 := o.commit(map[string]string{"f1": "1", "f2": "2"}, "", "two", c1)
		o.upload(t, l.env, l.repo)

		_, err := l.landExpecting(l.root, c1, wrongTree)
		refuses(t, err, client.ErrLandStale, 409)
		if got := client.StaleTree(err); got != l.treeOf(c1) {
			t.Errorf("fast-forward stale tree = %q, want the tree of head %q", got, l.treeOf(c1))
		}
		if got := l.tip("main").Commit; got != l.root {
			t.Fatalf("main = %s after a stale fast-forward, want the root", got)
		}
		r, err := l.landExpecting(l.root, c1, l.treeOf(c1))
		if err != nil || !r.FastForward || r.Tip != c1 {
			t.Fatalf("fast-forward with the matching expect_tree = %+v, %v", r, err)
		}

		_, err = l.landExpecting(l.root, c1, wrongTree)
		refuses(t, err, client.ErrLandStale, 409)
		if got := client.StaleTree(err); got != l.treeOf(c1) {
			t.Errorf("head-is-tip stale tree = %q, want the tree of the tip %q", got, l.treeOf(c1))
		}
		if r, err := l.landExpecting(l.root, c1, l.treeOf(c1)); err != nil || r.Tip != c1 {
			t.Errorf("head-is-tip with the matching expect_tree = %+v, %v", r, err)
		}

		_, err = l.landExpecting(c1, c2, wrongTree)
		refuses(t, err, client.ErrLandStale, 409)
		if got := l.tip("main").Commit; got != c1 {
			t.Errorf("main = %s after a stale fast-forward, want %s", got, c1)
		}
	})
}

func TestLandExpectTreeStaleItemMovesNothingAndDoesNotStopItsBatch(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		x1 := o.commit(map[string]string{"f1": "1"}, "", "x1", l.root)
		x2 := o.commit(map[string]string{"f2": "2"}, "", "x2", l.root)
		x3 := o.commit(map[string]string{"f3": "3"}, "", "x3", l.root)
		both := o.tree(map[string]string{"f1": "1", "f3": "3"})
		o.upload(t, l.env, l.repo)
		for id, it := range map[string]meta.LandRequest{
			"land-one-aaaa": {Head: x1},
			"land-two-bbbb": {Head: x2, ExpectTree: wrongTree},
		} {
			it.ID, it.Repo, it.Ref, it.Base, it.Kind = id, l.repo, "main", l.root, meta.WriterMerge
			if _, err := l.Meta.EnqueueLand(context.Background(), it, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		last, err := l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: "land-three-c", Ref: "main", Base: l.root, Head: x3, ExpectTree: both})
		if err != nil {
			t.Fatalf("a later land whose expect_tree holds the earlier land of its batch: %v", err)
		}
		_, err = l.merge.Land(context.Background(), l.repo, api.LandRequest{ID: "land-two-bbbb", Ref: "main", Base: l.root, Head: x2, ExpectTree: wrongTree})
		refuses(t, err, client.ErrLandStale, 409)
		if got := l.files(last.Tip); !slices.Equal(got, []string{"f1", "f3"}) {
			t.Errorf("files = %v, want f1 and f3 and no f2", got)
		}
	})
}

func TestLandExpectTreeRepeatOfAnIdMustCarryTheSameExpectTree(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		a := o.commit(map[string]string{"a": "1"}, "", "a", l.root)
		o.upload(t, l.env, l.repo)
		req := api.LandRequest{ID: "once-only-2", Ref: "main", Base: l.root, Head: a, ExpectTree: l.treeOf(a)}
		first, err := l.merge.Land(context.Background(), l.repo, req)
		if err != nil {
			t.Fatal(err)
		}
		if again, err := l.merge.Land(context.Background(), l.repo, req); err != nil || again.Generation != first.Generation {
			t.Errorf("repeat with the same expect_tree = %+v, %v, want %+v", again, err, first)
		}
		for _, tree := range []string{"", wrongTree} {
			req.ExpectTree = tree
			_, err = l.merge.Land(context.Background(), l.repo, req)
			refuses(t, err, client.ErrInvalidBody, 400)
		}
	})
}

func TestLandExpectTreeMustBeFortyLowercaseHexDigits(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		var o objSet
		a := o.commit(map[string]string{"a": "1"}, "", "a", l.root)
		o.upload(t, l.env, l.repo)
		for _, tree := range []string{"abc", strings.Repeat("A", 40), strings.Repeat("g", 40), strings.Repeat("1", 41)} {
			_, err := l.landExpecting(l.root, a, tree)
			refuses(t, err, client.ErrInvalidBody, 400)
		}
		if got := l.tip("main").Commit; got != l.root {
			t.Errorf("main = %s after refused requests, want the root", got)
		}
	})
}
