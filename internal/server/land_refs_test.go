package server_test

import (
	"context"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
)

func TestMergeGrantWithRefsMovesOnlyMatchingRefs(t *testing.T) {
	eachBackend(t, func(t *testing.T, l *landEnv) {
		ctx := context.Background()
		var o objSet
		c1 := o.commit(map[string]string{"a": "1"}, "", "c1", l.root)
		o.upload(t, l.env, l.repo)
		scoped := l.Client(l.mint(l.cp, grant.Claims{Scope: grant.ScopeMerge, Repo: l.repo, Refs: []string{"feature/*"}}))
		if _, err := scoped.SwapRef(ctx, "feature/a", 0, l.root); err != nil {
			t.Fatal(err)
		}
		_, err := scoped.SwapRef(ctx, "release", 0, l.root)
		refuses(t, err, client.ErrRepoForbidden, 403)
		refuses(t, scoped.DeleteRef(ctx, "main", l.tip("main").Generation), client.ErrRepoForbidden, 403)
		_, err = scoped.Land(ctx, l.repo, api.LandRequest{Ref: "main", Base: l.root, Head: c1})
		refuses(t, err, client.ErrRepoForbidden, 403)
		if r, err := scoped.Land(ctx, l.repo, api.LandRequest{Ref: "feature/a", Base: l.root, Head: c1}); err != nil || r.Tip != c1 {
			t.Errorf("land on a named ref = %+v, %v", r, err)
		}
		if got := l.tip("main").Commit; got != l.root {
			t.Errorf("main = %s, want it unmoved", got)
		}
		if err := scoped.DeleteRef(ctx, "feature/a", l.tip("feature/a").Generation); err != nil {
			t.Errorf("delete of a named ref: %v", err)
		}
	})
}
