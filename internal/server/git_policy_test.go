package server_test

import (
	"context"
	"strings"
	"testing"
)

func TestGitPushHonorsProtectedRefsAndRecordsEvents(t *testing.T) {
	g := newGitEnv(t)
	ctx := context.Background()
	op := g.Client(g.operator())
	if err := op.SetProtectedRefs(ctx, g.text, []string{"main", "release/*"}); err != nil {
		t.Fatal(err)
	}
	write, merge := g.WriteGrant(t, g.text), g.remote(g.path, g.MergeGrant(t, g.text))
	w := g.clone(write)
	g.commit(w, "w.txt", "w")
	for _, dst := range []string{"main", "release/1"} {
		if out := g.git(w, false, "push", "origin", "HEAD:refs/heads/"+dst); !strings.Contains(out, "protected") {
			t.Errorf("write push to %s: %q, want a per-ref refusal that says protected", dst, out)
		}
	}
	g.git(w, true, "push", "-q", "origin", "HEAD:refs/heads/feature")
	g.git(w, false, "push", "origin", ":refs/heads/main")
	g.git(w, true, "push", "-q", merge, "HEAD:refs/heads/main")
	g.git(w, true, "push", "-q", merge, "HEAD:refs/heads/release/1")
	g.git(w, true, "push", "-q", merge, ":release/1")
	if got, head := g.git(w, true, "ls-remote", "origin", "refs/heads/main"), g.git(w, true, "rev-parse", "HEAD"); !strings.HasPrefix(got, head) {
		t.Errorf("main after the merge push = %q, want %s", got, head)
	}

	evs, _, err := op.Events(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string][]string{}
	for _, e := range evs {
		kinds[e.Ref] = append(kinds[e.Ref], e.Kind)
	}
	want := map[string]string{"main": "[operator merge]", "feature": "[write]", "release/1": "[merge merge]"}
	for ref, w := range want {
		if got := strings.Join(kinds[ref], " "); "["+got+"]" != w {
			t.Errorf("events of %s = %s, want %s: every accepted push has one, a refused push none", ref, kinds[ref], w)
		}
	}
	if len(evs) != 5 {
		t.Errorf("events = %+v, want 5", evs)
	}
}
