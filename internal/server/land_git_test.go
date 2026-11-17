package server_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
)

func TestLandedCommitsAreValidForGit(t *testing.T) {
	g := newGitEnv(t)
	ctx := context.Background()
	merge := g.Client(g.MergeGrant(t, g.text))
	write := g.WriteGrant(t, g.text)
	one, two := g.clone(write), g.clone(write)
	root := g.git(one, true, "rev-parse", "HEAD")
	for _, dir := range []string{one, two} {
		if err := os.MkdirAll(filepath.Join(dir, "src", "deep"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	g.commit(one, "src/a.txt", "a")
	g.git(one, true, "push", "-q", "origin", "HEAD:refs/heads/one")
	g.commit(two, "src/b.txt", "b1")
	g.commit(two, "src/deep/c.txt", "c")
	g.commit(two, "src/b.txt", "b2")
	g.git(two, true, "push", "-q", "origin", "HEAD:refs/heads/two")
	headTwo := g.git(two, true, "rev-parse", "HEAD")

	first, err := merge.Land(ctx, g.text, api.LandRequest{Ref: "main", Base: root, Head: g.git(one, true, "rev-parse", "HEAD")})
	if err != nil || !first.FastForward {
		t.Fatalf("first land = %+v, %v", first, err)
	}
	second, err := merge.Land(ctx, g.text, api.LandRequest{Ref: "main", Base: root, Head: headTwo})
	if err != nil || second.FastForward || len(second.Commits) != 3 || second.Commits[2].Old != headTwo {
		t.Fatalf("second land = %+v, %v", second, err)
	}

	check := g.clone(g.readGrant(g.text))
	g.git(check, true, "fsck", "--strict")
	if got := g.git(check, true, "rev-parse", "HEAD"); got != second.Tip {
		t.Errorf("clone is at %s, want the landed tip %s", got, second.Tip)
	}
	if got := g.git(check, true, "log", "--format=%s", "--first-parent"); got != "edit src/b.txt\nedit src/deep/c.txt\nedit src/b.txt\nedit src/a.txt\nInitial commit" {
		t.Errorf("history = %q", got)
	}
	if got := g.git(check, true, "ls-files"); got != "src/a.txt\nsrc/b.txt\nsrc/deep/c.txt" {
		t.Errorf("files = %q", got)
	}
	if got := g.git(check, true, "show", "HEAD:src/b.txt"); got != "b2" {
		t.Errorf("b.txt = %q", got)
	}
	if got := g.git(check, true, "log", "-1", "--format=%an <%ae> %cn <%ce>"); !strings.HasPrefix(got, "a <a@example.com> a <a@example.com>") {
		t.Errorf("author and committer = %q", got)
	}
}
