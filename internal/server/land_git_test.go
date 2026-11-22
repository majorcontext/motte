package server_test

import (
	"context"
	"io/fs"
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
	root := g.head(one)
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
	headTwo := g.head(two)

	first, err := merge.Land(ctx, g.text, api.LandRequest{Ref: "main", Base: root, Head: g.head(one)})
	if err != nil || !first.FastForward {
		t.Fatalf("first land = %+v, %v", first, err)
	}
	second, err := merge.Land(ctx, g.text, api.LandRequest{Ref: "main", Base: root, Head: headTwo})
	if err != nil || second.FastForward || len(second.Commits) != 3 || second.Commits[2].Old != headTwo {
		t.Fatalf("second land = %+v, %v", second, err)
	}

	check := g.clone(g.readGrant(g.text))
	g.git(check, true, "fsck", "--strict")
	if got := g.head(check); got != second.Tip {
		t.Errorf("clone is at %s, want the landed tip %s", got, second.Tip)
	}
	history := strings.Split(g.git(check, true, "log", "--format=%s|%an <%ae> %cn <%ce>", "--first-parent"), "\n")
	var subjects []string
	for _, line := range history {
		subjects = append(subjects, strings.SplitN(line, "|", 2)[0])
	}
	if got := strings.Join(subjects, "\n"); got != "edit src/b.txt\nedit src/deep/c.txt\nedit src/b.txt\nedit src/a.txt\nInitial commit" {
		t.Errorf("history = %q", got)
	}
	if !strings.HasSuffix(history[0], "|a <a@example.com> a <a@example.com>") {
		t.Errorf("author and committer = %q", history[0])
	}
	var files []string
	_ = filepath.WalkDir(check, func(path string, d fs.DirEntry, err error) error {
		if rel, _ := filepath.Rel(check, path); err == nil && !d.IsDir() && !strings.HasPrefix(rel, ".git"+string(filepath.Separator)) {
			files = append(files, filepath.ToSlash(rel))
		}
		return err
	})
	if got := strings.Join(files, "\n"); got != "src/a.txt\nsrc/b.txt\nsrc/deep/c.txt" {
		t.Errorf("files = %q", got)
	}
	if got, err := os.ReadFile(filepath.Join(check, "src", "b.txt")); err != nil || string(got) != "b2" {
		t.Errorf("b.txt = %q, %v", got, err)
	}
}
