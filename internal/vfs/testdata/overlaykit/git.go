//go:build linux

// Package overlaykit holds what the privileged overlay tools share: the control-socket client, git plumbing
// over a bare repository with alternate objects, the application of a dirty set to a base tree, and the
// plain-worktree oracle.
package overlaykit

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Git runs git against one bare repository. Index names the index file of the commands (empty: the default
// of Dir). Work names the worktree (empty: none). A Git is safe to copy and holds no state.
type Git struct {
	Dir   string
	Index string
	Work  string
}

// Run runs git with stdin and returns its standard output. The error holds the standard error.
func (g Git) Run(stdin []byte, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "GIT_DIR=") && !strings.HasPrefix(kv, "GIT_INDEX_FILE=") && !strings.HasPrefix(kv, "GIT_WORK_TREE=") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_DIR="+g.Dir)
	if g.Index != "" {
		cmd.Env = append(cmd.Env, "GIT_INDEX_FILE="+g.Index)
	}
	if g.Work != "" {
		cmd.Env = append(cmd.Env, "GIT_WORK_TREE="+g.Work)
	}
	cmd.Stdin = bytes.NewReader(stdin)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %v: %w\n%s", args, err, errb.Bytes())
	}
	return out.String(), nil
}

// Line runs git and trims the output.
func (g Git) Line(args ...string) (string, error) {
	out, err := g.Run(nil, args...)
	return strings.TrimSpace(out), err
}

// Init creates a bare repository at dir whose alternates are the object directories in alt, separated by ":".
func Init(dir, alt string) (Git, error) {
	if out, err := exec.Command("git", "init", "-q", "--bare", dir).CombinedOutput(); err != nil {
		return Git{}, fmt.Errorf("git init: %w\n%s", err, out)
	}
	list := strings.ReplaceAll(alt, ":", "\n") + "\n"
	if err := os.WriteFile(filepath.Join(dir, "objects/info/alternates"), []byte(list), 0o644); err != nil {
		return Git{}, err
	}
	return Git{Dir: dir}, nil
}

// NameStatus lists the paths whose entries differ between two trees, one "S\tpath" line each.
func (g Git) NameStatus(a, b string) string {
	out, err := g.Run(nil, "diff-tree", "-r", "--name-status", a, b)
	if err != nil {
		return err.Error()
	}
	return strings.TrimSpace(out)
}

// Report formats a tree mismatch: the paths that each side changed against the base, and the difference
// between the two sides.
func (g Git) Report(base, got, want string) string {
	return fmt.Sprintf("changed by the dirty set (base..got):\n%s\nchanged by the oracle (base..want):\n%s\ngot..want:\n%s",
		g.NameStatus(base, got), g.NameStatus(base, want), g.NameStatus(got, want))
}

// Oracle is a plain worktree that follows the same edits as the mount under test.
type Oracle struct {
	Git
	Shadow string
}

// NewOracle checks the tree base out into a new worktree at shadow, with the repository at gitdir.
func NewOracle(gitdir, shadow, alt, base string) (*Oracle, error) {
	if err := os.RemoveAll(shadow); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(gitdir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(shadow, 0o755); err != nil {
		return nil, err
	}
	g, err := Init(gitdir, alt)
	if err != nil {
		return nil, err
	}
	g.Work = shadow
	for _, args := range [][]string{{"read-tree", base}, {"checkout-index", "-a", "-f"}, {"config", "core.fileMode", "true"}} {
		if _, err := g.Run(nil, args...); err != nil {
			return nil, err
		}
	}
	return &Oracle{Git: g, Shadow: shadow}, nil
}

// Tree is the tree of the worktree: `git add -A -f; git write-tree`. A fresh index drops the entries of the
// last call, so a path that stopped being a file or symlink leaves the tree.
func (o *Oracle) Tree(fresh bool) (string, error) {
	if fresh {
		_ = os.Remove(filepath.Join(o.Dir, "index"))
	}
	if _, err := o.Run(nil, "add", "-A", "-f"); err != nil {
		return "", err
	}
	return o.Line("write-tree")
}

// RefuseGitlinks fails when the tree base holds a gitlink (mode 160000). The mount shows a submodule as an
// empty directory and `git add` drops it, so the oracle cannot model it: the run needs a base without
// submodules.
func (g Git) RefuseGitlinks(base string) error {
	out, err := g.Run(nil, "ls-tree", "-r", "-z", base)
	if err != nil {
		return err
	}
	for _, rec := range strings.Split(out, "\x00") {
		if meta, path, ok := strings.Cut(rec, "\t"); ok && strings.HasPrefix(meta, "160000 ") {
			return fmt.Errorf("the base tree %s has a submodule at %s; the oracle cannot model submodules: choose a base without submodules", base, path)
		}
	}
	return nil
}
