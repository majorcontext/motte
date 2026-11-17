//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/majorcontext/motte/internal/vfs/testdata/overlaykit"
	"golang.org/x/sys/unix"
)

// restartRecovery cannot run here: it needs the node plugin pod.
func restartRecovery(e *env) (string, error) {
	fmt.Println(`INFO restart-recovery: manual procedure, run it with a workspace that has edits in its upper directory:
  1. Write files, delete a base file, and rename a base directory through the merged mount. Record the output of "dirty" and the file bytes.
  2. Stop the plugin pod (the process that serves the FUSE lower and the control socket). Do not unmount anything.
  3. Check that a read of an unread lower file fails with ENOTCONN and that a read of an edited file still works.
  4. Start the plugin pod again and publish the same volume again with the same base commit.
  5. Check that the stale overlay and the stale FUSE lower were unmounted, the overlay first, with no lazy unmount of the overlay.
  6. Check that the upper directory was reused (the "dirty" output of step 1 is unchanged) and that "work" was deleted and created again.
  7. Publish the volume with another base commit and check that the upper directory was discarded and the plugin logged it.
  8. Unpublish: check that the overlay, then the lower, then <vol> are gone, and that EBUSY on the overlay returns an error that the kubelet retries.`)
	return "", errSkip("not automatable; the procedure is above")
}

// renameBaseDir renames a base directory and requires the merged view and the dirty set to show the move.
func renameBaseDir(e *env) (string, error) {
	d, ls, err := e.pool.dir(e.lower)
	if err != nil {
		return "", err
	}
	moved := d + "-fz-moved"
	if err := os.Rename(e.path(d), e.path(moved)); err != nil {
		if errors.Is(err, syscall.EXDEV) {
			return "", fmt.Errorf("rename of a base directory: %w (redirect_dir is off or not honored)", err)
		}
		return "", fmt.Errorf("rename of a base directory: %w", err)
	}
	if _, err := os.Lstat(e.path(d)); !os.IsNotExist(err) {
		return "", fmt.Errorf("old path still resolves: %v", err)
	}
	for _, l := range ls {
		want, err := os.ReadFile(filepath.Join(e.lower, d, l))
		if err != nil {
			continue
		}
		got, err := os.ReadFile(filepath.Join(e.mnt, moved, l))
		if err != nil || string(got) != string(want) {
			return "", fmt.Errorf("%s at the new path differs from the base (err=%v)", l, err)
		}
	}
	dd, _, err := e.dirty()
	if err != nil {
		return "", err
	}
	if err := listed(dd, "deleted", d); err != nil {
		return "", err
	}
	for _, l := range ls {
		en, ok := dd[filepath.Join(moved, l)]
		if !ok || (en.Kind != "file" && en.Kind != "symlink") {
			return "", fmt.Errorf("moved leaf %s is not a file or symlink in the dirty set: %+v (present=%v)", l, en, ok)
		}
	}
	return fmt.Sprintf("%d leaves listed at the new path; old path deleted", len(ls)), nil
}

// deletedDirTree removes a base directory tree, requires the old path as deleted with nothing below it,
// then makes the directory again and requires it as an opaque directory.
func deletedDirTree(e *env) (string, error) {
	d, ls, err := e.pool.dir(e.lower)
	if err != nil {
		return "", err
	}
	if err := os.RemoveAll(e.path(d)); err != nil {
		return "", err
	}
	dd, _, err := e.dirty()
	if err != nil {
		return "", err
	}
	if err := listed(dd, "deleted", d); err != nil {
		return "", err
	}
	var below []string
	for p := range dd {
		if strings.HasPrefix(p, d+"/") {
			below = append(below, p)
		}
	}
	sort.Strings(below)
	e.info("entries below the deleted directory (%d leaves removed): %v", len(ls), below)
	if len(below) > 0 {
		return "", fmt.Errorf("the dirty set lists %d paths below the deleted directory %s", len(below), d)
	}
	if err := os.Mkdir(e.path(d), 0o755); err != nil {
		return "", err
	}
	dd, _, err = e.dirty()
	if err != nil {
		return "", err
	}
	if en := dd[d]; en.Kind != "dir" || !en.Opaque {
		return "", fmt.Errorf("the directory made again is %+v, want a dir with opaque", en)
	}
	if ents, err := os.ReadDir(e.path(d)); err != nil || len(ents) != 0 {
		return "", fmt.Errorf("the directory made again lists %d entries (err=%v), want 0", len(ents), err)
	}
	return "deleted at the old path with no entry below; made again it is opaque and empty", nil
}

// smallScenario edits a directory of base files, the same way for the mount and the oracle.
func smallScenario(root, dir string, names []string) error {
	p := func(n string) string { return filepath.Join(root, dir, n) }
	steps := []func() error{
		func() error { return os.WriteFile(p(names[0]), []byte("overwritten\n"), 0o644) },
		func() error { return os.Remove(p(names[1])) },
		func() error { return os.WriteFile(p("fz_ov_new.txt"), []byte("new\n"), 0o644) },
		func() error { return os.MkdirAll(p("fz_ov_sub/deep"), 0o755) },
		func() error { return os.WriteFile(p("fz_ov_sub/deep/a"), []byte("a\n"), 0o755) },
		func() error { return os.Rename(p(names[2]), p("fz_ov_renamed.txt")) },
		func() error { return os.Chmod(p(names[3]), 0o755) },
		func() error { return os.Symlink("../elsewhere", p("fz_ov_link")) },
		func() error { return unix.Mkfifo(p("fz_ov_fifo"), 0o644) },
		func() error { return os.Mkdir(p("fz_ov_empty"), 0o755) },
	}
	for i, s := range steps {
		if err := s(); err != nil {
			return fmt.Errorf("step %d in %s: %w", i, root, err)
		}
	}
	return nil
}

// dirtyVsOracleSmall applies a fixed edit script to one base directory of the mount and of a plain worktree,
// builds the tree from the dirty set as vfsfuzz does, and compares the entries below that directory.
func dirtyVsOracleSmall(e *env) (string, error) {
	if *alt == "" {
		return "", fmt.Errorf("-alt or ALT_OBJECTS must name the objects of the base commit")
	}
	tok, err := e.ctl.Call("token")
	if err != nil {
		return "", err
	}
	o, err := overlaykit.NewOracle(*gitdir, *shadow, *alt, tok.BaseTree)
	if err != nil {
		return "", err
	}
	if err := o.RefuseGitlinks(tok.BaseTree); err != nil {
		return "", err
	}
	d, _, err := e.pool.dir(e.lower)
	if err != nil {
		return "", err
	}
	ents, err := os.ReadDir(filepath.Join(e.lower, d))
	if err != nil {
		return "", err
	}
	var names []string
	for _, en := range ents {
		if en.Type().IsRegular() {
			names = append(names, en.Name())
		}
	}
	if len(names) < 4 {
		return "", errSkip(fmt.Sprintf("the directory %s has %d plain files, the script needs 4", d, len(names)))
	}
	slices.Sort(names)
	if err := smallScenario(e.mnt, d, names); err != nil {
		return "", err
	}
	_, rep, err := e.dirty()
	if err != nil {
		return "", err
	}
	if err := smallScenario(o.Shadow, d, names); err != nil {
		return "", err
	}
	want, err := o.Tree(true)
	if err != nil {
		return "", err
	}
	g := overlaykit.Git{Dir: *gitdir, Index: filepath.Join(*gitdir, "apply.index")}
	got, err := overlaykit.Apply(g, rep.BaseTree, e.mnt, rep.Entries)
	if err != nil {
		return "", err
	}
	a, _ := g.Run(nil, "ls-tree", "-r", got, d+"/")
	b, _ := g.Run(nil, "ls-tree", "-r", want, d+"/")
	if a != b {
		return "", fmt.Errorf("entries below %s differ\nfrom the dirty set:\n%s\nfrom the oracle:\n%s", d, a, b)
	}
	return fmt.Sprintf("%d entries below %s equal", strings.Count(b, "\n"), d), nil
}

// renameBigDir renames the base directory of -bigdir and reports the size and the time of the dirty reply,
// which lists every leaf below the new path. It requires each leaf and the deleted old path in the reply.
func renameBigDir(e *env) (string, error) {
	if *bigdir == "" {
		return "", errSkip("set -bigdir to a large base directory, for example the source tree of the repository")
	}
	ls, err := leaves(e.lower, *bigdir)
	if err != nil {
		return "", err
	}
	moved := *bigdir + "-fz-moved"
	if err := os.Rename(e.path(*bigdir), e.path(moved)); err != nil {
		return "", fmt.Errorf("rename of %s (%d leaves): %w", *bigdir, len(ls), err)
	}
	t0 := time.Now()
	dd, rep, err := e.dirty()
	if err != nil {
		return "", err
	}
	raw, _ := json.Marshal(rep.Entries)
	e.info("OBSERVED dirty after the rename of %d leaves: %d entries, about %d bytes of JSON, settled in %s", len(ls), len(dd), len(raw), time.Since(t0).Round(time.Millisecond))
	if err := listed(dd, "deleted", *bigdir); err != nil {
		return "", err
	}
	for _, l := range ls {
		if _, ok := dd[filepath.Join(moved, l)]; !ok {
			return "", fmt.Errorf("moved leaf %s is not in the dirty set", l)
		}
	}
	return fmt.Sprintf("%d leaves listed at the new path", len(ls)), nil
}
