//go:build linux

// overlaycheck runs scripted checks against a workspace that is an overlayfs over the read-only FUSE lower
// layer of motte-vfsd (the daemon runs with -upper and -ctl-sock). Each check prints one PASS, FAIL or SKIP
// line, and INFO lines for what it observed. The exit status is non-zero when a check fails. Run it with
//
//	ALT_OBJECTS=<git objects directory of the base commit> go run ./internal/vfs/testdata/overlaycheck \
//	  -mnt /workspace -sock <ctl socket> -upper <upper dir> -lower <lower mount> [-only name]
//
// The checks change the workspace and leave their edits in the upper directory. Start from a fresh upper.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/majorcontext/motte/internal/vfs/testdata/overlaykit"
)

var (
	mnt    = flag.String("mnt", "/workspace", "the merged overlay mount")
	sock   = flag.String("sock", "/run/motte-vfs/ctl.sock", "control socket of the daemon")
	upper  = flag.String("upper", "", "upper directory of the overlay (read on the node)")
	lower  = flag.String("lower", "", "FUSE lower mount")
	only   = flag.String("only", "", "run only this check")
	alt    = flag.String("alt", os.Getenv("ALT_OBJECTS"), "git object directories that hold the base commit, separated by :")
	shadow = flag.String("shadow", "/tmp/overlaycheck-shadow", "oracle worktree of dirty-vs-oracle-small")
	gitdir = flag.String("gitdir", "/tmp/overlaycheck-shadow.git", "oracle git dir of dirty-vs-oracle-small")
	bigdir = flag.String("bigdir", "", "base directory that rename-big-dir renames (skipped when empty)")
)

type check struct {
	name string
	run  func(*env) (string, error)
}

var checks = []check{
	{"mmap-two-stores", mmapTwoStores},
	{"mmap-readonly-lower", mmapReadonlyLower},
	{"hardlink-across-layers", hardlinkAcrossLayers},
	{"inode-stability", inodeStability},
	{"xattr", xattrCheck},
	{"readonly-lower", readonlyLower},
	{"restart-recovery", restartRecovery},
	{"rename-base-dir", renameBaseDir},
	{"metacopy", metacopy},
	{"deleted-dir-tree", deletedDirTree},
	{"dirty-vs-oracle-small", dirtyVsOracleSmall},
	{"rename-big-dir", renameBigDir},
}

// errSkip marks a check that cannot run or that the operator runs by hand.
type errSkip string

func (e errSkip) Error() string { return string(e) }

type env struct {
	mnt, upper, lower string
	ctl               overlaykit.Ctl
	pool              *pool
	cur               string
}

func (e *env) info(format string, a ...any) {
	fmt.Printf("INFO %s: %s\n", e.cur, fmt.Sprintf(format, a...))
}

// needRoot skips a check that reads trusted.* attributes when the tool runs as another user.
func needRoot() error {
	if os.Geteuid() != 0 {
		return errSkip("reading trusted.* attributes needs CAP_SYS_ADMIN: run this check as root on the host")
	}
	return nil
}

func (e *env) path(rel string) string { return filepath.Join(e.mnt, rel) }

// dirty settles the dirty set as the shim does and indexes it by path.
func (e *env) dirty() (map[string]overlaykit.Entry, overlaykit.Reply, error) {
	r, _, err := e.ctl.StableDirty(e.mnt, true, 10, 60*time.Millisecond)
	m := map[string]overlaykit.Entry{}
	for _, en := range r.Entries {
		m[en.Path] = en
	}
	return m, r, err
}

// listed requires each path to be in the dirty set with the kind.
func listed(d map[string]overlaykit.Entry, kind string, paths ...string) error {
	for _, p := range paths {
		if en, ok := d[p]; !ok || en.Kind != kind {
			return fmt.Errorf("dirty set has %+v for %s (present=%v), want kind %s", en, p, ok, kind)
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if *lower == "" || *upper == "" {
		fmt.Fprintln(os.Stderr, "overlaycheck needs -lower and -upper")
		os.Exit(2)
	}
	p, err := discover(*lower)
	if err != nil {
		fmt.Fprintln(os.Stderr, "overlaycheck: reading the lower mount:", err)
		os.Exit(2)
	}
	if *bigdir != "" {
		p.take(strings.Trim(*bigdir, "/"))
	}
	e := &env{mnt: *mnt, upper: *upper, lower: *lower, ctl: overlaykit.Ctl{Sock: *sock}, pool: p}
	failed, ran, skipped := 0, 0, 0
	for _, c := range checks {
		if *only != "" && *only != c.name {
			continue
		}
		ran++
		e.cur = c.name
		sum, err := c.run(e)
		var skip errSkip
		switch {
		case errors.As(err, &skip):
			skipped++
			fmt.Printf("SKIP %s: %s\n", c.name, skip)
		case err != nil:
			failed++
			fmt.Printf("FAIL %s: %v\n", c.name, err)
		default:
			fmt.Printf("PASS %s: %s\n", c.name, sum)
		}
	}
	if ran == 0 {
		fmt.Fprintf(os.Stderr, "overlaycheck: no check named %q; names: %s\n", *only, names())
		os.Exit(2)
	}
	fmt.Printf("RESULT checks=%d failed=%d skipped=%d\n", ran, failed, skipped)
	if failed > 0 {
		os.Exit(1)
	}
}

func names() string {
	var n []string
	for _, c := range checks {
		n = append(n, c.name)
	}
	return strings.Join(n, ", ")
}
