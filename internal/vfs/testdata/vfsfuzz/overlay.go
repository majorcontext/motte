//go:build linux

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/majorcontext/motte/internal/vfs/testdata/overlaykit"
	"golang.org/x/sys/unix"
)

var (
	overlayDirty int
	tokenRetries int
	prevToken    string
	prevTree     string
	looseFail    int
)

// requireSocket stops a run whose control socket is missing: the daemon serves the socket only.
func requireSocket() {
	if st, err := os.Stat(*sock); err != nil || st.Mode().Type() != fs.ModeSocket {
		fmt.Fprintf(os.Stderr, "vfsfuzz refuses: -sock %s is not a socket\n", *sock)
		os.Exit(2)
	}
}

// requireCleanUpper stops a run whose upper directory already holds edits: the oracle starts from the base
// tree, so any earlier edit would show as a difference.
func requireCleanUpper(r overlaykit.Reply) {
	if len(r.Entries) != 0 {
		fmt.Fprintf(os.Stderr, "vfsfuzz refuses: the upper directory already has %d dirty entries; start from a fresh upper\n", len(r.Entries))
		os.Exit(2)
	}
}

// overlayCheck asks the daemon for the dirty set, applies the set to the base
// tree as the shim does, and compares the tree with the tree of the oracle. It returns the number of
// failures.
func overlayCheck(s int, log []string) int {
	rep, retries, err := overlaykit.Ctl{Sock: *sock}.StableDirty(*mnt, !*skipSync, 10, 60*time.Millisecond)
	tokenRetries += retries
	if err != nil {
		fmt.Printf("SEQ %d DIRTY ERROR: %v ops=%q\n", s, err, log)
		return 1
	}
	overlayDirty += len(rep.Entries)
	g := overlaykit.Git{Dir: *gitdir, Index: filepath.Join(*gitdir, "apply.index")}
	got, err := overlaykit.Apply(g, rep.BaseTree, *mnt, rep.Entries)
	if err != nil {
		fmt.Printf("SEQ %d APPLY ERROR: %v ops=%q\n", s, err, log)
		return 1
	}
	if *freshIdx {
		os.Remove(filepath.Join(*gitdir, "index"))
	}
	git("add", "-A", "-f")
	want := git("write-tree")
	stale := prevToken != "" && rep.Token == prevToken && want != prevTree
	prevToken, prevTree = rep.Token, want
	if stale {
		fmt.Printf("SEQ %d TOKEN UNCHANGED although the tree changed (the snapshot timer would skip this edit) token=%s ops=%q\n", s, rep.Token, log)
		return 1
	}
	if got == want {
		return 0
	}
	fmt.Printf("SEQ %d TREE MISMATCH got=%s want=%s ops=%q\n%s\n", s, got, want, log, g.Report(rep.BaseTree, got, want))
	if d := fullCompare(); d != "" {
		fmt.Println(d)
	}
	return 1
}

func pickOverlayOp() *op {
	switch rng.Intn(7) {
	case 0:
		return linkOp()
	case 1:
		return rmdirmkOp()
	case 2:
		return mvdirbackOp()
	case 3:
		return mvdirontoOp()
	case 4:
		return setxattrOp()
	case 5:
		return mvspecialOp()
	}
	name := filepath.Join(randDir(), newName()+".fifo")
	return &op{"fifo " + name, func(r string) error { return unix.Mkfifo(filepath.Join(r, name), 0o644) }}
}

// linkOp makes a hard link of a file, then often writes through the new name. Git sees two files with equal
// content, which holds only while the overlay keeps the two names one inode.
func linkOp() *op {
	f, ok := randFile()
	if !ok {
		return nil
	}
	dst := filepath.Join(randDir(), newName()+".hl")
	write := *linkWrite && rng.Intn(2) == 0
	data := content()
	return &op{fmt.Sprintf("link %s -> %s write=%v", f, dst, write), func(r string) error {
		if err := os.Link(filepath.Join(r, f), filepath.Join(r, dst)); err != nil {
			return err
		}
		if !write {
			return nil
		}
		h, err := os.OpenFile(filepath.Join(r, dst), os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return err
		}
		defer h.Close()
		_, err = h.Write(data)
		return err
	}}
}

// rmdirmkOp removes a directory and recreates it with new content, also under the name of a removed file.
func rmdirmkOp() *op {
	d := randDir()
	if strings.Count(d, "/") < 2 || !smallDir(d) {
		return nil
	}
	ents, _ := os.ReadDir(filepath.Join(*shadow, d))
	names := []string{"again.txt"}
	for _, e := range ents {
		if e.Type().IsRegular() && len(names) < 3 {
			names = append(names, e.Name())
		}
	}
	datas := make([][]byte, len(names))
	for i := range datas {
		datas[i] = content()
	}
	return &op{"rmdirmk " + d, func(r string) error {
		p := filepath.Join(r, d)
		if err := os.RemoveAll(p); err != nil {
			return err
		}
		if err := os.Mkdir(p, 0o755); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(p, "sub2"), 0o755); err != nil {
			return err
		}
		for i, n := range names {
			if err := os.WriteFile(filepath.Join(p, n), datas[i], 0o644); err != nil {
				return err
			}
		}
		return os.WriteFile(filepath.Join(p, "sub2", "x"), datas[0], 0o644)
	}}
}

// mvdirbackOp renames a directory away and back.
func mvdirbackOp() *op {
	d := randDir()
	if strings.Count(d, "/") < 2 || !smallDir(d) {
		return nil
	}
	away := d + "-away-" + newName()
	return &op{"mvdirback " + d, func(r string) error {
		if err := os.Rename(filepath.Join(r, d), filepath.Join(r, away)); err != nil {
			return err
		}
		return os.Rename(filepath.Join(r, away), filepath.Join(r, d))
	}}
}

// mvdirontoOp renames a directory onto an empty directory: a new one, or a base directory that the op
// removed and made again.
func mvdirontoOp() *op {
	d := randDir()
	if strings.Count(d, "/") < 2 || !smallDir(d) {
		return nil
	}
	if rng.Intn(2) == 0 {
		to := filepath.Join(randDir(), newName())
		if strings.HasPrefix(to, d+"/") || to == d {
			return nil
		}
		return &op{"mvdironto new " + d + " -> " + to, func(r string) error {
			if err := os.Mkdir(filepath.Join(r, to), 0o755); err != nil {
				return err
			}
			return os.Rename(filepath.Join(r, d), filepath.Join(r, to))
		}}
	}
	e := randDir()
	if strings.Count(e, "/") < 2 || !smallDir(e) || e == d || strings.HasPrefix(e, d+"/") || strings.HasPrefix(d, e+"/") {
		return nil
	}
	return &op{"mvdironto removed " + d + " -> " + e, func(r string) error {
		if err := os.RemoveAll(filepath.Join(r, e)); err != nil {
			return err
		}
		if err := os.Mkdir(filepath.Join(r, e), 0o755); err != nil {
			return err
		}
		return os.Rename(filepath.Join(r, d), filepath.Join(r, e))
	}}
}

// setxattrOp sets a user attribute. The oracle may lack user attributes, so the loop compares no errno for
// the op, and only the mount must not break the dirty set.
func setxattrOp() *op {
	f, ok := randFile()
	if !ok {
		return nil
	}
	attr := fmt.Sprintf("user.fz%d", rng.Intn(3))
	val := content()
	if len(val) > 200 {
		val = val[:200]
	}
	return &op{"setxattr " + f + " " + attr, func(r string) error {
		return unix.Setxattr(filepath.Join(r, f), attr, val, 0)
	}}
}

// mvspecialOp renames a symlink or a fifo: one that the directory holds, else a new one.
func mvspecialOp() *op {
	d := randDir()
	to := filepath.Join(randDir(), newName()+".mv")
	var from string
	ents, _ := os.ReadDir(filepath.Join(*shadow, d))
	for _, e := range ents {
		if e.Type()&(os.ModeSymlink|os.ModeNamedPipe) != 0 {
			from = filepath.Join(d, e.Name())
			break
		}
	}
	mk := from == ""
	fifo := rng.Intn(2) == 0
	if mk {
		from = filepath.Join(d, newName()+".mk")
	}
	return &op{fmt.Sprintf("mvspecial %s -> %s fifo=%v", from, to, fifo), func(r string) error {
		if mk {
			var err error
			if fifo {
				err = unix.Mkfifo(filepath.Join(r, from), 0o644)
			} else {
				err = os.Symlink("../t/x", filepath.Join(r, from))
			}
			if err != nil {
				return err
			}
		}
		return os.Rename(filepath.Join(r, from), filepath.Join(r, to))
	}}
}
