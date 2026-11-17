//go:build linux

package overlaykit

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const zeroSHA = "0000000000000000000000000000000000000000"

// Apply builds the tree that the shim builds from a dirty set, using a temporary index seeded from base. The
// bytes and modes of the listed files come from the merged directory. A deleted path and an opaque directory
// remove the base subtree, a file or symlink removes the base subtree that it replaces, and an empty
// directory adds nothing. The index file g.Index is replaced.
func Apply(g Git, base, merged string, es []Entry) (string, error) {
	if g.Index == "" {
		return "", fmt.Errorf("apply needs its own index file")
	}
	_ = os.Remove(g.Index)
	if _, err := g.Run(nil, "read-tree", base); err != nil {
		return "", err
	}
	listing, err := g.Run(nil, "ls-files", "-z")
	if err != nil {
		return "", err
	}
	var paths []string
	if listing != "" {
		paths = strings.Split(strings.TrimSuffix(listing, "\x00"), "\x00")
	}
	sort.Strings(paths)

	var info bytes.Buffer
	for _, e := range es {
		for _, p := range removed(paths, e) {
			fmt.Fprintf(&info, "0 %s\t%s\x00", zeroSHA, p)
		}
	}
	for _, e := range es {
		if e.Kind != "file" && e.Kind != "symlink" {
			continue
		}
		mode, sha, err := blob(g, merged, e)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&info, "%s %s\t%s\x00", mode, sha, e.Path)
	}
	if info.Len() > 0 {
		if _, err := g.Run(info.Bytes(), "update-index", "-z", "--index-info"); err != nil {
			return "", err
		}
	}
	return g.Line("write-tree")
}

// removed lists the sorted base paths that an entry takes out of the tree: the path itself, when the base
// holds it as a leaf, for a deleted path and for any directory (a directory over a base file removes the
// file), and the subtree below it for a deleted path and an opaque directory. update-index replaces the
// subtree below a path that it sets as a file, so a file entry needs no removal.
func removed(paths []string, e Entry) []string {
	var out []string
	if e.Kind == "deleted" || e.Kind == "dir" && e.Opaque {
		under := e.Path + "/"
		for i := sort.SearchStrings(paths, under); i < len(paths) && strings.HasPrefix(paths[i], under); i++ {
			out = append(out, paths[i])
		}
	}
	if e.Kind == "deleted" || e.Kind == "dir" {
		if i := sort.SearchStrings(paths, e.Path); i < len(paths) && paths[i] == e.Path {
			out = append(out, e.Path)
		}
	}
	return out
}

func hash(g Git, stdin []byte, args ...string) (string, error) {
	out, err := g.Run(stdin, append([]string{"hash-object", "-w"}, args...)...)
	return strings.TrimSpace(out), err
}

// blob hashes the file or symlink that an entry names, as the merged directory holds it. A listed path
// that is gone or is of another type, or an exec flag that the merged mode contradicts, is an error: the
// dirty set is wrong.
func blob(g Git, merged string, e Entry) (mode, sha string, err error) {
	p := filepath.Join(merged, e.Path)
	st, err := os.Lstat(p)
	if err != nil {
		return "", "", fmt.Errorf("%s is listed as %s but: %w", e.Path, e.Kind, err)
	}
	switch {
	case st.Mode()&fs.ModeSymlink != 0:
		if e.Kind != "symlink" {
			return "", "", fmt.Errorf("%s is listed as %s but is a symlink", e.Path, e.Kind)
		}
		target, err := os.Readlink(p)
		if err != nil {
			return "", "", err
		}
		sha, err = hash(g, []byte(target), "--stdin")
		return "120000", sha, err
	case st.Mode().IsRegular():
		if e.Kind != "file" {
			return "", "", fmt.Errorf("%s is listed as %s but is a file", e.Path, e.Kind)
		}
		exec := st.Mode()&0o100 != 0
		if exec != e.Exec {
			return "", "", fmt.Errorf("%s is listed with exec=%v but the merged mode is %v", e.Path, e.Exec, st.Mode())
		}
		mode = "100644"
		if exec {
			mode = "100755"
		}
		sha, err = hash(g, []byte(p+"\n"), "--no-filters", "--stdin-paths")
		return mode, sha, err
	}
	return "", "", fmt.Errorf("%s is listed as %s but is neither a file nor a symlink (%v)", e.Path, e.Kind, st.Mode().Type())
}
