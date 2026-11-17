//go:build linux

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/majorcontext/motte/internal/vfs/testdata/overlaykit"
	"golang.org/x/sys/unix"
)

func appendTo(path, data string) error {
	h, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer h.Close()
	_, err = h.WriteString(data)
	return err
}

func nlink(path string) uint64 {
	var st unix.Stat_t
	if unix.Lstat(path, &st) != nil {
		return 0
	}
	return uint64(st.Nlink)
}

// fresh removes a leftover of an earlier run and returns the path of a new name beside file f.
func (e *env) fresh(f, suffix string) string {
	p := e.path(filepath.Join(filepath.Dir(f), "fz_ov_"+suffix))
	_ = os.RemoveAll(p)
	return p
}

// consistent requires a base path to be listed as a file, or, when it is not listed, to read the bytes and
// the exec bit of the base: a metacopy-only upper copy equals the base and the dirty set leaves it out.
func (e *env) consistent(d map[string]overlaykit.Entry, rel string) error {
	if _, ok := d[rel]; ok {
		return listed(d, "file", rel)
	}
	got, gerr := os.ReadFile(e.path(rel))
	want, werr := os.ReadFile(filepath.Join(e.lower, rel))
	gs, _ := os.Lstat(e.path(rel))
	ws, _ := os.Lstat(filepath.Join(e.lower, rel))
	if gerr != nil || werr != nil || !bytes.Equal(got, want) || gs.Mode()&0o100 != ws.Mode()&0o100 {
		return fmt.Errorf("%s is not in the dirty set but differs from the base (read errors: %v, %v)", rel, gerr, werr)
	}
	return nil
}

// hardlinkAcrossLayers links a base file (which copies it up first) and a file of the upper layer, writes
// through one name, and records whether the other name sees the write.
func hardlinkAcrossLayers(e *env) (string, error) {
	f, err := e.pool.file()
	if err != nil {
		return "", err
	}
	orig, err := os.ReadFile(e.path(f))
	if err != nil {
		return "", err
	}
	other := e.fresh(f, "hl_base")
	if err := os.Link(e.path(f), other); err != nil {
		return "", fmt.Errorf("link of a base file: %w", err)
	}
	if b, err := os.ReadFile(other); err != nil || !bytes.Equal(b, orig) {
		return "", fmt.Errorf("the new name does not read the bytes of the base file (err=%v)", err)
	}
	e.info("nlink after link: base name=%d new name=%d", nlink(e.path(f)), nlink(other))
	if err := appendTo(e.path(f), "X"); err != nil {
		return "", err
	}
	b, err := os.ReadFile(other)
	if err != nil {
		return "", err
	}
	e.info("OBSERVED other name sees the write through the base name: %v (index=off can break the link at the copy-up of a lower file)", bytes.Equal(b, append(append([]byte{}, orig...), 'X')))
	n1, n2 := e.fresh(f, "hl_up1"), e.fresh(f, "hl_up2")
	if err := os.WriteFile(n1, []byte("one"), 0o644); err != nil {
		return "", err
	}
	if err := os.Link(n1, n2); err != nil {
		return "", fmt.Errorf("link of an upper file: %w", err)
	}
	if err := appendTo(n2, "two"); err != nil {
		return "", err
	}
	if b, err := os.ReadFile(n1); err != nil || string(b) != "onetwo" {
		return "", fmt.Errorf("two upper names of one file differ after a write: %q (err=%v)", b, err)
	}
	d, _, err := e.dirty()
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(f)
	rel := func(p string) string { return filepath.Join(dir, filepath.Base(p)) }
	if err := listed(d, "file", rel(n1), rel(n2)); err != nil {
		return "", err
	}
	for _, p := range []string{f, rel(other)} {
		if err := e.consistent(d, p); err != nil {
			return "", err
		}
	}
	return "the new upper names are listed; the base names are listed or equal the base; two upper names share one inode's data", nil
}

// dirIno reads d_ino of one name with getdents.
func dirIno(dir, name string) (uint64, error) {
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return 0, err
	}
	defer unix.Close(fd)
	buf := make([]byte, 1<<16)
	for {
		n, err := unix.Getdents(fd, buf)
		if err != nil || n == 0 {
			return 0, errors.Join(errors.New("name not found in getdents"), err)
		}
		for b := buf[:n]; len(b) > 0; {
			reclen := int(binary.LittleEndian.Uint16(b[16:18]))
			if got, _, _ := strings.Cut(string(b[19:reclen]), "\x00"); got == name {
				return binary.LittleEndian.Uint64(b[0:8]), nil
			}
			b = b[reclen:]
		}
	}
}

func ino(path string) uint64 {
	var st unix.Stat_t
	if unix.Lstat(path, &st) != nil {
		return 0
	}
	return st.Ino
}

// inodeStability prints st_ino of base files before and after a copy-up by write and by chmod, and d_ino of
// the write. It always passes: the operator reads CHANGED as the documented limitation of xino=off.
func inodeStability(e *env) (string, error) {
	w, err := e.pool.file()
	if err != nil {
		return "", err
	}
	c, err := e.pool.file()
	if err != nil {
		return "", err
	}
	report := func(what, path string, before uint64) {
		after := ino(path)
		state := "same"
		if after != before {
			state = "CHANGED"
		}
		e.info("%s %s st_ino old=%d new=%d %s", what, path, before, after, state)
	}
	wo, co := ino(e.path(w)), ino(e.path(c))
	if err := appendTo(e.path(w), "X"); err != nil {
		return "", err
	}
	report("after write", e.path(w), wo)
	if d, err := dirIno(filepath.Dir(e.path(w)), filepath.Base(w)); err == nil {
		e.info("d_ino of the written file=%d, st_ino=%d, equal=%v", d, ino(e.path(w)), d == ino(e.path(w)))
	}
	st, err := os.Lstat(e.path(c))
	if err != nil {
		return "", err
	}
	if err := os.Chmod(e.path(c), st.Mode().Perm()^0o100); err != nil {
		return "", err
	}
	report("after chmod", e.path(c), co)
	return "values printed", nil
}

func listxattr(path string) ([]string, error) {
	buf := make([]byte, 1<<14)
	n, err := unix.Llistxattr(path, buf)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range strings.Split(string(buf[:n]), "\x00") {
		if a != "" {
			out = append(out, a)
		}
	}
	return out, nil
}

// xattrCheck sets, reads and lists user attributes on a base file and a new file, requires that no
// trusted.overlay.* attribute shows in the merged view, and requires both files in the dirty set.
func xattrCheck(e *env) (string, error) {
	if err := needRoot(); err != nil {
		return "", err
	}
	f, err := e.pool.file()
	if err != nil {
		return "", err
	}
	n := e.fresh(f, "xattr_new")
	if err := os.WriteFile(n, []byte("new"), 0o644); err != nil {
		return "", err
	}
	for _, p := range []string{e.path(f), n} {
		if err := unix.Lsetxattr(p, "user.fz", []byte("v1"), 0); err != nil {
			return "", fmt.Errorf("setxattr user.fz on %s: %w", p, err)
		}
		buf := make([]byte, 16)
		k, err := unix.Lgetxattr(p, "user.fz", buf)
		if err != nil || string(buf[:k]) != "v1" {
			return "", fmt.Errorf("getxattr user.fz on %s = %q, %v", p, buf[:max(k, 0)], err)
		}
		names, err := listxattr(p)
		if err != nil {
			return "", fmt.Errorf("listxattr on %s: %w", p, err)
		}
		e.info("%s attributes: %v", p, names)
		if !contains(names, "user.fz") {
			return "", fmt.Errorf("listxattr on %s lacks user.fz", p)
		}
		for _, a := range names {
			if strings.HasPrefix(a, "trusted.overlay.") {
				return "", fmt.Errorf("%s is visible through the merged mount on %s", a, p)
			}
		}
		_, gerr := unix.Lgetxattr(p, "trusted.overlay.metacopy", buf)
		if gerr == nil {
			return "", fmt.Errorf("getxattr trusted.overlay.metacopy succeeds on %s", p)
		}
		e.info("getxattr trusted.overlay.metacopy on %s: %v", p, gerr)
	}
	d, _, err := e.dirty()
	if err != nil {
		return "", err
	}
	if err := listed(d, "file", filepath.Join(filepath.Dir(f), filepath.Base(n))); err != nil {
		return "", err
	}
	if err := e.consistent(d, f); err != nil {
		return "", err
	}
	_, isListed := d[f]
	e.info("OBSERVED base file with only an attribute change is listed in the dirty set: %v (git ignores attributes; both answers give the base tree)", isListed)
	return "user.* attributes work; no trusted.overlay.* visible; the new file is listed", nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// readonlyLower requires EROFS for a create, unlink, chmod, mkdir and open for write directly on the lower
// mount.
func readonlyLower(e *env) (string, error) {
	f, err := e.pool.file()
	if err != nil {
		return "", err
	}
	lf := filepath.Join(e.lower, f)
	tries := map[string]error{}
	_, tries["create"] = os.OpenFile(filepath.Join(e.lower, "fz_ov_new"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	tries["unlink"] = os.Remove(lf)
	tries["chmod"] = os.Chmod(lf, 0o777)
	tries["mkdir"] = os.Mkdir(filepath.Join(e.lower, "fz_ov_dir"), 0o755)
	_, tries["open for write"] = os.OpenFile(lf, os.O_WRONLY, 0)
	for _, what := range []string{"create", "unlink", "chmod", "mkdir", "open for write"} {
		if !errors.Is(tries[what], syscall.EROFS) {
			return "", fmt.Errorf("%s on the lower mount: %v, want EROFS", what, tries[what])
		}
	}
	if _, err := os.Stat(lf); err != nil {
		return "", fmt.Errorf("the lower file is gone after the refused unlink: %w", err)
	}
	return "five writes refused with EROFS", nil
}

// metacopy toggles the exec bit twice (the dirty set must not list the file) and once (it must list it as
// exec), and requires that the upper copy of the second file has no data extent.
func metacopy(e *env) (string, error) {
	if err := needRoot(); err != nil {
		return "", err
	}
	a, err := e.pool.file()
	if err != nil {
		return "", err
	}
	b, err := e.pool.file()
	if err != nil {
		return "", err
	}
	for _, step := range []os.FileMode{0o755, 0o644} {
		if err := os.Chmod(e.path(a), step); err != nil {
			return "", err
		}
	}
	if err := os.Chmod(e.path(b), 0o755); err != nil {
		return "", err
	}
	d, _, err := e.dirty()
	if err != nil {
		return "", err
	}
	if en, ok := d[a]; ok {
		return "", fmt.Errorf("%s was changed and changed back, but the dirty set lists %+v", a, en)
	}
	if en, ok := d[b]; !ok || en.Kind != "file" || !en.Exec {
		return "", fmt.Errorf("%s was made executable, dirty set has %+v (present=%v)", b, en, ok)
	}
	up := filepath.Join(e.upper, b)
	var st, base unix.Stat_t
	if err := unix.Lstat(up, &st); err != nil {
		return "", fmt.Errorf("upper copy of %s: %w", b, err)
	}
	_ = unix.Lstat(filepath.Join(e.lower, b), &base)
	e.info("OBSERVED upper copy of the chmod-only file: size=%d blocks=%d, base size=%d", st.Size, st.Blocks, base.Size)
	if _, err := unix.Lgetxattr(up, "trusted.overlay.metacopy", make([]byte, 64)); errors.Is(err, unix.ENOTSUP) {
		return "", errSkip("this sandbox cannot read trusted.* attributes (ENOTSUP); the check is host-only")
	} else if err != nil {
		return "", fmt.Errorf("the upper copy of %s has no trusted.overlay.metacopy (%v): a full copy-up, not a metacopy", b, err)
	}
	if st.Size == 0 {
		return "", errSkip(fmt.Sprintf("%s is empty, so a missing data extent shows nothing", b))
	}
	free, err := dataFree(up)
	if err != nil {
		return "", fmt.Errorf("SEEK_DATA on the upper copy of %s: %w", b, err)
	}
	if !free {
		return "", fmt.Errorf("the metacopy upper copy of %s holds a data extent, want none", b)
	}
	return "chmod and back is not listed; chmod +x is listed as exec; the upper copy is a metacopy with no data extent", nil
}

// dataFree reports whether the file at path holds no data extent: SEEK_DATA fails with ENXIO. The block count
// cannot tell, because ext4 reports st_blocks above 0 for a metacopy upper copy.
func dataFree(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = unix.Seek(int(f.Fd()), 0, unix.SEEK_DATA)
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, unix.ENXIO):
		return true, nil
	}
	return false, err
}
