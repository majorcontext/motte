//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/majorcontext/motte/internal/vfs/testdata/overlaykit"
	"golang.org/x/sys/unix"
)

func (e *env) token() (string, error) {
	r, err := e.ctl.Call("token")
	return r.Token, err
}

func firstByte(path string) (byte, error) {
	b := make([]byte, 1)
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	_, err = f.ReadAt(b, 0)
	return b[0], err
}

// mmapTwoStores maps a base file MAP_SHARED, stores twice to its first byte without unmapping, and records
// whether the change token moves after the second store. The spec says it may not. The check passes when
// the merged file and the dirty set show the second store after syncfs.
func mmapTwoStores(e *env) (string, error) {
	f, err := e.pool.file()
	if err != nil {
		return "", err
	}
	h, err := os.OpenFile(e.path(f), os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer h.Close()
	st, err := h.Stat()
	if err != nil {
		return "", err
	}
	m, err := unix.Mmap(int(h.Fd()), 0, int(st.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return "", err
	}
	defer unix.Munmap(m)
	a, b := m[0]^0x55, m[0]^0xaa
	upperByte := func() string {
		v, err := firstByte(filepath.Join(e.upper, f))
		if err != nil {
			return err.Error()
		}
		return fmt.Sprintf("%#x", v)
	}

	m[0] = a
	time.Sleep(100 * time.Millisecond)
	d1, rep1, err := e.dirty()
	if err != nil {
		return "", err
	}
	_, listed1 := d1[f]
	e.info("after store %#x and 100 ms: file listed in dirty=%v", a, listed1)

	m[0] = b
	time.Sleep(100 * time.Millisecond)
	if err := overlaykit.Syncfs(e.mnt); err != nil {
		return "", err
	}
	k2, err := e.token()
	if err != nil {
		return "", err
	}
	unchanged := k2 == rep1.Token
	e.info("OBSERVED token after the second store %#x and syncfs equals the token before it: %v (true means the snapshot timer skips this edit; the change time moves at the first write fault only)", b, unchanged)
	e.info("upper byte after syncfs=%s (informational: the merged file and the upper file share the page cache)", upperByte())
	d, _, err := e.dirty()
	if err != nil {
		return "", err
	}
	got, err := firstByte(e.path(f))
	if err != nil {
		return "", err
	}
	if got != b {
		return "", fmt.Errorf("merged byte is %#x, want the second store %#x", got, b)
	}
	if err := listed(d, "file", f); err != nil {
		return "", err
	}
	e.info("a forced snapshot (jj snapshot --force) is what catches a second store when the token did not change")
	return fmt.Sprintf("token equal after the second store: %v", unchanged), nil
}

// mmapReadonlyLower maps a base file MAP_SHARED and read-only, writes the file through a second descriptor
// (a copy-up), and records whether the mapping sees the new byte. Overlayfs documents that it does not.
func mmapReadonlyLower(e *env) (string, error) {
	f, err := e.pool.file()
	if err != nil {
		return "", err
	}
	r, err := os.Open(e.path(f))
	if err != nil {
		return "", err
	}
	defer r.Close()
	st, err := r.Stat()
	if err != nil {
		return "", err
	}
	m, err := unix.Mmap(int(r.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return "", errors.Join(errors.New("mmap of a lower file MAP_SHARED read-only"), err)
	}
	defer unix.Munmap(m)
	before := m[0]
	w, err := os.OpenFile(e.path(f), os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer w.Close()
	next := []byte{before ^ 0xff}
	if _, err := w.WriteAt(next, 0); err != nil {
		return "", err
	}
	viaFile, err := firstByte(e.path(f))
	if err != nil {
		return "", err
	}
	e.info("OBSERVED mapping sees the write after copy-up: %v (mapping byte %#x, file byte %#x, written %#x)", m[0] == next[0], m[0], viaFile, next[0])
	if viaFile != next[0] {
		return "", fmt.Errorf("a read of the merged file returns %#x after the write, want %#x", viaFile, next[0])
	}
	return fmt.Sprintf("mapping sees the write: %v", m[0] == next[0]), nil
}
