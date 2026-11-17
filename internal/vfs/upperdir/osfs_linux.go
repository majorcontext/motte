package upperdir

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
)

// OSFS reads an upper directory on a local file system. The sandbox can change the tree while the walk
// runs, so OSFS opens the root once and every directory below it one component at a time, and requires the
// opened inode to be the directory that fstatat found: no read goes through a symlink. A component that
// turns into a file or a symlink is gone. Attributes are read through a descriptor of the entry, so the
// walk needs no /proc. Reading the trusted.overlay.* attributes needs CAP_SYS_ADMIN in the initial user
// namespace.
type OSFS struct {
	Dir string
	// Test seams: afterEntries runs after a directory is listed, beforeOpen between fstatat and openat, afterRead after the bytes of a file are read, beforeRead before the target of a symlink.
	afterEntries func(dir string)
	beforeOpen   func(name string)
	afterRead    func(name string)
	beforeRead   func(name string)
}

const (
	openDirFlags  = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	openAttrFlags = unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
)

func mapErr(err error) error {
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) ||
		errors.Is(err, unix.ENXIO) || errors.Is(err, unix.ENODEV) {
		return fs.ErrNotExist
	}
	return err
}

// openAt opens the entry name of the directory fd with flags, after fstatat found a type in want, and
// requires the opened inode to be the one that fstatat found.
func (o *OSFS) openAt(fd int, name string, flags int, want ...uint32) (int, error) {
	var before, after unix.Stat_t
	if err := unix.Fstatat(fd, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return -1, mapErr(err)
	}
	if !slices.Contains(want, before.Mode&unix.S_IFMT) {
		return -1, fs.ErrNotExist
	}
	if o.beforeOpen != nil {
		o.beforeOpen(name)
	}
	next, err := unix.Openat(fd, name, flags, 0)
	if err != nil {
		return -1, mapErr(err)
	}
	if err := unix.Fstat(next, &after); err != nil || after.Dev != before.Dev || after.Ino != before.Ino {
		_ = unix.Close(next)
		return -1, fs.ErrNotExist
	}
	return next, nil
}

type osDir struct {
	o    *OSFS
	fd   int
	path string
}

func (o *OSFS) Open(rel string) (Dir, error) {
	fd, err := unix.Open(o.Dir, openDirFlags, 0)
	if err != nil {
		return nil, mapErr(err)
	}
	d := &osDir{o: o, fd: fd}
	for _, comp := range strings.Split(rel, "/") {
		if comp == "" {
			continue
		}
		next, err := d.Open(comp)
		_ = d.Close()
		if err != nil {
			return nil, err
		}
		d = next.(*osDir)
	}
	return d, nil
}

func (d *osDir) Open(name string) (Dir, error) {
	fd, err := d.o.openAt(d.fd, name, openDirFlags, unix.S_IFDIR)
	if err != nil {
		return nil, err
	}
	return &osDir{o: d.o, fd: fd, path: join(d.path, name)}, nil
}

func (d *osDir) Close() error { return unix.Close(d.fd) }

func direntOf(name string, st *unix.Stat_t) Dirent {
	m := fs.FileMode(st.Mode & 0o777)
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		m |= fs.ModeDir
	case unix.S_IFLNK:
		m |= fs.ModeSymlink
	case unix.S_IFIFO:
		m |= fs.ModeNamedPipe
	case unix.S_IFSOCK:
		m |= fs.ModeSocket
	case unix.S_IFBLK:
		m |= fs.ModeDevice
	case unix.S_IFCHR:
		m |= fs.ModeDevice | fs.ModeCharDevice
	}
	return Dirent{
		Name: name, Mode: m, Size: st.Size, Ctime: st.Ctim.Nano(), Ino: st.Ino,
		Whiteout: st.Mode&unix.S_IFMT == unix.S_IFCHR && st.Rdev == 0,
	}
}

func (d *osDir) Entries() ([]Dirent, error) {
	dup, err := unix.FcntlInt(uintptr(d.fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(dup), "")
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	out := make([]Dirent, 0, len(names))
	for _, n := range names {
		var st unix.Stat_t
		switch err := unix.Fstatat(d.fd, n, &st, unix.AT_SYMLINK_NOFOLLOW); {
		case errors.Is(err, unix.ENOENT):
		case err != nil:
			return nil, err
		default:
			out = append(out, direntOf(n, &st))
		}
	}
	if d.o.afterEntries != nil {
		d.o.afterEntries(d.path)
	}
	return out, nil
}

func (d *osDir) Stat() (Dirent, error) {
	var st unix.Stat_t
	if err := unix.Fstat(d.fd, &st); err != nil {
		return Dirent{}, err
	}
	return direntOf("", &st), nil
}

func (d *osDir) Lstat(name string) (Dirent, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(d.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Dirent{}, mapErr(err)
	}
	return direntOf(name, &st), nil
}

func fgetxattr(fd int, attr string) (string, bool, error) {
	buf := make([]byte, 256)
	for {
		n, err := unix.Fgetxattr(fd, attr, buf)
		switch {
		case err == nil:
			return string(buf[:n]), true, nil
		case errors.Is(err, unix.ENODATA):
			return "", false, nil
		case errors.Is(err, unix.ERANGE):
			buf = make([]byte, 2*len(buf))
		default:
			return "", false, err
		}
	}
}

// Attr reads an attribute of the directory itself through its descriptor, the one that Entries reads.
func (d *osDir) Attr(attr string) (string, bool, error) { return fgetxattr(d.fd, attr) }

// Xattr reads the attribute of a regular file through a descriptor of the file.
func (d *osDir) Xattr(name, attr string) (string, bool, error) {
	fd, err := d.o.openAt(d.fd, name, openAttrFlags, unix.S_IFREG)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = unix.Close(fd) }()
	return fgetxattr(fd, attr)
}

// Read reads a regular file through one descriptor, so the mode, the size and the bytes come from one inode,
// and a symlink through readlinkat between two checks of its inode. A regular file past the limit, or one
// that changes size while it is read, reports its size and no bytes. An entry that turns into another inode,
// or into another type, is gone.
func (d *osDir) Read(name string, limit int64) (Blob, error) {
	var first unix.Stat_t
	if err := unix.Fstatat(d.fd, name, &first, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Blob{}, mapErr(err)
	}
	switch first.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		return d.readLink(name, &first)
	case unix.S_IFREG:
		return d.readFile(name, limit)
	}
	return Blob{}, fs.ErrNotExist
}

func (d *osDir) readFile(name string, limit int64) (Blob, error) {
	fd, err := d.o.openAt(d.fd, name, openAttrFlags, unix.S_IFREG)
	if err != nil {
		return Blob{}, err
	}
	defer func() { _ = unix.Close(fd) }()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return Blob{}, err
	}
	b := Blob{Mode: direntOf(name, &st).Mode, Size: st.Size}
	if st.Size > limit {
		return b, nil
	}
	buf := make([]byte, st.Size)
	for n := 0; n < len(buf); {
		m, err := unix.Pread(fd, buf[n:], int64(n))
		if err != nil {
			return Blob{}, err
		}
		n += m
		b.Read = int64(n)
		if m == 0 {
			return b, nil
		}
	}
	if d.o.afterRead != nil {
		d.o.afterRead(name)
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return Blob{}, err
	}
	if after.Size != st.Size {
		b.Size = after.Size
		return b, nil
	}
	b.Data, b.HasData = buf, true
	return b, nil
}

func (d *osDir) readLink(name string, first *unix.Stat_t) (Blob, error) {
	if d.o.beforeRead != nil {
		d.o.beforeRead(name)
	}
	var target string
	for size := 256; ; size *= 2 {
		buf := make([]byte, size)
		n, err := unix.Readlinkat(d.fd, name, buf)
		if errors.Is(err, unix.EINVAL) {
			return Blob{}, fs.ErrNotExist
		}
		if err != nil {
			return Blob{}, mapErr(err)
		}
		if n < size {
			target = string(buf[:n])
			break
		}
	}
	if d.o.afterRead != nil {
		d.o.afterRead(name)
	}
	var after unix.Stat_t
	if err := unix.Fstatat(d.fd, name, &after, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Blob{}, mapErr(err)
	}
	if after.Ino != first.Ino || after.Mode&unix.S_IFMT != unix.S_IFLNK {
		return Blob{}, fs.ErrNotExist
	}
	return Blob{Mode: direntOf(name, first).Mode, Size: int64(len(target)), Data: []byte(target), HasData: true, Read: int64(len(target))}, nil
}

// ProbeTrustedBeside runs ProbeTrusted on a new directory next to the upper directory, on its file system, and
// removes it. It never writes into the upper directory, which a live overlay can reference. It checks the file
// system of the parent of upper, which is the file system of upper when no mount point lies at upper.
func ProbeTrustedBeside(upper string) error {
	dir, err := os.MkdirTemp(filepath.Dir(filepath.Clean(upper)), ".probe-")
	if err != nil {
		return fmt.Errorf("upperdir: probe directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	return ProbeTrusted(dir)
}

// ProbeTrusted checks that the process can set and read a trusted.* attribute on the file system of dir, a
// directory that no overlay references.
// Without CAP_SYS_ADMIN in the initial user namespace a read of trusted.overlay.* answers no data, and
// Scan would report a wrong dirty set.
func ProbeTrusted(dir string) error {
	f, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return fmt.Errorf("upperdir: probe file: %w", err)
	}
	name := f.Name()
	_ = f.Close()
	defer func() { _ = os.Remove(name) }()
	const attr = "trusted.motte.probe"
	if err := unix.Lsetxattr(name, attr, []byte("1"), 0); err != nil {
		return fmt.Errorf("upperdir: trusted.* attributes need CAP_SYS_ADMIN in the initial user namespace and a file system that holds them: %w", err)
	}
	d, err := (&OSFS{Dir: dir}).Open("")
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	v, ok, err := d.Xattr(filepath.Base(name), attr)
	if err != nil {
		return fmt.Errorf("upperdir: reading the trusted.* attribute back: %w", err)
	}
	if !ok || v != "1" {
		return errors.New("upperdir: trusted.* attribute did not read back (CAP_SYS_ADMIN needed)")
	}
	return nil
}
