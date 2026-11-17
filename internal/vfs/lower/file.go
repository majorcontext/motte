//go:build linux

package lower

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

func (n *Node) Open(ctx context.Context, flags uint32) (_ fs.FileHandle, _ uint32, rerr syscall.Errno) {
	ctx, t := n.v.begin(ctx, opOpen)
	defer func() { t.done(rerr, 0) }()
	v := n.v
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC|syscall.O_APPEND|syscall.O_CREAT) != 0 {
		return nil, 0, syscall.EROFS
	}
	if v.cfg.NoOpen {
		return nil, 0, syscall.ENOSYS
	}
	p := n.path()
	t.at(p)
	vw, errno := v.resolve(ctx, p)
	if errno != 0 {
		return nil, 0, errno
	}
	if vw.isDir {
		return nil, 0, syscall.EISDIR
	}
	bp, err := v.c.Blob(ctx, vw.be.SHA, v.readAhead(p))
	if err != nil {
		return nil, 0, syscall.EIO
	}
	fd, err := syscall.Open(bp, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	return &fhandle{v: v, fd: fd}, fuse.FOPEN_KEEP_CACHE, 0
}

// fhandle is an open cached blob.
type fhandle struct {
	v  *VFS
	fd int
}

func (h *fhandle) Read(ctx context.Context, dest []byte, off int64) (res fuse.ReadResult, rerr syscall.Errno) {
	_, t := h.v.begin(ctx, opRead)
	defer func() { t.done(rerr, readSize(res)) }()
	return fuse.ReadResultFd(uintptr(h.fd), off, len(dest)), 0
}

// Flush answers ENOSYS so the kernel stops sending FLUSH.
func (h *fhandle) Flush(ctx context.Context) syscall.Errno {
	_, t := h.v.begin(ctx, opFlush)
	t.done(0, 0)
	return syscall.ENOSYS
}

func (h *fhandle) Release(ctx context.Context) (rerr syscall.Errno) {
	_, t := h.v.begin(ctx, opRelease)
	defer func() { t.done(rerr, 0) }()
	return fs.ToErrno(syscall.Close(h.fd))
}

func (h *fhandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	n, err := unix.Seek(h.fd, int64(off), int(whence))
	return uint64(n), fs.ToErrno(err)
}

// Fsync succeeds: the lower layer holds nothing dirty.
func (n *Node) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno { return 0 }

// Read serves handle-less reads when the kernel runs without OPEN (no_open).
func (n *Node) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (res fuse.ReadResult, rerr syscall.Errno) {
	if h, ok := f.(*fhandle); ok {
		return h.Read(ctx, dest, off)
	}
	ctx, t := n.v.begin(ctx, opRead)
	defer func() { t.done(rerr, readSize(res)) }()
	p := n.path()
	t.at(p)
	v := n.v
	vw, errno := v.resolve(ctx, p)
	if errno != 0 {
		return nil, errno
	}
	if vw.isDir {
		return nil, syscall.EISDIR
	}
	path, err := v.c.Blob(ctx, vw.be.SHA, v.readAhead(p))
	if err != nil {
		return nil, syscall.EIO
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	defer func() { _ = syscall.Close(fd) }()
	m, err := syscall.Pread(fd, dest, off)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	return fuse.ReadResultData(dest[:m]), 0
}
