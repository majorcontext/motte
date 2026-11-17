//go:build linux

package lower

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// The mount is read-only, so the kernel answers EROFS to a write before it reaches the daemon. These methods
// answer EROFS for a caller that does reach them. Link, Mknod, Setxattr, Removexattr and CopyFileRange are not
// implemented.

func (v *VFS) erofs(ctx context.Context, op opKind) syscall.Errno {
	_, t := v.begin(ctx, op)
	t.done(syscall.EROFS, 0)
	return syscall.EROFS
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, n.v.erofs(ctx, opMkdir)
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, n.v.erofs(ctx, opSymlink)
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return nil, nil, 0, n.v.erofs(ctx, opCreate)
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.v.erofs(ctx, opUnlink)
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno { return n.v.erofs(ctx, opRmdir) }

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return n.v.erofs(ctx, opRename)
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return n.v.erofs(ctx, opSetattr)
}

func (n *Node) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	return 0, n.v.erofs(ctx, opWrite)
}

func (n *Node) Allocate(ctx context.Context, f fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	return n.v.erofs(ctx, opAllocate)
}

func (h *fhandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	return 0, h.v.erofs(ctx, opWrite)
}

func (h *fhandle) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	return h.v.erofs(ctx, opAllocate)
}
