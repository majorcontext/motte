//go:build linux

package lower

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type Node struct {
	fs.Inode
	v *VFS
}

var (
	_ fs.NodeGetattrer  = (*Node)(nil)
	_ fs.NodeLookuper   = (*Node)(nil)
	_ fs.NodeReaddirer  = (*Node)(nil)
	_ fs.NodeReadlinker = (*Node)(nil)
	_ fs.NodeMkdirer    = (*Node)(nil)
	_ fs.NodeSymlinker  = (*Node)(nil)
	_ fs.NodeCreater    = (*Node)(nil)
	_ fs.NodeUnlinker   = (*Node)(nil)
	_ fs.NodeRmdirer    = (*Node)(nil)
	_ fs.NodeRenamer    = (*Node)(nil)
	_ fs.NodeSetattrer  = (*Node)(nil)
	_ fs.NodeOpener     = (*Node)(nil)
	_ fs.NodeFsyncer    = (*Node)(nil)
	_ fs.NodeReader     = (*Node)(nil)
	_ fs.NodeWriter     = (*Node)(nil)
	_ fs.NodeAllocater  = (*Node)(nil)
)

func (v *VFS) Root() *Node { return &Node{v: v} }

func (n *Node) path() string { return n.Path(n.Root()) }

func (v *VFS) fillAttr(p string, vw *view, a *fuse.Attr) {
	a.Ino = inoOf(p)
	if p == "" {
		a.Ino = 1
	}
	mt := uint64(v.mtime.Unix())
	a.Atime, a.Mtime, a.Ctime = mt, mt, mt
	a.Owner = fuse.Owner{Uid: v.cfg.UID, Gid: v.cfg.GID}
	a.Blksize = 4096
	switch {
	case vw.isDir:
		a.Mode = syscall.S_IFDIR | 0o755
		a.Size = 4096
		a.Nlink = 2
	case vw.isLink:
		a.Mode = syscall.S_IFLNK | 0o777
		a.Size = uint64(vw.be.Size)
		a.Nlink = 1
	default:
		a.Mode = syscall.S_IFREG | 0o644
		if vw.be.IsExec() {
			a.Mode = syscall.S_IFREG | 0o755
		}
		a.Size = uint64(vw.be.Size)
		a.Nlink = 1
		a.Blocks = (a.Size + 511) / 512
	}
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) (rerr syscall.Errno) {
	ctx, t := n.v.begin(ctx, opGetattr)
	defer func() { t.done(rerr, 0) }()
	p := n.path()
	t.at(p)
	vw, errno := n.v.resolve(ctx, p)
	if errno == 0 {
		n.v.fillAttr(p, &vw, &out.Attr)
	}
	return errno
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (_ *fs.Inode, rerr syscall.Errno) {
	ctx, t := n.v.begin(ctx, opLookup)
	defer func() { t.done(rerr, 0) }()
	p := join(n.path(), name)
	t.at(p)
	vw, errno := n.v.resolve(ctx, p)
	if errno != 0 {
		return nil, errno
	}
	n.v.fillAttr(p, &vw, &out.Attr)
	return n.NewInode(ctx, &Node{v: n.v}, fs.StableAttr{Mode: vw.kindMode(), Ino: inoOf(p)}), 0
}

func (n *Node) Readdir(ctx context.Context) (_ fs.DirStream, rerr syscall.Errno) {
	ctx, t := n.v.begin(ctx, opReaddir)
	defer func() { t.done(rerr, 0) }()
	p := n.path()
	t.at(p)
	vw, errno := n.v.resolve(ctx, p)
	if errno != 0 {
		return nil, errno
	}
	if !vw.isDir {
		return nil, syscall.ENOTDIR
	}
	return fs.NewListDirStream(listDir(p, &vw)), 0
}

func listDir(p string, vw *view) []fuse.DirEntry {
	if vw.baseTree == nil {
		return nil
	}
	out := make([]fuse.DirEntry, 0, len(vw.baseTree.Entries))
	for _, e := range vw.baseTree.Entries {
		mode := uint32(syscall.S_IFREG)
		switch {
		case e.IsDir() || e.IsGitlink():
			mode = syscall.S_IFDIR
		case e.IsSymlink():
			mode = syscall.S_IFLNK
		}
		out = append(out, fuse.DirEntry{Name: e.Name, Mode: mode, Ino: inoOf(join(p, e.Name))})
	}
	return out
}

func (n *Node) Readlink(ctx context.Context) (_ []byte, rerr syscall.Errno) {
	ctx, t := n.v.begin(ctx, opReadlink)
	defer func() { t.done(rerr, 0) }()
	vw, errno := n.v.resolve(ctx, n.path())
	if errno != 0 {
		return nil, errno
	}
	if !vw.isLink {
		return nil, syscall.EINVAL
	}
	b, err := n.v.c.ReadBlob(ctx, vw.be.SHA)
	if err != nil {
		return nil, syscall.EIO
	}
	return b, 0
}

func (n *Node) Statfs(ctx context.Context, out *fuse.StatfsOut) (rerr syscall.Errno) {
	_, t := n.v.begin(ctx, opStatfs)
	defer func() { t.done(rerr, 0) }()
	var s syscall.Statfs_t
	if err := syscall.Statfs(n.v.upper, &s); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return 0
}
