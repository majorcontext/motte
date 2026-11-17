//go:build linux

package lower

import (
	"context"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/majorcontext/motte/internal/vfs/tel"
)

type MountOpts struct {
	TTL               time.Duration
	Debug             bool
	DefaultPermission bool
	DirectMount       bool
}

// MountFUSE mounts v at mnt through the host kernel FUSE.
func (v *VFS) MountFUSE(ctx context.Context, mnt string, o MountOpts) (*fuse.Server, error) {
	ctx, span := v.tel.Start(ctx, "vfs.mount", attribute.Bool("direct", o.DirectMount))
	srv, err := v.mountFUSE(mnt, o)
	if err != nil {
		v.tel.LogError(ctx, "vfs.mount failed", err)
	}
	tel.Finish(span, err)
	return srv, err
}

func (v *VFS) mountFUSE(mnt string, o MountOpts) (*fuse.Server, error) {
	if o.TTL == 0 {
		o.TTL = time.Hour
	}
	ttl := o.TTL
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: true, FsName: "motte-vfs", Name: "motte-vfs", Debug: o.Debug,
			MaxWrite: 1 << 20, MaxReadAhead: 1 << 20, MaxBackground: 64, DirectMount: o.DirectMount,
		},
		EntryTimeout: &ttl, AttrTimeout: &ttl, NegativeTimeout: &ttl,
	}
	if o.DefaultPermission {
		opts.Options = append(opts.Options, "default_permissions")
	}
	if o.DirectMount {
		opts.DirectMountStrict = true
		opts.DirectMountFlags = syscall.MS_NOSUID | syscall.MS_NODEV | syscall.MS_RDONLY
	} else {
		opts.Options = append(opts.Options, "ro")
	}
	return fs.Mount(mnt, v.Root(), opts)
}
