//go:build linux

package csi

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/majorcontext/motte/internal/vfs/lower"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/majorcontext/motte/internal/vfs/ctl"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

// mounter is every step of overlay publish that needs a privileged mount or a live file system. The plugin
// calls nothing else that mounts, so a test replaces it.
type mounter interface {
	// probe checks that the process reads trusted.* attributes on the file system of the upper directory.
	probe(upper string) error
	// openLower opens the commit of cfg as a read-only lower layer.
	openLower(ctx context.Context, cfg lower.Config) (lowerLayer, error)
	mountOverlay(target, options string) error
	unmount(target string, flags int) error
	mounts() ([]mountEntry, error)
}

// lowerLayer is the VFS of an overlay volume and its FUSE mount.
type lowerLayer interface {
	mount(ctx context.Context, dir string) error
	// serve answers the control protocol on l until the returned stop function runs.
	serve(l net.Listener, onErr func(error)) (stop func())
	unmount() error
	close()
}

type osMounter struct{}

func (osMounter) probe(upper string) error { return upperdir.ProbeTrustedBeside(upper) }

func (osMounter) openLower(ctx context.Context, cfg lower.Config) (lowerLayer, error) {
	v, err := lower.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &fuseLower{v: v, ctl: &ctl.Server{Store: cfg.Store, Cache: v.Cache(), Workspace: v}}, nil
}

func (osMounter) mountOverlay(target, options string) error {
	return syscall.Mount("overlay", target, "overlay", 0, options)
}

func (osMounter) unmount(target string, flags int) error { return syscall.Unmount(target, flags) }

func (osMounter) mounts() ([]mountEntry, error) { return hostMounts("/proc") }

// CheckHostPID refuses to run the overlay mode unless the process shares the PID namespace of the node and has a
// mount namespace of its own. Without the PID namespace the scan of other pods finds nothing and a stale overlay of a
// running pod goes unseen. readlink is os.Readlink in production.
func CheckHostPID(readlink func(string) (string, error)) error {
	ns := func(path string) string { s, _ := readlink(path); return s }
	pid1, pidSelf := ns("/proc/1/ns/pid"), ns("/proc/self/ns/pid")
	mnt1, mntSelf := ns("/proc/1/ns/mnt"), ns("/proc/self/ns/mnt")
	if pid1 == "" || pid1 != pidSelf || mnt1 == "" || mnt1 == mntSelf {
		return errors.New("overlay mode needs the PID namespace of the node and a mount namespace of its own: run the pod with hostPID: true")
	}
	return nil
}

type fuseLower struct {
	v   *lower.VFS
	srv *fuse.Server
	ctl *ctl.Server
}

func (l *fuseLower) mount(ctx context.Context, dir string) error {
	srv, err := l.v.MountFUSE(ctx, dir, lower.MountOpts{DefaultPermission: true, DirectMount: true})
	if err != nil {
		if srv != nil {
			_ = srv.Unmount()
		}
		return err
	}
	l.srv = srv
	return nil
}

func (l *fuseLower) serve(ln net.Listener, onErr func(error)) func() {
	ctx, stop := context.WithCancel(context.Background())
	go func() {
		if err := l.ctl.Serve(ctx, ln); err != nil {
			onErr(err)
		}
	}()
	return stop
}

func (l *fuseLower) unmount() error { return l.srv.Unmount() }

func (l *fuseLower) close() { l.v.Close() }
