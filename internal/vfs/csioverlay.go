//go:build linux

package vfs

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/majorcontext/motte/internal/gitpack"
	volattr "github.com/majorcontext/motte/volume"
)

const (
	overlayOptions   = "redirect_dir=on,metacopy=on,index=off,xino=off,nfs_export=off"
	legacyOverlayDir = "overlay"
)

// overlayPaths are the directories of one overlay volume under the state directory.
type overlayPaths struct{ vol, lower, upper, work, base string }

func (p *NodePlugin) overlayPaths(id string) (overlayPaths, error) {
	if id == "." || id == ".." || strings.ContainsAny(id, ",:\\/") || strings.ContainsAny(p.StateDir, ",:\\") {
		return overlayPaths{}, status.Error(codes.InvalidArgument, "the volume id or the state directory holds a character that overlayfs options cannot carry")
	}
	vol := p.volDir(id)
	return overlayPaths{vol: vol, lower: filepath.Join(vol, "lower"), upper: filepath.Join(vol, "upper"), work: filepath.Join(vol, "work"), base: filepath.Join(vol, "base")}, nil
}

func (o overlayPaths) options() string {
	return "lowerdir=" + o.lower + ",upperdir=" + o.upper + ",workdir=" + o.work + "," + overlayOptions
}

func (p *NodePlugin) mountOps() mounter {
	if p.mnt != nil {
		return p.mnt
	}
	return osMounter{}
}

// isOverlayVolume reports whether an overlay publish wrote the base file of the volume. The file exists before any
// mount of the volume does.
func (p *NodePlugin) isOverlayVolume(id string) bool {
	o, err := p.overlayPaths(id)
	if err != nil {
		return false
	}
	_, err = os.Lstat(o.base)
	return err == nil
}

// layer is the lower layer of an overlay volume with the control socket that its VFS serves.
type layer struct {
	lower   lowerLayer
	l       net.Listener
	stop    func()
	mounted bool
}

// release unmounts the FUSE lower, stops the control socket and closes the VFS. It never touches the overlay.
// A lower that does not unmount is detached, which is safe only because the caller has unmounted the overlay.
func (y *layer) release(m mounter, dir string) error {
	if y.mounted {
		if err := y.lower.unmount(); err != nil {
			if derr := m.unmount(dir, syscall.MNT_DETACH); derr != nil {
				return err
			}
		}
		y.mounted = false
	}
	if y.stop != nil {
		y.stop()
	} else if y.l != nil {
		_ = y.l.Close()
	}
	y.lower.close()
	return nil
}

// umount unmounts target without MNT_DETACH. A path that is no mount point is already gone; EBUSY is retryable.
func umount(m mounter, target string) error {
	err := m.unmount(target, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EINVAL), errors.Is(err, syscall.ENOENT):
		return nil
	case errors.Is(err, syscall.EBUSY):
		return status.Errorf(codes.Unavailable, "unmount %s: %v", target, err)
	}
	return status.Errorf(codes.Internal, "unmount %s: %v", target, err)
}

// publishOverlay mounts the workspace as a kernel overlay over a read-only FUSE lower layer. It holds no plugin lock.
func (p *NodePlugin) publishOverlay(ctx context.Context, req *csi.NodePublishVolumeRequest, pub Publication, tel Telemetry) (vol *volume, err error) {
	base, err := gitpack.ParseSHA(pub.Commit)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	o, err := p.overlayPaths(req.VolumeId)
	if err != nil {
		return nil, err
	}
	ctlDir, err := p.ctlDir(req.VolumeContext)
	if err != nil {
		return nil, err
	}
	m := p.mountOps()
	ents, err := m.mounts()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read mountinfo: %v", err)
	}
	edits, err := dirHasEntries(o.upper)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer func() {
		if err != nil {
			err = p.failPublish(ctx, m, o, edits, tel, err)
		}
	}()
	if err = os.MkdirAll(o.vol, 0o700); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err = m.probe(o.upper); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "the node plugin needs CAP_SYS_ADMIN and trusted.* attributes on %s: %v", p.StateDir, err)
	}
	if err = releaseStale(m, ents, o, req.TargetPath); err != nil {
		return nil, err
	}
	if err = p.prepareVolume(ctx, o, pub.Commit, filepath.Base(ctlDir), tel); err != nil {
		return nil, err
	}
	return p.mountVolume(ctx, req, pub, tel, o, base, ctlDir)
}

// failPublish keeps the volume when upper held edits before the attempt, or when a mount of the volume is left,
// and answers Unavailable so that the kubelet tracks the volume and unpublishes it later. Otherwise nothing durable
// exists: it removes the volume and returns err. It never removes the control directory of the pod, because the
// control socket volume may be bound to it.
func (p *NodePlugin) failPublish(ctx context.Context, m mounter, o overlayPaths, edits bool, tel Telemetry, err error) error {
	if !edits {
		if ents, merr := m.mounts(); merr == nil && len(referencing(ents, o)) == 0 {
			if rerr := removeVolume(ctx, o, tel); rerr == nil {
				return err
			}
		}
	}
	if status.Code(err) == codes.Unavailable {
		return err
	}
	return status.Errorf(codes.Unavailable, "%v", err)
}

// removeVolume deletes the entries of the volume directory one by one and the lower mount point with os.Remove,
// because a RemoveAll through a dead FUSE lower never succeeds. A lower that stays is logged, not an error.
func removeVolume(ctx context.Context, o overlayPaths, tel Telemetry) error {
	names, err := os.ReadDir(o.vol)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	for _, n := range names {
		if path := filepath.Join(o.vol, n.Name()); path != o.lower {
			if err := os.RemoveAll(path); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		}
	}
	if err := os.Remove(o.lower); err != nil && !errors.Is(err, os.ErrNotExist) {
		tel.logError(ctx, "the lower mount point stays", err)
	}
	if err := os.Remove(o.vol); err != nil && !errors.Is(err, os.ErrNotExist) {
		tel.logError(ctx, "the volume directory stays", err)
	}
	return nil
}

// referencing lists the mounts that hold the upper directory or the lower mount point of a volume.
func referencing(ents []mountEntry, o overlayPaths) []mountEntry {
	var out []mountEntry
	for _, e := range ents {
		if e.FSType == "overlay" && e.upperdir() == o.upper || e.Mountpoint == o.lower {
			out = append(out, e)
		}
	}
	return out
}

// releaseStale makes room for a fresh mount. The plugin serves a ready volume itself, so an overlay of this volume
// at the target belongs to a plugin that died: its FUSE lower is dead. That overlay goes first. Then no overlay may
// reference upper, because a pod that still runs keeps the superblock in its own mount namespace and the plugin never
// mounts under it. Only then the stale FUSE lower goes.
func releaseStale(m mounter, ents []mountEntry, o overlayPaths, target string) error {
	var atTarget, atLower bool
	for _, e := range ents {
		switch {
		case e.Mountpoint == target && (e.FSType == "overlay" && e.upperdir() == o.upper || e.FSType == "fuse.motte-vfs"):
			atTarget = true
		case e.Mountpoint == o.lower:
			atLower = true
		}
	}
	if atTarget {
		if err := umount(m, target); err != nil {
			return err
		}
		var err error
		if ents, err = m.mounts(); err != nil {
			return status.Errorf(codes.Internal, "read mountinfo: %v", err)
		}
	}
	if err := noOverlayRefs(ents, o); err != nil {
		return err
	}
	if atLower {
		return unmountLower(m, o.lower)
	}
	return nil
}

// noOverlayRefs fails with Unavailable while an overlay of upper is mounted anywhere.
func noOverlayRefs(ents []mountEntry, o overlayPaths) error {
	for _, e := range ents {
		if e.FSType == "overlay" && e.upperdir() == o.upper {
			return status.Errorf(codes.Unavailable, "an overlay at %s (pid %d, mount namespace %s) still references %s", e.Mountpoint, e.PID, e.NS, o.upper)
		}
	}
	return nil
}

// unmountLower unmounts a FUSE lower that this process does not serve. A dead FUSE unmounts plainly; the lazy
// unmount is the last resort, and the caller has shown that no overlay references the lower.
func unmountLower(m mounter, dir string) error {
	err := umount(m, dir)
	if status.Code(err) == codes.OK {
		return nil
	}
	if derr := m.unmount(dir, syscall.MNT_DETACH); derr != nil && !errors.Is(derr, syscall.EINVAL) && !errors.Is(derr, syscall.ENOENT) {
		return err
	}
	return nil
}

// prepareVolume decides what survives of an earlier upper directory, then writes the base file. A non-empty upper
// directory whose base file is missing or names another commit is discarded: the control plane holds everything
// that the workspace reported. The base file is durable before any mount.
func (p *NodePlugin) prepareVolume(ctx context.Context, o overlayPaths, commit, podUID string, tel Telemetry) error {
	fail := func(err error) error { return status.Error(codes.Internal, err.Error()) }
	if err := os.WriteFile(filepath.Join(o.vol, podUIDFile), []byte(podUID), 0o600); err != nil {
		return fail(err)
	}
	if _, err := os.Lstat(filepath.Join(o.vol, legacyOverlayDir)); err == nil {
		tel.logger().WarnContext(ctx, "discarding the overlay directory of a legacy mount", tel.Corr.logAttrs()...)
		if err := os.RemoveAll(filepath.Join(o.vol, legacyOverlayDir)); err != nil {
			return fail(err)
		}
	}
	have, err := os.ReadFile(o.base)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	used, err := dirHasEntries(o.upper)
	if err != nil {
		return fail(err)
	}
	if used && strings.TrimSpace(string(have)) != commit {
		tel.logger().WarnContext(ctx, "discarding the upper directory: the base file names another commit or is missing", tel.Corr.logAttrs()...)
		if err := os.RemoveAll(o.upper); err != nil {
			return fail(err)
		}
	}
	if err := os.RemoveAll(o.work); err != nil {
		return fail(err)
	}
	if err := os.MkdirAll(o.upper, 0o755); err != nil {
		return fail(err)
	}
	if err := os.Chown(o.upper, int(p.UID), int(p.GID)); err != nil {
		return fail(err)
	}
	if err := os.Chmod(o.upper, 0o755); err != nil {
		return fail(err)
	}
	if err := os.Mkdir(o.work, 0o700); err != nil {
		return fail(err)
	}
	if err := writeBase(o, commit); err != nil {
		return fail(err)
	}
	return nil
}

func dirHasEntries(dir string) (bool, error) {
	f, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	return err == nil, err
}

// writeBase replaces the base file atomically and makes it durable: write and sync a new file, rename it, sync the
// directory.
func writeBase(o overlayPaths, commit string) error {
	tmp := o.base + ".new"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(commit)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, o.base); err != nil {
		return err
	}
	d, err := os.Open(o.vol)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// mountVolume opens the commit, mounts the FUSE lower and then the overlay at the target. A failure undoes the
// steps in reverse order and keeps the volume directory.
func (p *NodePlugin) mountVolume(ctx context.Context, req *csi.NodePublishVolumeRequest, pub Publication, tel Telemetry, o overlayPaths, base gitpack.SHA, ctlDir string) (*volume, error) {
	m := p.mountOps()
	st := &HTTPStore{BaseURL: p.APIURL, Client: p.HTTP, Tel: tel, Grant: func() (string, error) { return p.Grant(pub), nil }}
	cacheDir := filepath.Join(p.StateDir, "cache")
	lower, err := m.openLower(ctx, Config{
		Store: st, CacheDir: cacheDir, UpperDir: o.upper, CtlSock: filepath.Join(ctlDir, volattr.CtlSocket), Base: base,
		UID: p.UID, GID: p.GID, Cache: NewCache(st, cacheDir), Clock: p.Clock, Telemetry: tel,
	})
	if errors.Is(err, ErrGrantRefused) {
		return nil, status.Errorf(codes.PermissionDenied, "open %s: %v", pub.Repo, err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open %s: %v", pub.Repo, err)
	}
	y := &layer{lower: lower}
	undo := func(err error) (*volume, error) {
		if rerr := y.release(m, o.lower); rerr != nil {
			tel.logError(ctx, "undo of a failed publish", rerr)
		}
		return nil, err
	}
	if err := os.Mkdir(o.lower, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return undo(status.Error(codes.Internal, err.Error()))
	}
	if err := os.MkdirAll(req.TargetPath, 0o755); err != nil {
		return undo(status.Error(codes.Internal, err.Error()))
	}
	if y.l, err = listenCtl(filepath.Join(ctlDir, volattr.CtlSocket), int(p.UID), int(p.GID)); err != nil {
		return undo(status.Errorf(codes.Internal, "control socket: %v", err))
	}
	if err := lower.mount(ctx, o.lower); err != nil {
		return undo(status.Errorf(codes.Internal, "mount: %v", err))
	}
	y.mounted = true
	y.stop = lower.serve(y.l, func(err error) { tel.logError(ctx, "control socket failed", err) })
	if err := m.mountOverlay(req.TargetPath, o.options()); err != nil {
		return undo(status.Errorf(codes.Internal, "mount overlay: %v", err))
	}
	return &volume{ready: true, lay: y, tel: tel}, nil
}

// unpublishOverlay unmounts the overlay, checks that no mount namespace still references upper, then unmounts the
// FUSE lower, and only then deletes the volume directory. While a reference remains it fails and leaves the lower
// running. It is the one place that deletes upper and work.
func (p *NodePlugin) unpublishOverlay(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, vol *volume, tel Telemetry) (*csi.NodeUnpublishVolumeResponse, error) {
	o, err := p.overlayPaths(req.VolumeId)
	if err != nil {
		return nil, err
	}
	m := p.mountOps()
	if err := umount(m, req.TargetPath); err != nil {
		return nil, err
	}
	ents, err := m.mounts()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read mountinfo: %v", err)
	}
	if err := noOverlayRefs(ents, o); err != nil {
		return nil, err
	}
	if vol != nil {
		if err := vol.lay.release(m, o.lower); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount %s: %v", o.lower, err)
		}
	} else if err := unmountLower(m, o.lower); err != nil {
		return nil, err
	}
	if uid, err := os.ReadFile(filepath.Join(o.vol, podUIDFile)); err == nil {
		if err := os.RemoveAll(filepath.Join(p.StateDir, "ctl", string(uid))); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if err := removeVolume(ctx, o, tel); err != nil {
		return nil, err
	}
	delete(p.vols, req.VolumeId)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
