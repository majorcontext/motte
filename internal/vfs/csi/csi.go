//go:build linux

package csi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/vfs/store"
	telemetry "github.com/majorcontext/motte/internal/vfs/tel"
	volattr "github.com/majorcontext/motte/volume"
)

// NodePlugin is the CSI node plugin of the virtual workspace. NodePublishVolume mounts the tree of one commit for
// one pod as a kernel overlay over a read-only FUSE lower layer, signs the node grant of the volume and serves the
// control socket in a directory of that pod. NodeUnpublishVolume unmounts it and removes its volume directory and
// the socket directory.
type NodePlugin struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	DriverName string
	NodeID     string
	// StateDir holds the blob cache of the node and the lower mount point, upper directory and work directory of every
	// volume.
	StateDir string
	// APIURL is the store base URL. Grant signs the node grant of a volume; the plugin calls it for every
	// store call, so a mount outlives any one grant.
	APIURL string
	Grant  func(volattr.Publication) (string, error)
	UID    uint32
	GID    uint32
	// AllowedNamespaces lists the namespaces whose pods may publish a volume. The plugin refuses every other pod.
	AllowedNamespaces []string
	HTTP              *http.Client
	Clock             clock.Clock
	// Logger receives the vfs.stats, vfs.health and error records. SlowOp and StatsEvery tune them; zero is the default.
	Logger     *slog.Logger
	SlowOp     time.Duration
	StatsEvery time.Duration

	mnt mounter
	// SweepCache replaces the walk of the node cache that Serve starts in the background.
	SweepCache func(dir string)
	// mu guards vols and ctlOwners. It is never held across a mount, a scan of mountinfo or a delete.
	mu        sync.Mutex
	vols      map[string]*volume
	ctlOwners map[string]string
}

type volume struct {
	// ready is false while one publish or unpublish of the volume runs; a second call then answers Aborted.
	ready bool
	tel   telemetry.Telemetry
	// lay is the lower layer of a workspace volume; a control socket volume has none.
	lay *layer
}

// telemetry names the mount of a volume in its spans and records.
func (p *NodePlugin) telemetry(id string, pub volattr.Publication) telemetry.Telemetry {
	return telemetry.Telemetry{
		Logger: p.Logger, SlowOp: p.SlowOp, StatsEvery: p.StatsEvery,
		Corr: telemetry.Correlation{WorkspaceID: pub.Workspace, Repo: pub.Repo, MountID: id, Mode: "mount", Runtime: pub.Runtime, Node: p.NodeID, Epoch: pub.Epoch},
	}
}

// Mounts is the number of workspaces the plugin serves.
func (p *NodePlugin) Mounts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, v := range p.vols {
		if v.lay != nil {
			n++
		}
	}
	return n
}

// sweepCache removes the temporary files that an earlier process left in the node cache and adds the size of its blobs
// to the tier-2 figure of vfs.health. Serve runs it in the background: it walks the whole cache, and it removes only a
// file that no reader can name.
func (p *NodePlugin) sweepCache(dir string) {
	store.RemoveStrayTemps(dir)
	store.SeedCacheSize(dir)
}

func (p *NodePlugin) volDir(id string) string { return filepath.Join(p.StateDir, "vol", id) }

const ctlDirFile = "ctl-dir"

// maxSocketPath is the longest unix socket path that Linux binds, without the terminating NUL.
const maxSocketPath = 107

func (p *NodePlugin) ctlPath(uid, workspace string) string {
	sum := sha256.Sum256([]byte(uid + "/" + workspace))
	return filepath.Join(p.StateDir, "ctl", hex.EncodeToString(sum[:16]))
}

// ctlDir is the control socket directory of one workspace of a pod. The kubelet names the pod, so a pod cannot name
// the directory of another one, and a pod with two workspace volumes has two directories. The name is a fixed-length
// hash of both, so the socket path stays short for any pod UID and workspace.
func (p *NodePlugin) ctlDir(attrs map[string]string, workspace string) (string, error) {
	uid := attrs[volattr.PodUIDAttr]
	if uid == "" || strings.Trim(uid, "0123456789abcdefABCDEF-") != "" {
		return "", status.Errorf(codes.InvalidArgument, "the publish request names no pod UID (%s); the CSIDriver needs podInfoOnMount", volattr.PodUIDAttr)
	}
	if !volattr.ValidWorkspace(workspace) {
		return "", status.Error(codes.InvalidArgument, "the volume attributes name no valid workspace")
	}
	dir := p.ctlPath(uid, workspace)
	if sock := filepath.Join(dir, volattr.CtlSocket); len(sock) > maxSocketPath {
		return "", status.Errorf(codes.InvalidArgument, "the socket path %s is %d bytes and a unix socket path holds %d; use a shorter state directory", sock, len(sock), maxSocketPath)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	if err := os.Chown(dir, int(p.UID), int(p.GID)); err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return dir, nil
}

// checkNamespace refuses a pod whose namespace the plugin does not list. The attributes of a volume come from the
// pod spec, so the namespace that the kubelet reports is the only part of the request that the pod cannot choose.
func (p *NodePlugin) checkNamespace(attrs map[string]string) error {
	ns := attrs[volattr.PodNamespaceAttr]
	if ns == "" {
		return status.Errorf(codes.InvalidArgument, "the publish request names no pod namespace (%s); the CSIDriver needs podInfoOnMount", volattr.PodNamespaceAttr)
	}
	if !slices.Contains(p.AllowedNamespaces, ns) {
		return status.Errorf(codes.PermissionDenied, "pods of the namespace %q may not publish a workspace volume", ns)
	}
	return nil
}

// removeCtlDir deletes the control directory that the volume directory names.
func (p *NodePlugin) removeCtlDir(volDir string) error {
	rel, err := os.ReadFile(filepath.Join(volDir, ctlDirFile))
	if err != nil || !filepath.IsLocal(string(rel)) {
		return nil
	}
	dir := filepath.Join(p.StateDir, "ctl", string(rel))
	if err := os.RemoveAll(dir); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

// Serve answers CSI calls on the unix socket until ctx ends.
func (p *NodePlugin) Serve(ctx context.Context, socket string) error {
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	p.vols, p.ctlOwners = map[string]*volume{}, map[string]string{}
	s := grpc.NewServer()
	csi.RegisterIdentityServer(s, p)
	csi.RegisterNodeServer(s, p)
	go func() { <-ctx.Done(); s.Stop() }()
	sweep := p.SweepCache
	if sweep == nil {
		sweep = p.sweepCache
	}
	go sweep(filepath.Join(p.StateDir, "cache"))
	return s.Serve(l)
}

func (p *NodePlugin) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: p.DriverName, VendorVersion: "1"}, nil
}

func (p *NodePlugin) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (p *NodePlugin) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (p *NodePlugin) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func (p *NodePlugin) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: p.NodeID}, nil
}

// NodePublishVolume mounts the workspace of the workspace named by the volume attributes at the target path, or binds
// the control socket directory of the workspace for a control socket volume.
func (p *NodePlugin) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if err := p.checkNamespace(req.VolumeContext); err != nil {
		return nil, err
	}
	if req.VolumeId == "" || req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	if volattr.IsCtl(req.VolumeContext) {
		return p.publishCtl(req)
	}
	pub, err := volattr.ParsePublication(req.VolumeContext)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	tel := p.telemetry(req.VolumeId, pub)
	ctx, span := tel.Start(ctx, "NodePublishVolume", attribute.String("component", "csi"))
	var perr error
	defer func() {
		if perr != nil {
			tel.LogError(ctx, "NodePublishVolume failed", perr)
		}
		telemetry.Finish(span, perr)
	}()
	done, err := p.reservePublish(req.VolumeId)
	if err != nil || done {
		perr = err
		if err != nil {
			return nil, err
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}
	vol, err := p.publishOverlay(ctx, req, pub, tel)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		perr = err
		delete(p.vols, req.VolumeId)
		p.dropCtlOwner(req.VolumeId)
		return nil, err
	}
	p.vols[req.VolumeId] = vol
	return &csi.NodePublishVolumeResponse{}, nil
}

// reservePublish marks the volume as being published. It reports done when the volume is already served, and fails
// with Aborted when another call works on it.
func (p *NodePlugin) reservePublish(id string) (done bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.vols[id]; ok {
		if !cur.ready {
			return false, status.Error(codes.Aborted, "the volume is still being published")
		}
		return true, nil
	}
	p.vols[id] = &volume{}
	return false, nil
}

// claimCtlDir records that the volume serves its workspace socket in dir. A second volume of the same workspace in
// the same pod would rebind the socket of the first, so it fails.
func (p *NodePlugin) claimCtlDir(dir, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if owner, ok := p.ctlOwners[dir]; ok && owner != id {
		return status.Errorf(codes.FailedPrecondition, "the volume %s already serves the control socket in %s", owner, dir)
	}
	if p.ctlOwners == nil {
		p.ctlOwners = map[string]string{}
	}
	p.ctlOwners[dir] = id
	return nil
}

func (p *NodePlugin) dropCtlOwner(id string) {
	for dir, owner := range p.ctlOwners {
		if owner == id {
			delete(p.ctlOwners, dir)
		}
	}
}

// publishCtl binds the control socket directory of the workspace at the target path. The workspace volume creates the
// socket in it, in whichever order the kubelet publishes the two.
func (p *NodePlugin) publishCtl(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	dir, err := p.ctlDir(req.VolumeContext, req.VolumeContext["workspace"])
	if err != nil {
		return nil, err
	}
	done, err := p.reservePublish(req.VolumeId)
	if err != nil {
		return nil, err
	}
	if done {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	fail := func(err error) (*csi.NodePublishVolumeResponse, error) {
		p.mu.Lock()
		delete(p.vols, req.VolumeId)
		p.mu.Unlock()
		return nil, err
	}
	if err := os.MkdirAll(req.TargetPath, 0o700); err != nil {
		return fail(status.Error(codes.Internal, err.Error()))
	}
	if err := syscall.Mount(dir, req.TargetPath, "", syscall.MS_BIND, ""); err != nil {
		return fail(status.Errorf(codes.Internal, "bind %s: %v", dir, err))
	}
	p.mu.Lock()
	p.vols[req.VolumeId] = &volume{ready: true, tel: telemetry.Telemetry{Logger: p.Logger, Corr: telemetry.Correlation{MountID: req.VolumeId, Mode: "ctl", Node: p.NodeID}}}
	p.mu.Unlock()
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the workspace and deletes its volume directory. A volume this process does not know,
// for example after a restart, still loses its mount and its volume directory. It holds no lock of the plugin while it
// unmounts, scans mountinfo or deletes, so a slow volume blocks only itself.
func (p *NodePlugin) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	tel := telemetry.Telemetry{Logger: p.Logger, Corr: telemetry.Correlation{MountID: req.VolumeId, Mode: "mount", Node: p.NodeID}}
	p.mu.Lock()
	cur, known := p.vols[req.VolumeId]
	if known && !cur.ready {
		p.mu.Unlock()
		return nil, status.Error(codes.Aborted, "the volume is still being published")
	}
	if known {
		tel = cur.tel
		cur.ready = false
	} else {
		p.vols[req.VolumeId] = &volume{}
	}
	p.mu.Unlock()
	ctx, span := tel.Start(ctx, "NodeUnpublishVolume", attribute.String("component", "csi"))
	resp, err := p.unpublish(ctx, req, cur, tel)
	if err != nil {
		tel.LogError(ctx, "NodeUnpublishVolume failed", err)
	}
	telemetry.Finish(span, err)
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case err == nil:
		delete(p.vols, req.VolumeId)
		p.dropCtlOwner(req.VolumeId)
	case known:
		cur.ready = true
	default:
		delete(p.vols, req.VolumeId)
	}
	return resp, err
}

func (p *NodePlugin) unpublish(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, vol *volume, tel telemetry.Telemetry) (*csi.NodeUnpublishVolumeResponse, error) {
	if vol != nil && vol.lay != nil || vol == nil && p.isOverlayVolume(req.VolumeId) {
		return p.unpublishOverlay(ctx, req, vol, tel)
	}
	// A control socket volume, or the state of a volume that this process does not serve.
	if err := syscall.Unmount(req.TargetPath, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		return nil, status.Errorf(codes.Internal, "unmount: %v", err)
	}
	if err := p.removeCtlDir(p.volDir(req.VolumeId)); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(p.volDir(req.VolumeId)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
