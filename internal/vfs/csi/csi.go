//go:build linux

package csi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	Grant  func(volattr.Publication) string
	UID    uint32
	GID    uint32
	HTTP   *http.Client
	Clock  clock.Clock
	// Logger receives the vfs.stats, vfs.health and error records. SlowOp and StatsEvery tune them; zero is the default.
	Logger     *slog.Logger
	SlowOp     time.Duration
	StatsEvery time.Duration

	mnt  mounter
	mu   sync.Mutex
	vols map[string]*volume
}

type volume struct {
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

// SeedCacheSize adds the size of the blob cache that an earlier process left in dir to the tier-2 figure of vfs.health.
func (p *NodePlugin) SeedCacheSize(dir string) {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	store.SeedTier2Bytes(n)
}

func (p *NodePlugin) volDir(id string) string { return filepath.Join(p.StateDir, "vol", id) }

const podUIDFile = "pod-uid"

// ctlDir is the control socket directory of a pod. The kubelet names the pod, so a pod cannot name the
// directory of another one.
func (p *NodePlugin) ctlDir(attrs map[string]string) (string, error) {
	uid := attrs[volattr.PodUIDAttr]
	if uid == "" || strings.Trim(uid, "0123456789abcdefABCDEF-") != "" {
		return "", status.Errorf(codes.InvalidArgument, "the publish request names no pod UID (%s); the CSIDriver needs podInfoOnMount", volattr.PodUIDAttr)
	}
	dir := filepath.Join(p.StateDir, "ctl", uid)
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

// Serve answers CSI calls on the unix socket until ctx ends.
func (p *NodePlugin) Serve(ctx context.Context, socket string) error {
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	p.vols = map[string]*volume{}
	s := grpc.NewServer()
	csi.RegisterIdentityServer(s, p)
	csi.RegisterNodeServer(s, p)
	go func() { <-ctx.Done(); s.Stop() }()
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
// the control socket directory of the pod for a control socket volume.
func (p *NodePlugin) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if volattr.IsCtl(req.VolumeContext) {
		return p.publishCtl(req)
	}
	pub, err := volattr.ParsePublication(req.VolumeContext)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.VolumeId == "" || req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
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
	p.mu.Lock()
	if cur, ok := p.vols[req.VolumeId]; ok {
		p.mu.Unlock()
		if !cur.ready {
			return nil, status.Error(codes.Aborted, "the volume is still being published")
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}
	p.vols[req.VolumeId] = &volume{}
	p.mu.Unlock()
	vol, err := p.publishOverlay(ctx, req, pub, tel)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		perr = err
		delete(p.vols, req.VolumeId)
		return nil, err
	}
	p.vols[req.VolumeId] = vol
	return &csi.NodePublishVolumeResponse{}, nil
}

// publishCtl binds the control socket directory of the pod at the target path. The workspace volume of the pod
// creates the socket in it, in whichever order the kubelet publishes the two.
func (p *NodePlugin) publishCtl(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.VolumeId == "" || req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	dir, err := p.ctlDir(req.VolumeContext)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.vols[req.VolumeId]; ok {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := os.MkdirAll(req.TargetPath, 0o700); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := syscall.Mount(dir, req.TargetPath, "", syscall.MS_BIND, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "bind %s: %v", dir, err)
	}
	p.vols[req.VolumeId] = &volume{ready: true, tel: telemetry.Telemetry{Logger: p.Logger, Corr: telemetry.Correlation{MountID: req.VolumeId, Mode: "ctl", Node: p.NodeID}}}
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the workspace and deletes its volume directory. A volume this process does not know,
// for example after a restart, still loses its mount and its volume directory.
func (p *NodePlugin) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tel := telemetry.Telemetry{Logger: p.Logger, Corr: telemetry.Correlation{MountID: req.VolumeId, Mode: "mount", Node: p.NodeID}}
	if cur, ok := p.vols[req.VolumeId]; ok {
		tel = cur.tel
	}
	ctx, span := tel.Start(ctx, "NodeUnpublishVolume", attribute.String("component", "csi"))
	resp, err := p.unpublish(ctx, req, tel)
	if err != nil {
		tel.LogError(ctx, "NodeUnpublishVolume failed", err)
	}
	telemetry.Finish(span, err)
	return resp, err
}

func (p *NodePlugin) unpublish(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, tel telemetry.Telemetry) (*csi.NodeUnpublishVolumeResponse, error) {
	vol, ok := p.vols[req.VolumeId]
	if ok && !vol.ready {
		return nil, status.Error(codes.Aborted, "the volume is still being published")
	}
	if ok && vol.lay != nil || !ok && p.isOverlayVolume(req.VolumeId) {
		return p.unpublishOverlay(ctx, req, vol, tel)
	}
	// A control socket volume, or the state of a volume that this process does not serve.
	if err := syscall.Unmount(req.TargetPath, syscall.MNT_DETACH); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		return nil, status.Errorf(codes.Internal, "unmount: %v", err)
	}
	delete(p.vols, req.VolumeId)
	if uid, err := os.ReadFile(filepath.Join(p.volDir(req.VolumeId), podUIDFile)); err == nil {
		if err := os.RemoveAll(filepath.Join(p.StateDir, "ctl", string(uid))); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if err := os.RemoveAll(p.volDir(req.VolumeId)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
