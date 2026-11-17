// Package volume is the contract between a caller's sandbox driver and the node plugin: the volume attributes of a workspace.
package volume

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/majorcontext/motte/internal/gitpack"
)

// GrantEnv names the environment variable that carries a bearer grant to motte-vfsd.
const GrantEnv = "MOTTE_VFS_GRANT"

// GrantRefused is the text of the error that the node plugin returns when the store refuses a node grant.
const GrantRefused = "the store refused the node grant"

// The workspace sees the control socket of its mount at CtlDir/CtlSocket, bound by a volume with CtlAttributes.
const (
	CtlDir     = "/run/motte-vfs"
	CtlSocket  = "ctl.sock"
	SockEnv    = "MOTTE_VFS_SOCK"
	PodUIDAttr = "csi.storage.k8s.io/pod.uid"
	// PodNamespaceAttr is the namespace of the pod. The kubelet sets it when the CSIDriver has podInfoOnMount.
	PodNamespaceAttr = "csi.storage.k8s.io/pod.namespace"
	kindAttr         = "volume"
	kindCtl          = "ctl"
)

// CtlAttributes returns the attributes of the volume that binds the control socket of one workspace into a pod. A pod
// with two workspace volumes has two control socket volumes.
func CtlAttributes(workspace string) map[string]string {
	return map[string]string{kindAttr: kindCtl, "workspace": workspace}
}

// IsCtl reports whether attrs are the attributes of a control socket volume.
func IsCtl(attrs map[string]string) bool { return attrs[kindAttr] == kindCtl }

var workspaceID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidWorkspace reports whether id can name a workspace. The node plugin uses it as a directory name.
func ValidWorkspace(id string) bool { return workspaceID.MatchString(id) }

// Publication is what the caller tells the node plugin about one workspace volume. It travels as the
// volume attributes of the inline CSI volume of the pod.
type Publication struct {
	Workspace string
	Epoch     uint64
	Repo      string // "<prefix>:<path>"
	Commit    string
	// Runtime names the sandbox runtime of the node; it only labels telemetry.
	Runtime string
}

// Attributes returns the CSI volume attributes of p.
func (p Publication) Attributes() map[string]string {
	return map[string]string{
		"workspace": p.Workspace, "epoch": strconv.FormatUint(p.Epoch, 10), "repo": p.Repo, "commit": p.Commit, "runtime": p.Runtime,
	}
}

// ParsePublication reads the volume attributes of a publish request.
func ParsePublication(attrs map[string]string) (Publication, error) {
	epoch, err := strconv.ParseUint(attrs["epoch"], 10, 64)
	if err != nil {
		return Publication{}, fmt.Errorf("vfs: epoch attribute: %w", err)
	}
	p := Publication{Workspace: attrs["workspace"], Epoch: epoch, Repo: attrs["repo"], Commit: attrs["commit"], Runtime: attrs["runtime"]}
	if !ValidWorkspace(p.Workspace) || p.Repo == "" {
		return Publication{}, fmt.Errorf("vfs: the volume attributes name no valid workspace or no repo")
	}
	if _, err := gitpack.ParseSHA(p.Commit); err != nil {
		return Publication{}, fmt.Errorf("vfs: commit attribute: %w", err)
	}
	return p, nil
}
