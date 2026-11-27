//go:build linux

package csi

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/majorcontext/motte/internal/vfs/lower"
	volattr "github.com/majorcontext/motte/volume"
)

const (
	testCommit = "0123456789abcdef0123456789abcdef01234567"
	otherSHA   = "fedcba9876543210fedcba9876543210fedcba98"
)

// fakeMnt records every privileged step of the plugin and keeps a mountinfo that follows the mounts it made.
type fakeMnt struct {
	mu         sync.Mutex
	gate       func(path string)
	t          *testing.T
	state, tgt string
	calls      []string
	ents       []mountEntry
	fail       map[string]error
	cfg        lower.Config
	baseAtMnt  string
}

func (f *fakeMnt) rec(format string, a ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := strings.NewReplacer(f.tgt, "T", f.state, "S").Replace(fmt.Sprintf(format, a...))
	f.calls = append(f.calls, s)
	if err, ok := f.fail[s]; ok {
		return err
	}
	return f.fail[strings.Fields(s)[0]]
}

func (f *fakeMnt) probe(string) error { return f.rec("probe") }
func (f *fakeMnt) mounts() ([]mountEntry, error) {
	err := f.rec("mounts")
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ents), err
}
func (f *fakeMnt) openLower(_ context.Context, cfg lower.Config) (lowerLayer, error) {
	f.cfg = cfg
	return fakeLower{f}, f.rec("openLower")
}
func (f *fakeMnt) mountOverlay(target, opts string) error {
	b, _ := os.ReadFile(filepath.Join(filepath.Dir(f.cfg.UpperDir), "base"))
	f.baseAtMnt = string(b)
	err := f.rec("mountOverlay %s %s", target, opts)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		f.ents = append(f.ents, mountEntry{Mountpoint: target, FSType: "overlay", Opts: map[string]string{"upperdir": f.cfg.UpperDir}})
	}
	return err
}
func (f *fakeMnt) unmount(path string, flags int) error {
	if f.gate != nil {
		f.gate(path)
	}
	if err := f.rec("unmount %s %d", path, flags); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.ents)
	f.ents = slices.DeleteFunc(f.ents, func(e mountEntry) bool { return e.Mountpoint == path })
	if len(f.ents) == n {
		return syscall.EINVAL
	}
	return nil
}

type fakeLower struct{ *fakeMnt }

func (l fakeLower) mount(_ context.Context, dir string) error {
	err := l.rec("lower.mount")
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil {
		l.ents = append(l.ents, mountEntry{Mountpoint: dir, FSType: "fuse.motte-vfs"})
	}
	return err
}

func (l fakeLower) serve(net.Listener, func(error)) func() {
	_ = l.rec("lower.serve")
	return func() { _ = l.rec("lower.stop") }
}

func (l fakeLower) unmount() error {
	if err := l.rec("lower.unmount"); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ents = slices.DeleteFunc(l.ents, func(e mountEntry) bool { return e.FSType == "fuse.motte-vfs" })
	return nil
}

func (l fakeLower) close() { _ = l.rec("lower.close") }

func newOverlayPlugin(t *testing.T) (*NodePlugin, *fakeMnt) {
	t.Helper()
	state, err := os.MkdirTemp("", "ov")
	must(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(state) })
	f := &fakeMnt{t: t, state: state, tgt: filepath.Join(state, "target"), fail: map[string]error{}}
	p := &NodePlugin{StateDir: state, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), mnt: f,
		Grant: func(volattr.Publication) (string, error) { return "", nil }, vols: map[string]*volume{}, AllowedNamespaces: []string{"ws"}}
	return p, f
}

func (f *fakeMnt) volDir() string { return filepath.Join(f.state, "vol", "v1") }

func publish(p *NodePlugin, f *fakeMnt, commit string) error {
	return publishVol(p, "v1", "b", f.tgt, commit, "ws")
}

func publishVol(p *NodePlugin, id, workspace, target, commit, namespace string) error {
	attrs := volattr.Publication{Workspace: workspace, Epoch: 1, Repo: "gh:o/r", Commit: commit}.Attributes()
	attrs[volattr.PodUIDAttr] = "abc-123"
	if namespace != "" {
		attrs[volattr.PodNamespaceAttr] = namespace
	}
	_, err := p.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{VolumeId: id, TargetPath: target, VolumeContext: attrs})
	return err
}

func unpublish(p *NodePlugin, f *fakeMnt) error {
	_, err := p.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "v1", TargetPath: f.tgt})
	return err
}

func wantCalls(t *testing.T, f *fakeMnt, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls:\n got %q\nwant %q", f.calls, want)
	}
	f.calls = nil
}

func wantCode(t *testing.T, err error, c codes.Code) {
	t.Helper()
	if status.Code(err) != c {
		t.Fatalf("code = %v (%v), want %v", status.Code(err), err, c)
	}
}

func exists(path string) bool { _, err := os.Lstat(path); return err == nil }

func TestOverlayPublishAndUnpublishOrder(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	if err := publish(p, f, testCommit); err != nil {
		t.Fatal(err)
	}
	opts := "lowerdir=S/vol/v1/lower,upperdir=S/vol/v1/upper,workdir=S/vol/v1/work,redirect_dir=on,metacopy=on,index=off,xino=off,nfs_export=off"
	wantCalls(t, f, "mounts", "probe", "openLower", "lower.mount", "lower.serve", "mountOverlay T "+opts)
	if f.baseAtMnt != testCommit || f.cfg.UpperDir != filepath.Join(f.volDir(), "upper") {
		t.Fatalf("base at overlay mount = %q, upper = %q", f.baseAtMnt, f.cfg.UpperDir)
	}
	if err := publish(p, f, testCommit); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, f)
	if err := unpublish(p, f); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, f, "unmount T 0", "mounts", "lower.unmount", "lower.stop", "lower.close")
	if exists(f.volDir()) || exists(p.ctlPath("abc-123", "b")) {
		t.Fatal("unpublish left the volume or the control directory")
	}
}

func TestOverlayPublishFailureUndoesInReverse(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		fail  string
		code  codes.Code
		undo  []string
		edits bool
	}{
		{"probe", codes.FailedPrecondition, nil, false},
		{"openLower", codes.Internal, nil, false},
		{"lower.mount", codes.Internal, []string{"lower.close"}, false},
		{"mountOverlay", codes.Internal, []string{"lower.unmount", "lower.stop", "lower.close"}, false},
		{"mountOverlay", codes.Unavailable, []string{"lower.unmount", "lower.stop", "lower.close"}, true},
	} {
		t.Run(fmt.Sprint(c.fail, c.edits), func(t *testing.T) {
			p, f := newOverlayPlugin(t)
			if c.edits {
				must(t, os.MkdirAll(filepath.Join(f.volDir(), "upper"), 0o755))
				must(t, os.WriteFile(filepath.Join(f.volDir(), "upper", "edit"), []byte("x"), 0o644))
			}
			f.fail[c.fail] = errors.New("boom")
			wantCode(t, publish(p, f, testCommit), c.code)
			var undo []string
			for _, s := range f.calls {
				if strings.HasPrefix(s, "lower.") && s != "lower.mount" && s != "lower.serve" {
					undo = append(undo, s)
				}
			}
			if !reflect.DeepEqual(undo, c.undo) {
				t.Fatalf("undo = %q, want %q", undo, c.undo)
			}
			if kept := exists(f.volDir()); kept != c.edits || !exists(p.ctlPath("abc-123", "b")) {
				t.Fatalf("volume kept = %v, want %v, or the control directory went", kept, c.edits)
			}
		})
	}
}

func TestOverlayPublishAfterRestartTakesTheStalePath(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	f.ents = []mountEntry{{Mountpoint: f.tgt, FSType: "overlay", Opts: map[string]string{"upperdir": filepath.Join(f.volDir(), "upper")}}}
	must(t, publish(p, f, testCommit))
	if got := f.calls[:5]; !reflect.DeepEqual(got, []string{"mounts", "probe", "unmount T 0", "mounts", "openLower"}) {
		t.Fatalf("calls = %q", f.calls)
	}
}

func TestOverlayPublishStale(t *testing.T) {
	t.Parallel()
	upper := func(f *fakeMnt) string { return filepath.Join(f.volDir(), "upper") }
	seed := func(t *testing.T, f *fakeMnt) {
		must(t, os.MkdirAll(filepath.Join(upper(f), "d"), 0o755))
		must(t, os.MkdirAll(filepath.Join(f.volDir(), "work", "w"), 0o755))
		must(t, os.WriteFile(filepath.Join(f.volDir(), "base"), []byte(testCommit), 0o600))
	}
	t.Run("held by another namespace", func(t *testing.T) {
		p, f := newOverlayPlugin(t)
		seed(t, f)
		f.ents = []mountEntry{{Mountpoint: "/workspace", FSType: "overlay", Opts: map[string]string{"upperdir": upper(f)}}}
		wantCode(t, publish(p, f, testCommit), codes.Unavailable)
		wantCalls(t, f, "mounts", "probe")
		if !exists(filepath.Join(upper(f), "d")) {
			t.Fatal("the upper directory was touched")
		}
	})
	t.Run("a private copy keeps the lower untouched", func(t *testing.T) {
		p, f := newOverlayPlugin(t)
		seed(t, f)
		o := map[string]string{"upperdir": upper(f)}
		f.ents = []mountEntry{{Mountpoint: f.tgt, FSType: "overlay", Opts: o}, {Mountpoint: "/workspace", FSType: "overlay", Opts: o}, {Mountpoint: filepath.Join(f.volDir(), "lower")}}
		wantCode(t, publish(p, f, testCommit), codes.Unavailable)
		wantCalls(t, f, "mounts", "probe", "unmount T 0", "mounts")
	})
	t.Run("dead lower at the target is unmounted overlay first", func(t *testing.T) {
		p, f := newOverlayPlugin(t)
		seed(t, f)
		f.ents = []mountEntry{
			{Mountpoint: f.tgt, FSType: "overlay", Opts: map[string]string{"upperdir": upper(f)}},
			{Mountpoint: filepath.Join(f.volDir(), "lower"), FSType: "fuse.motte-vfs"},
		}
		must(t, publish(p, f, testCommit))
		if got := f.calls[:5]; !reflect.DeepEqual(got, []string{"mounts", "probe", "unmount T 0", "mounts", "unmount S/vol/v1/lower 0"}) {
			t.Fatalf("calls = %q", f.calls)
		}
		if !exists(filepath.Join(upper(f), "d")) || exists(filepath.Join(f.volDir(), "work", "w")) {
			t.Fatal("upper must survive and work must be recreated")
		}
	})
	t.Run("busy target keeps everything", func(t *testing.T) {
		p, f := newOverlayPlugin(t)
		seed(t, f)
		f.ents = []mountEntry{{Mountpoint: f.tgt, FSType: "overlay", Opts: map[string]string{"upperdir": upper(f)}}, {Mountpoint: filepath.Join(f.volDir(), "lower")}}
		f.fail["unmount T 0"] = syscall.EBUSY
		wantCode(t, publish(p, f, testCommit), codes.Unavailable)
		wantCalls(t, f, "mounts", "probe", "unmount T 0")
		if !exists(filepath.Join(upper(f), "d")) || !exists(filepath.Join(f.volDir(), "work", "w")) {
			t.Fatal("a busy unmount touched upper or work")
		}
	})
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOverlayBaseFileDecidesTheUpper(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, base string
		keep       bool
		legacy     bool
	}{
		{"same commit keeps upper", testCommit, true, false},
		{"other commit discards upper", otherSHA, false, false},
		{"missing base discards upper", "", false, false},
		{"legacy overlay directory is dropped", testCommit, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, f := newOverlayPlugin(t)
			must(t, os.MkdirAll(filepath.Join(f.volDir(), "upper"), 0o755))
			must(t, os.WriteFile(filepath.Join(f.volDir(), "upper", "edit"), []byte("x"), 0o644))
			if c.base != "" {
				must(t, os.WriteFile(filepath.Join(f.volDir(), "base"), []byte(c.base), 0o600))
			}
			if c.legacy {
				must(t, os.MkdirAll(filepath.Join(f.volDir(), "overlay"), 0o755))
			}
			must(t, publish(p, f, testCommit))
			if got := exists(filepath.Join(f.volDir(), "upper", "edit")); got != c.keep {
				t.Fatalf("upper kept = %v, want %v", got, c.keep)
			}
			if b, _ := os.ReadFile(filepath.Join(f.volDir(), "base")); string(b) != testCommit || exists(filepath.Join(f.volDir(), "overlay")) {
				t.Fatalf("base = %q", b)
			}
		})
	}
}

func TestOverlayUnpublishOfUnknownVolume(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T) (*NodePlugin, *fakeMnt) {
		p, f := newOverlayPlugin(t)
		must(t, os.MkdirAll(filepath.Join(f.volDir(), "upper"), 0o755))
		must(t, os.WriteFile(filepath.Join(f.volDir(), "base"), []byte(testCommit), 0o600))
		f.ents = []mountEntry{{Mountpoint: f.tgt, FSType: "overlay", Opts: map[string]string{"upperdir": filepath.Join(f.volDir(), "upper")}}, {Mountpoint: filepath.Join(f.volDir(), "lower")}}
		return p, f
	}
	p, f := setup(t)
	must(t, unpublish(p, f))
	wantCalls(t, f, "unmount T 0", "mounts", "unmount S/vol/v1/lower 0")
	if exists(f.volDir()) {
		t.Fatal("the volume directory stayed")
	}
	p, f = setup(t)
	f.fail["unmount T 0"] = syscall.EBUSY
	wantCode(t, unpublish(p, f), codes.Unavailable)
	wantCalls(t, f, "unmount T 0")
	if !exists(filepath.Join(f.volDir(), "upper")) {
		t.Fatal("upper was removed before the overlay unmounted")
	}
	p, f = setup(t)
	f.ents = append(f.ents, mountEntry{Mountpoint: "/workspace", FSType: "overlay", Opts: map[string]string{"upperdir": filepath.Join(f.volDir(), "upper")}})
	wantCode(t, unpublish(p, f), codes.Unavailable)
	if !exists(filepath.Join(f.volDir(), "upper")) {
		t.Fatal("upper was removed while a namespace references it")
	}
}

func TestParseMountinfo(t *testing.T) {
	t.Parallel()
	text := `22 28 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
701 650 0:80 / /var/lib/kubelet/pods/u/volumes/kubernetes.io~csi/w/mount rw,relatime shared:1 master:2 - overlay overlay rw,lowerdir=/s/vol/a\040b/lower,upperdir=/s/vol/a\040b/upper,workdir=/s/vol/a\040b/work,redirect_dir=on,metacopy=on,index=off
702 650 0:81 / /s/vol/a\040b/lower ro,nosuid,nodev - fuse.motte-vfs motte-vfs ro,user_id=0,group_id=0,allow_other
`
	es, err := parseMountinfo(text)
	if err != nil || len(es) != 3 {
		t.Fatalf("entries = %d, err = %v", len(es), err)
	}
	if es[1].FSType != "overlay" || es[1].upperdir() != "/s/vol/a b/upper" || es[1].Opts["redirect_dir"] != "on" || es[1].Mountpoint != "/var/lib/kubelet/pods/u/volumes/kubernetes.io~csi/w/mount" {
		t.Fatalf("overlay entry = %+v", es[1])
	}
	if es[2].Mountpoint != "/s/vol/a b/lower" || es[2].FSType != "fuse.motte-vfs" || es[0].FSType != "sysfs" {
		t.Fatalf("entries = %+v", es)
	}
	if _, err := parseMountinfo("1 2 3:4 / /x rw"); err == nil {
		t.Fatal("a line without a separator parsed")
	}
}

func TestOverlayUnpublishKeepsTheServedLowerWhileReferenced(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	must(t, publish(p, f, testCommit))
	f.calls = nil
	ref := mountEntry{Mountpoint: "/workspace", FSType: "overlay", Opts: map[string]string{"upperdir": filepath.Join(f.volDir(), "upper")}, PID: 77, NS: "mnt:[9]"}
	f.ents = append(f.ents, ref)
	err := unpublish(p, f)
	wantCode(t, err, codes.Unavailable)
	if !strings.Contains(err.Error(), "pid 77") || !strings.Contains(err.Error(), "mnt:[9]") {
		t.Fatalf("the error does not name the holder: %v", err)
	}
	wantCalls(t, f, "unmount T 0", "mounts")
	if p.Mounts() != 1 || !exists(f.volDir()) {
		t.Fatal("the entry or the volume went while a reference remained")
	}
	f.ents = f.ents[:len(f.ents)-1]
	must(t, unpublish(p, f))
	if p.Mounts() != 0 || exists(f.volDir()) {
		t.Fatal("the retry left the volume")
	}
}

func TestCheckHostPID(t *testing.T) {
	t.Parallel()
	ns := func(m map[string]string) func(string) (string, error) {
		return func(p string) (string, error) { return m[p], nil }
	}
	host := map[string]string{"/proc/1/ns/pid": "pid:[1]", "/proc/self/ns/pid": "pid:[1]", "/proc/1/ns/mnt": "mnt:[1]", "/proc/self/ns/mnt": "mnt:[2]"}
	if err := CheckHostPID(ns(host)); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{"/proc/self/ns/pid": "pid:[9]", "/proc/self/ns/mnt": "mnt:[1]", "/proc/1/ns/pid": ""} {
		m := maps.Clone(host)
		m[k] = v
		if err := CheckHostPID(ns(m)); err == nil || !strings.Contains(err.Error(), "hostPID") {
			t.Fatalf("%s=%q: err = %v", k, v, err)
		}
	}
}

func TestOverlayPublishFailureKeepsAVolumeThatAMountReferences(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	f.fail["mountOverlay"] = errors.New("boom")
	f.fail["unmount S/vol/v1/lower 2"] = syscall.EBUSY
	f.fail["lower.unmount"] = syscall.EBUSY
	wantCode(t, publish(p, f, testCommit), codes.Unavailable)
	if !exists(f.volDir()) {
		t.Fatal("a volume with a mounted lower was removed")
	}
}

func TestOverlayUnpublishNeverWalksIntoTheLowerMountPoint(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	must(t, os.MkdirAll(filepath.Join(f.volDir(), "upper"), 0o755))
	must(t, os.MkdirAll(filepath.Join(f.volDir(), "lower"), 0o755))
	must(t, os.WriteFile(filepath.Join(f.volDir(), "lower", "x"), []byte("x"), 0o644))
	must(t, os.WriteFile(filepath.Join(f.volDir(), "base"), []byte(testCommit), 0o600))
	must(t, unpublish(p, f))
	if !exists(filepath.Join(f.volDir(), "lower", "x")) || exists(filepath.Join(f.volDir(), "upper")) {
		t.Fatal("unpublish must remove upper and leave the content of the lower mount point")
	}
}

func TestHostMountsFailsClosed(t *testing.T) {
	t.Parallel()
	const ovl = "5 1 0:9 / /w rw - overlay overlay rw,upperdir=/u\n"
	proc := t.TempDir()
	write := func(pid, ns, mountinfo string) {
		dir := filepath.Join(proc, pid)
		must(t, os.MkdirAll(filepath.Join(dir, "ns"), 0o755))
		must(t, os.Symlink(ns, filepath.Join(dir, "ns", "mnt")))
		if mountinfo != "" {
			must(t, os.WriteFile(filepath.Join(dir, "mountinfo"), []byte(mountinfo), 0o644))
		}
	}
	write("self", "mnt:[1]", "1 0 0:1 / / rw - ext4 /dev/x rw\n")
	write("10", "mnt:[2]", "")
	write("11", "mnt:[2]", ovl)
	es, err := hostMounts(proc)
	if err != nil || len(es) != 2 || es[1].upperdir() != "/u" || es[1].PID != 11 || es[1].NS != "mnt:[2]" {
		t.Fatalf("entries = %+v, err = %v", es, err)
	}
	must(t, os.Remove(filepath.Join(proc, "11", "mountinfo")))
	if es, err = hostMounts(proc); err != nil || len(es) != 1 {
		t.Fatalf("a namespace whose processes all exited is empty: %+v, %v", es, err)
	}
	must(t, os.WriteFile(filepath.Join(proc, "11", "mountinfo"), []byte("garbage"), 0o644))
	if _, err = hostMounts(proc); err == nil {
		t.Fatal("a mountinfo that does not parse must fail the scan")
	}
	must(t, os.Remove(filepath.Join(proc, "11", "mountinfo")))
	must(t, os.Mkdir(filepath.Join(proc, "11", "mountinfo"), 0o755))
	if _, err = hostMounts(proc); err == nil {
		t.Fatal("an unreadable mountinfo must fail the scan")
	}
}

// TestPublishRefusesAPodOutsideTheAllowedNamespaces: the volume attributes come from the pod spec, so the namespace
// of the pod decides who may name a workspace. A pod of another namespace, or a request without pod info, mounts
// nothing, and an empty list allows no pod.
func TestPublishRefusesAPodOutsideTheAllowedNamespaces(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	wantCode(t, publishVol(p, "v1", "b", f.tgt, testCommit, "other"), codes.PermissionDenied)
	wantCode(t, publishVol(p, "v1", "b", f.tgt, testCommit, ""), codes.InvalidArgument)
	ctl := volattr.CtlAttributes("b")
	ctl[volattr.PodUIDAttr], ctl[volattr.PodNamespaceAttr] = "abc-123", "other"
	_, err := p.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{VolumeId: "c1", TargetPath: f.tgt, VolumeContext: ctl})
	wantCode(t, err, codes.PermissionDenied)
	p.AllowedNamespaces = nil
	wantCode(t, publishVol(p, "v1", "b", f.tgt, testCommit, "ws"), codes.PermissionDenied)
	wantCalls(t, f)
	p.AllowedNamespaces = []string{"ws"}
	must(t, publishVol(p, "v1", "b", f.tgt, testCommit, "ws"))
}

// TestWorkspaceVolumesOfOnePodKeepSeparateControlDirectories: two workspace volumes in one pod must not share a
// socket, and unpublishing one must not delete the socket of the other.
func TestWorkspaceVolumesOfOnePodKeepSeparateControlDirectories(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	t1, t2 := filepath.Join(f.state, "t1"), filepath.Join(f.state, "t2")
	must(t, publishVol(p, "v1", "b", t1, testCommit, "ws"))
	must(t, publishVol(p, "v2", "c", t2, testCommit, "ws"))
	wantCode(t, publishVol(p, "v3", "b", filepath.Join(f.state, "t3"), testCommit, "ws"), codes.FailedPrecondition)
	ctlB, ctlC := p.ctlPath("abc-123", "b"), p.ctlPath("abc-123", "c")
	if !exists(ctlB) || !exists(ctlC) {
		t.Fatal("a workspace volume has no control directory of its own")
	}
	_, err := p.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "v1", TargetPath: t1})
	must(t, err)
	if exists(ctlB) || !exists(ctlC) {
		t.Fatalf("after the unpublish of v1: control directory of b exists = %v, of c = %v", exists(ctlB), exists(ctlC))
	}
}

// TestUnpublishOfOneVolumeDoesNotBlockAnother: an unmount that hangs holds only its own volume. A failure hangs here until the test deadline.
func TestUnpublishOfOneVolumeDoesNotBlockAnother(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	t1, t2 := filepath.Join(f.state, "t1"), filepath.Join(f.state, "t2")
	must(t, publishVol(p, "v1", "b", t1, testCommit, "ws"))
	must(t, publishVol(p, "v2", "c", t2, testCommit, "ws"))
	entered, release := make(chan struct{}), make(chan struct{})
	f.gate = func(path string) {
		if path == t2 {
			close(entered)
			<-release
		}
	}
	slow := make(chan error, 1)
	go func() {
		_, err := p.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "v2", TargetPath: t2})
		slow <- err
	}()
	<-entered
	fast := make(chan error, 1)
	go func() {
		_, err := p.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "v1", TargetPath: t1})
		fast <- err
	}()
	must(t, <-fast)
	close(release)
	must(t, <-slow)
}

// TestControlSocketPathStaysUnderTheLimit: a unix socket path holds at most 107 bytes, and a 36-byte pod UID with a
// 64-byte workspace would pass it under any layout that names them. A state directory that is too long fails the
// publish with a message that names the path.
func TestControlSocketPathStaysUnderTheLimit(t *testing.T) {
	t.Parallel()
	p, f := newOverlayPlugin(t)
	uid, ws := "01234567-89ab-cdef-0123-456789abcdef", strings.Repeat("w", 64)
	pub := func(id string) error {
		attrs := volattr.Publication{Workspace: ws, Epoch: 1, Repo: "gh:o/r", Commit: testCommit}.Attributes()
		attrs[volattr.PodUIDAttr], attrs[volattr.PodNamespaceAttr] = uid, "ws"
		_, err := p.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{VolumeId: id, TargetPath: f.tgt, VolumeContext: attrs})
		return err
	}
	must(t, pub("v1"))
	long := filepath.Join(f.state, strings.Repeat("d", 100))
	must(t, os.MkdirAll(long, 0o755))
	p.StateDir = long
	err := pub("v2")
	wantCode(t, err, codes.InvalidArgument)
	if !strings.Contains(err.Error(), "socket path") {
		t.Errorf("err = %v, want it to name the socket path", err)
	}
}

func TestServeAnswersWhileTheCacheSweepRuns(t *testing.T) {
	t.Parallel()
	p, _ := newOverlayPlugin(t)
	started, release := make(chan struct{}), make(chan struct{})
	p.SweepCache = func(string) { close(started); <-release }
	sock := filepath.Join(p.StateDir, "s.sock")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Serve(ctx, sock) }()
	<-started
	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(t, err)
	defer conn.Close()
	if _, err := csi.NewIdentityClient(conn).Probe(ctx, &csi.ProbeRequest{}); err != nil {
		t.Fatal(err)
	}
	close(release)
	cancel()
	<-done
}
