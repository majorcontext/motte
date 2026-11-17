//go:build linux

// Command motte-vfsd is the node daemon of the virtual workspace. "mount" serves one commit of the
// version control store as the read-only FUSE lower layer of a kernel overlay; "materialize" writes the checkout of a commit to a
// directory; "import" uploads a repository from a remote to the store with an import grant; "csi" serves the
// CSI node plugin that mounts a workspace for each workspace pod of a node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/csi"
	"github.com/majorcontext/motte/internal/vfs/ctl"
	"github.com/majorcontext/motte/internal/vfs/importer"
	"github.com/majorcontext/motte/internal/vfs/lower"
	"github.com/majorcontext/motte/internal/vfs/materialize"
	"github.com/majorcontext/motte/internal/vfs/nodegrant"
	"github.com/majorcontext/motte/internal/vfs/store"
	"github.com/majorcontext/motte/internal/vfs/tel"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
	"github.com/majorcontext/motte/volume"
)

var logger = slog.New(slog.NewJSONHandler(os.Stderr, nil))

func main() { os.Exit(run()) }

// run enables trace export when OTEL_EXPORTER_OTLP_ENDPOINT is set, then runs the command.
func run() int {
	if len(os.Args) < 2 {
		logger.Error("usage: motte-vfsd mount|materialize|import|csi [flags]")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			logger.Error("trace exporter failed", "error", err.Error())
			return 1
		}
		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
		otel.SetTracerProvider(tp)
		defer func() {
			fctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = tp.Shutdown(fctx)
		}()
	}
	otel.SetTextMapPropagator(propagation.TraceContext{})
	var err error
	switch os.Args[1] {
	case "mount":
		err = runMount(ctx, os.Args[2:])
	case "materialize":
		err = runMaterialize(ctx, os.Args[2:])
	case "import":
		err = runImport(ctx, os.Args[2:])
	case "csi":
		err = runCSI(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		logger.Error("motte-vfsd failed", "command", os.Args[1], "error", err.Error())
		return 1
	}
	return 0
}

// storeFlags are the flags both commands need to reach the store.
type storeFlags struct {
	api, commit                             string
	workspace, repo, runtime, mountID, node string
	epoch                                   uint64
}

// telemetry names the mount in the spans and records of the command.
func (s *storeFlags) telemetry(mode string) tel.Telemetry {
	return tel.Telemetry{Logger: logger, Corr: tel.Correlation{WorkspaceID: s.workspace, Repo: s.repo, MountID: s.mountID, Mode: mode, Runtime: s.runtime, Node: s.node, Epoch: s.epoch}}
}

func (s *storeFlags) register(fl *flag.FlagSet) {
	fl.StringVar(&s.api, "api", "", "store base URL")
	fl.StringVar(&s.commit, "commit", "", "commit to serve")
	fl.StringVar(&s.workspace, "workspace", "", "workspace ID, for telemetry")
	fl.StringVar(&s.repo, "repo", "", "repo, for telemetry")
	fl.StringVar(&s.runtime, "runtime", "", "sandbox runtime, for telemetry")
	fl.StringVar(&s.mountID, "mount-id", "", "mount ID, for telemetry")
	fl.StringVar(&s.node, "node", os.Getenv("NODE_NAME"), "node name, for telemetry")
	fl.Uint64Var(&s.epoch, "epoch", 0, "claim epoch, for telemetry")
}

// open returns the store client and the commit. The grant comes from the environment, so it stays out of the process list.
func (s *storeFlags) open(mode string) (*store.HTTPStore, gitpack.SHA, error) {
	grant := os.Getenv(volume.GrantEnv)
	if s.api == "" || grant == "" {
		return nil, gitpack.SHA{}, fmt.Errorf("-api and %s are required", volume.GrantEnv)
	}
	sha, err := gitpack.ParseSHA(s.commit)
	if err != nil {
		return nil, sha, fmt.Errorf("-commit: %w", err)
	}
	return &store.HTTPStore{BaseURL: s.api, Grant: client.StaticGrant(grant), Client: &http.Client{Timeout: 2 * time.Minute}, Tel: s.telemetry(mode)}, sha, nil
}

func runImport(ctx context.Context, args []string) error {
	fl := flag.NewFlagSet("import", flag.ExitOnError)
	api := fl.String("api", "", "store base URL")
	remote := fl.String("remote", "", "repository to fetch")
	ref := fl.String("ref", "HEAD", "ref to import")
	depth := fl.Int("depth", 1, "history depth; zero imports the whole history as packs of at most about 48 MiB and skips the ranges that the store holds")
	if err := fl.Parse(args); err != nil {
		return err
	}
	grant := os.Getenv(volume.GrantEnv)
	if *api == "" || *remote == "" || grant == "" {
		return fmt.Errorf("-api, -remote and %s are required", volume.GrantEnv)
	}
	st := &store.HTTPStore{BaseURL: *api, Grant: client.StaticGrant(grant), Client: &http.Client{Timeout: 5 * time.Minute}}
	if *depth == 0 {
		sha, stats, err := importer.ImportHistory(ctx, st, *remote, *ref, func(format string, args ...any) { logger.Info(fmt.Sprintf(format, args...)) })
		if err != nil {
			return err
		}
		logger.Info("history imported", "packs", stats.Uploaded, "skipped", stats.Skipped, "bytes", stats.Bytes)
		fmt.Println(sha)
		return nil
	}
	sha, err := importer.Import(ctx, st, *remote, *ref, *depth)
	if err != nil {
		return err
	}
	fmt.Println(sha)
	return nil
}

func runCSI(ctx context.Context, args []string) error {
	fl := flag.NewFlagSet("csi", flag.ExitOnError)
	endpoint := fl.String("endpoint", "/csi/csi.sock", "unix socket of the plugin")
	driver := fl.String("driver", "vfs.motte.dev", "CSI driver name")
	node := fl.String("node-id", "node", "node name")
	state := fl.String("state", "/var/lib/motte-vfsd", "directory of the blob cache and the overlays")
	api := fl.String("api", "", "store base URL")
	keyFile := fl.String("grant-key-file", "", `file with the JSON {"kid","key"} of the key that signs node grants; key is the standard base64 of an Ed25519 private key or its 32-byte seed`)
	namespaces := fl.String("allowed-namespaces", "", "comma-separated namespaces whose pods may publish a volume; the plugin refuses every other pod")
	uid := fl.Uint("uid", 1000, "owner of base files")
	gid := fl.Uint("gid", 1000, "group of base files")
	if err := fl.Parse(args); err != nil {
		return err
	}
	var allowed []string
	for _, ns := range strings.Split(*namespaces, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			allowed = append(allowed, ns)
		}
	}
	if *api == "" || *keyFile == "" || len(allowed) == 0 {
		return errors.New("-api, -grant-key-file and -allowed-namespaces are required")
	}
	if err := csi.CheckHostPID(os.Readlink); err != nil {
		return err
	}
	minter, err := nodegrant.Load(*keyFile)
	if err != nil {
		return err
	}
	p := &csi.NodePlugin{
		DriverName: *driver, NodeID: *node, StateDir: *state, APIURL: *api,
		Grant: minter.Mint, AllowedNamespaces: allowed,
		UID: uint32(*uid), GID: uint32(*gid), HTTP: &http.Client{Timeout: 2 * time.Minute}, Logger: logger,
	}
	go (&tel.Health{Logger: logger, Node: *node, Mounts: p.Mounts, Tier2: store.Tier2Bytes}).Run(ctx)
	go p.SeedCacheSize(*state + "/cache")
	logger.Info("serving", "driver", *driver, "endpoint", *endpoint)
	return p.Serve(ctx, *endpoint)
}

func runMaterialize(ctx context.Context, args []string) error {
	fl := flag.NewFlagSet("materialize", flag.ExitOnError)
	var sf storeFlags
	sf.register(fl)
	dest := fl.String("dest", "", "directory to write the checkout to")
	marker := fl.String("marker", "", "file that records a finished checkout")
	uid := fl.Int("uid", -1, "owner of the files; -1 keeps the caller")
	gid := fl.Int("gid", -1, "group of the files; -1 keeps the caller")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *dest == "" || *marker == "" {
		return errors.New("-dest and -marker are required")
	}
	st, sha, err := sf.open("materialize")
	if err != nil {
		return err
	}
	return materialize.MaterializeWith(ctx, sf.telemetry("materialize"), st, sha, *dest, *marker, *uid, *gid)
}

func runMount(ctx context.Context, args []string) error {
	fl := flag.NewFlagSet("mount", flag.ExitOnError)
	var sf storeFlags
	sf.register(fl)
	mnt := fl.String("mnt", "", "mount point")
	cacheDir := fl.String("cache", "/var/lib/motte-vfsd/cache", "node blob cache")
	upper := fl.String("upper", "", "upper directory of the kernel overlay over the mount: the daemon serves a read-only lower layer and reads the dirty set from this directory")
	uid := fl.Uint("uid", 1000, "owner of base files")
	gid := fl.Uint("gid", 1000, "group of base files")
	ctlSock := fl.String("ctl-sock", "", "unix socket that serves the control protocol; its directory must be private to the workspace (mode 0700 or 0750)")
	noOpen := fl.Bool("noopen", false, "answer OPEN with ENOSYS so the kernel skips OPEN and RELEASE")
	if err := fl.Parse(args); err != nil {
		return err
	}
	if *mnt == "" || *upper == "" || *ctlSock == "" {
		return errors.New("-mnt, -upper and -ctl-sock are required")
	}
	if err := upperdir.ProbeTrustedBeside(*upper); err != nil {
		return err
	}
	st, sha, err := sf.open("mount")
	if err != nil {
		return err
	}
	tl := sf.telemetry("mount")
	h := &tel.Health{Logger: logger, Node: sf.node, Mounts: func() int { return 1 }, Tier2: store.Tier2Bytes}
	go h.Run(ctx)
	v, err := lower.New(ctx, lower.Config{Telemetry: tl, Store: st, CacheDir: *cacheDir, UpperDir: *upper, Base: sha, UID: uint32(*uid), GID: uint32(*gid), NoOpen: *noOpen})
	if err != nil {
		return err
	}
	defer v.Close()
	if err := os.MkdirAll(*mnt, 0o755); err != nil {
		return err
	}
	ln, err := ctl.Listen(*ctlSock, int(*uid), int(*gid))
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	srv, err := v.MountFUSE(ctx, *mnt, lower.MountOpts{DefaultPermission: true})
	if err != nil {
		if srv != nil {
			_ = srv.Unmount()
		}
		_ = ln.Close()
		return fmt.Errorf("mount: %w", err)
	}
	logger.Info("mounted", "commit", sha.String(), "mnt", *mnt)
	go func() {
		if err := (&ctl.Server{Store: st, Cache: v.Cache(), Workspace: v}).Serve(ctx, ln); err != nil {
			logger.Error("control socket failed", "error", err.Error())
		}
	}()
	serve(ctx, srv)
	return nil
}

func serve(ctx context.Context, srv *fuse.Server) {
	go func() {
		<-ctx.Done()
		if err := srv.Unmount(); err != nil {
			logger.Warn("unmount failed", "error", err.Error())
		}
	}()
	srv.Wait()
}
