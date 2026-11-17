// Package vfs is the node daemon of the virtual workspace: a lazy read-only file system over one commit of the
// version control store, the lower layer of a kernel overlay, with the dirty set read from the upper directory,
// and a checkout writer. The FUSE server builds on Linux only.
package vfs

import (
	"errors"
	"fmt"
	"os"

	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
	volattr "github.com/majorcontext/motte/volume"
)

// Config is what the daemon needs to serve one commit.
type Config struct {
	Store    Store
	CacheDir string
	// UpperDir is the upper directory of the kernel overlay over the mount. The daemon reads the dirty set there.
	UpperDir string
	CtlSock  string
	Base     gitpack.SHA // commit
	UID, GID uint32
	Cache    *Cache
	NoOpen   bool
	// Clock paces the stats record and the settle test of the upper directory walk. Nil is the real clock.
	Clock     clock.Clock
	Telemetry Telemetry

	upperFS upperdir.FS
}

// Publication and ParsePublication are the volume attributes of a workspace, shared with the sandbox driver.
type Publication = volattr.Publication

var ParsePublication = volattr.ParsePublication

// validate refuses a config that lacks what the mount needs.
func (c Config) validate() error {
	switch {
	case c.CtlSock == "":
		return errors.New("vfs: the mount needs a control socket, because it serves no control file")
	case c.upperFS != nil:
	case c.UpperDir == "":
		return errors.New("vfs: the mount needs the upper directory of its overlay")
	default:
		if st, err := os.Stat(c.UpperDir); err != nil || !st.IsDir() {
			return fmt.Errorf("vfs: upper directory %s is not a directory", c.UpperDir)
		}
	}
	return nil
}
