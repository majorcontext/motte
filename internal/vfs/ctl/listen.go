package ctl

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// Listen binds the control socket before a caller mounts. The socket must lie in a directory that only the workspace
// of this mount can reach: the directory may not be open to others, and the socket is created 0660 and owned by uid
// and gid, so no other workspace or node process can connect.
func Listen(path string, uid, gid int) (net.Listener, error) {
	if path == "" {
		return nil, errors.New("ctl: the mount needs a control socket")
	}
	st, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if st.Mode().Perm()&0o007 != 0 {
		return nil, fmt.Errorf("ctl: control socket directory %s is open to others", filepath.Dir(path))
	}
	_ = os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := os.Chown(path, uid, gid); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}
