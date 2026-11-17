package ctl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

const ctlLineMax = 64 << 20

// Listen binds the control socket before a caller mounts. The socket must lie in a directory that only the workspace
// of this mount can reach: the directory may not be open to others, and the socket is created 0660 and owned by uid
// and gid, so no other workspace or node process can connect.
func Listen(path string, uid, gid int) (net.Listener, error) {
	st, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if st.Mode().Perm()&0o007 != 0 {
		return nil, fmt.Errorf("vfs: control socket directory %s is open to others", filepath.Dir(path))
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

// Serve answers the control protocol on l until ctx ends.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.serveConn(ctx, c)
	}
}

func (s *Server) serveConn(ctx context.Context, c net.Conn) {
	defer func() { _ = c.Close() }()
	sc := bufio.NewScanner(c)
	sc.Buffer(make([]byte, 1<<20), ctlLineMax)
	enc := json.NewEncoder(c)
	for sc.Scan() {
		var rq ctlReq
		rs := ctlResp{}
		if err := json.Unmarshal(sc.Bytes(), &rq); err != nil {
			rs.Err = err.Error()
		} else {
			rs = s.handle(ctx, rq)
		}
		if enc.Encode(rs) != nil {
			return
		}
	}
}
