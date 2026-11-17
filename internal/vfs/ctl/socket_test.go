//go:build linux

package ctl_test

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/internal/vfs/ctl"
)

// TestOverLongLineGetsAnErrAnswer: a request line over MaxLine is answered line_too_long, not dropped with a closed
// socket, and the next request on the connection is served.
func TestOverLongLineGetsAnErrAnswer(t *testing.T) {
	d := startDaemon(t, 1<<10)
	long := `{"op":"put","type":"blob","data":"` + strings.Repeat("A", 4<<10) + `"}`
	if ans := d.send(long); ans["code"] != "line_too_long" || ans["err"] == "" {
		t.Errorf("answer = %v, want the code line_too_long", ans)
	}
	if ans := d.send(`{"op":"opheads"}`); ans["gen"] != float64(0) {
		t.Errorf("the request after the long line got %v", ans)
	}
}

// TestServeClosesConnectionsWhenItStops: a shim that holds a connection open must not outlive the workspace.
func TestServeClosesConnectionsWhenItStops(t *testing.T) {
	d := startDaemon(t, 0)
	d.stop()
	d.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := d.rd.ReadByte(); err != io.EOF {
		t.Errorf("read after stop = %v, want EOF", err)
	}
}

// TestListenRefusesAnOpenDirectory: the listener opens before the mount, so a socket directory that other users can
// reach stops the start instead of leaving a mount that no shim can use.
func TestListenRefusesAnOpenDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	var l net.Listener
	l, err := ctl.Listen(filepath.Join(dir, "ctl.sock"), os.Getuid(), os.Getgid())
	if err == nil {
		_ = l.Close()
		t.Fatal("listener opened in a directory that is open to others")
	}
	if _, err := ctl.Listen("", 0, 0); err == nil {
		t.Error("listener opened with no socket path")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if l, err = ctl.Listen(filepath.Join(dir, "ctl.sock"), os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
}
