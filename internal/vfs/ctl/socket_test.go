//go:build linux

package ctl_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/majorcontext/motte/internal/vfs/ctl"
)

// TestOverLongLineGetsAnErrAnswer: a request line over MaxLine is answered line_too_long, not dropped with a closed
// socket, and the next request on the connection is served.
func TestOverLongLineGetsAnErrAnswer(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	d := startDaemon(t, 0)
	d.stop()
	d.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := d.rd.ReadByte(); err != io.EOF && !errors.Is(err, syscall.ECONNRESET) {
		t.Errorf("read after stop = %v, want a closed connection", err)
	}
}

// TestListenRefusesAnOpenDirectory: the listener opens before the mount, so a socket directory that other users can
// reach stops the start instead of leaving a mount that no shim can use.
func TestListenRefusesAnOpenDirectory(t *testing.T) {
	t.Parallel()
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

// TestStatsRecordCountsControlAndStoreCalls: an operator must see how many control calls of each op the daemon
// served and which store requests they caused, or the time of one snapshot cannot be attributed.
func TestStatsRecordCountsControlAndStoreCalls(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	d := startDaemonLogged(t, 0, slog.New(slog.NewJSONHandler(&buf, nil)))
	d.send(`{"op":"put","type":"blob","data":"eA=="}`)
	d.send(`{"op":"opheads"}`)
	d.send(`{"op":"opheads"}`)
	d.vfs.Close()
	var rec map[string]any
	for _, l := range bytes.Split(buf.Bytes(), []byte("\n")) {
		var r map[string]any
		if json.Unmarshal(l, &r) == nil && r["msg"] == "vfs.stats" && r["ctl_opheads_count"] != nil {
			rec = r
		}
	}
	want := map[string]float64{"ctl_put_count": 1, "ctl_opheads_count": 2, "store_object_put_count": 1, "store_op_heads_get_count": 2}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("%s = %v, want %v", k, rec[k], v)
		}
	}
	for _, k := range []string{"ctl_opheads_total_us", "ctl_opheads_max_us", "store_op_heads_get_total_us", "store_op_heads_get_max_us"} {
		if _, ok := rec[k].(float64); !ok {
			t.Errorf("%s missing from %v", k, rec)
		}
	}
}
