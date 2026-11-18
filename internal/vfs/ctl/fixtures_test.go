//go:build linux

package ctl_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/ctl"
	"github.com/majorcontext/motte/internal/vfs/lower"
	"github.com/majorcontext/motte/internal/vfs/store"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
	"github.com/majorcontext/motte/testserver"
)

type step struct {
	Name    string          `json:"name"`
	Given   string          `json:"given"`
	Request json.RawMessage `json:"request"`
	Raw     string          `json:"raw"`
	Answer  json.RawMessage `json:"answer"`
}

// daemon is a real control socket over the real service and a lower layer on an in-memory upper directory.
type daemon struct {
	t    *testing.T
	st   *store.HTTPStore
	up   *upperdir.MemFS
	vfs  *lower.VFS
	conn net.Conn
	rd   *bufio.Reader
	stop context.CancelFunc
}

func startDaemon(t *testing.T, maxLine int) *daemon {
	t.Helper()
	const repo = "gh:acme/widgets"
	srv := testserver.Start(t, "")
	op := client.New(srv.URL, client.StaticGrant(srv.OperatorGrant(t)))
	if _, err := op.Claim(t.Context(), "w1", api.ClaimRequest{Epoch: 1, Owner: "o", Repo: repo}); err != nil {
		t.Fatal(err)
	}
	st := &store.HTTPStore{BaseURL: srv.URL, Grant: client.StaticGrant(srv.NodeGrant(t, "w1", 1, repo))}
	blob := func(s string) gitpack.Object { return gitpack.Object{Type: gitpack.Blob, Data: []byte(s)} }
	entry := func(mode uint32, name string, o gitpack.Object) store.Entry {
		return store.Entry{Name: name, Mode: mode, SHA: gitpack.Hash(o.Type, o.Data)}
	}
	d := gitpack.Object{Type: gitpack.Tree, Data: store.EncodeTree([]store.Entry{entry(0o100644, "x", blob("2"))})}
	root := gitpack.Object{Type: gitpack.Tree, Data: store.EncodeTree([]store.Entry{entry(0o100644, "a", blob("1")), entry(0o40000, "d", d)})}
	commit := gitpack.Object{Type: gitpack.Commit, Data: []byte(fmt.Sprintf("tree %s\nauthor a <a@x> 1700000000 +0000\ncommitter a <a@x> 1700000000 +0000\n\nm\n", gitpack.Hash(root.Type, root.Data)))}
	if err := st.PutObjects(t.Context(), []gitpack.Object{blob("1"), blob("2"), d, root, commit}); err != nil {
		t.Fatal(err)
	}
	up := upperdir.NewMemFS()
	v, err := lower.New(t.Context(), lower.Config{Store: st, CacheDir: t.TempDir(), Base: gitpack.Hash(commit.Type, commit.Data), UpperFS: up})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Close)
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	l, err := ctl.Listen(sock, os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		_ = (&ctl.Server{Store: st, Cache: v.Cache(), Workspace: v, MaxLine: maxLine}).Serve(ctx, l)
		close(done)
	}()
	t.Cleanup(func() { stop(); <-done })
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &daemon{t: t, st: st, up: up, vfs: v, conn: c, rd: bufio.NewReader(c), stop: stop}
}

func (d *daemon) send(line string) map[string]any {
	d.t.Helper()
	if _, err := fmt.Fprintln(d.conn, line); err != nil {
		d.t.Fatal(err)
	}
	return d.read()
}

func (d *daemon) read() map[string]any {
	d.t.Helper()
	line, err := d.rd.ReadBytes('\n')
	if err != nil {
		d.t.Fatalf("read answer: %v", err)
	}
	var ans map[string]any
	if err := json.Unmarshal(line, &ans); err != nil {
		d.t.Fatalf("answer %q: %v", line, err)
	}
	return ans
}

func memFile(body string) upperdir.MemNode {
	return upperdir.MemNode{Mode: 0o644, Size: int64(len(body)), Data: []byte(body)}
}

// match reports whether got equals want. The string "*" in want matches any value that is present.
func match(got, want any) bool {
	if s, ok := want.(string); ok && s == "*" {
		return got != nil
	}
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for k, wv := range w {
			if gv, present := g[k]; !present || !match(gv, wv) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for i := range w {
			if !match(g[i], w[i]) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(got, want)
}

func TestControlProtocolFixtures(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("../../../testdata/ctl/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, f := range files {
		t.Run(strings.TrimSuffix(filepath.Base(f), ".json"), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var fx struct{ Steps []step }
			if err := json.Unmarshal(raw, &fx); err != nil {
				t.Fatal(err)
			}
			d := startDaemon(t, 0)
			for _, s := range fx.Steps {
				if s.Given == "edited" {
					d.up.Put("a", memFile("one"))
					d.up.Put("n", memFile("new"))
				}
				line := s.Raw
				if line == "" {
					line = string(s.Request)
				}
				var want any
				if err := json.Unmarshal(s.Answer, &want); err != nil {
					t.Fatal(err)
				}
				got := d.send(line)
				if !match(got, want) {
					t.Errorf("%s: request %s\n got  %v\n want %s", s.Name, line, got, s.Answer)
				}
				if code, _ := got["code"].(string); code != "" && code != "unknown_op" && !strings.Contains(got["err"].(string), code) {
					t.Errorf("%s: err %q lacks its code %q, which a released shim matches in the text", s.Name, got["err"], code)
				}
			}
		})
	}
}
