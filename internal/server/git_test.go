package server_test

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/pktline"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/internal/testcount"
	"github.com/majorcontext/motte/testserver"
)

type gitEnv struct {
	*env
	path, text string
}

func newGitEnv(t *testing.T) *gitEnv {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("the git binary is not installed")
	}
	t.Parallel()
	e := newEnv(t)
	path, err := e.Client(e.operator()).CreateRepo(context.Background(), "widgets", "o1")
	if err != nil {
		t.Fatal(err)
	}
	return &gitEnv{env: e, path: path, text: "native:" + path}
}

func (g *gitEnv) remote(repoPath, token string) string {
	return strings.Replace(g.url, "http://", "http://git:"+token+"@", 1) + "/git/native/" + repoPath + ".git"
}

// git runs git in dir and returns its output. It fails the test unless the command ends as wantOK says.
func (g *gitEnv) git(dir string, wantOK bool, args ...string) string {
	g.t.Helper()
	cmd := testcount.Command(g.t, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "HOME="+dir, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=a", "GIT_AUTHOR_EMAIL=a@example.com", "GIT_COMMITTER_NAME=a", "GIT_COMMITTER_EMAIL=a@example.com")
	out, err := cmd.CombinedOutput()
	if (err == nil) != wantOK {
		g.t.Fatalf("git %s: err = %v, want ok = %v\n%s", strings.Join(args, " "), err, wantOK, out)
	}
	return strings.TrimSpace(string(out))
}

// head returns the commit that HEAD of the clone in dir names, read from the repository files without a process.
func (g *gitEnv) head(dir string) string {
	g.t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, ".git", "HEAD"))
	if err != nil {
		g.t.Fatal(err)
	}
	ref, ok := strings.CutPrefix(strings.TrimSpace(string(raw)), "ref: ")
	if !ok {
		return strings.TrimSpace(string(raw))
	}
	if raw, err := os.ReadFile(filepath.Join(dir, ".git", ref)); err == nil {
		return strings.TrimSpace(string(raw))
	}
	packed, err := os.ReadFile(filepath.Join(dir, ".git", "packed-refs"))
	if err != nil {
		g.t.Fatal(err)
	}
	for _, line := range strings.Split(string(packed), "\n") {
		if sha, name, ok := strings.Cut(line, " "); ok && name == ref {
			return sha
		}
	}
	g.t.Fatalf("HEAD of %s names %s, which holds no commit", dir, ref)
	return ""
}

func (g *gitEnv) clone(token string, extra ...string) string {
	g.t.Helper()
	dir := g.t.TempDir()
	g.git(filepath.Dir(dir), true, append(append([]string{"clone", "-q"}, extra...), g.remote(g.path, token), dir)...)
	return dir
}

func (g *gitEnv) commit(dir, file, content string) {
	g.t.Helper()
	if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
		g.t.Fatal(err)
	}
	g.git(dir, true, "add", file)
	g.git(dir, true, "commit", "-q", "-m", "edit "+file)
}

func bigText(n int, tail string) string {
	return strings.Repeat("line of filler text for deltas\n", n) + tail
}

func TestGitCloneFetchAndPush(t *testing.T) {
	g := newGitEnv(t)
	read, write := g.readGrant(g.text), g.WriteGrant(t, g.text)
	reader, writer := g.clone(read), g.clone(write)
	g.commit(writer, "big.txt", bigText(2000, "one\n"))
	g.git(writer, true, "push", "-q", "origin", "HEAD:main")
	g.commit(writer, "big.txt", bigText(2000, "two\n"))
	g.git(writer, true, "push", "-q", "origin", "HEAD:refs/heads/feature/x")
	g.git(reader, true, "fetch", "-q", "origin")
	if got := g.git(reader, true, "show", "origin/feature/x:big.txt"); !strings.HasSuffix(got, "two") {
		t.Errorf("fetched branch content ends %q", got[len(got)-10:])
	}
}

func TestSmallRequestsNeedLittleMemory(t *testing.T) {
	t.Parallel()
	ts := testserver.StartLocal(t)
	srv := server.New(server.Config{Meta: ts.Meta, Bucket: ts.Bucket, Verifier: ts.Verifier})
	web := httptest.NewServer(srv.Handler())
	t.Cleanup(web.Close)
	e := &env{Server: ts, t: t, url: web.URL, cp: ts.Operator, node: ts.Node}
	path, err := e.Client(e.operator()).CreateRepo(context.Background(), "widgets", "o1")
	if err != nil {
		t.Fatal(err)
	}
	g := &gitEnv{env: e, path: path, text: "native:" + path}
	w, r := g.clone(g.WriteGrant(t, g.text)), g.clone(g.readGrant(g.text))
	g.commit(w, "a.txt", "a")
	server.HoldBudget(t, srv, store.MemoryBudget-100<<20)
	patient := []string{"-c", "http.lowSpeedLimit=1", "-c", "http.lowSpeedTime=3"}
	g.git(w, true, append(patient, "push", "-q", "origin", "HEAD:main")...)
	g.git(r, true, append(patient, "fetch", "-q", "origin")...)
	g.commit(w, "b.txt", "b")
	g.git(w, true, append(patient, "push", "-q", "origin", "HEAD:main")...)
	server.HoldBudget(t, srv, 90<<20)
	g.git(r, true, append(patient, "fetch", "-q", "origin")...)
	g.commit(w, "c.txt", "c")
	g.git(w, true, append(patient, "push", "-q", "origin", "HEAD:main")...)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	c := client.New(web.URL, client.StaticGrant(g.readGrant(g.text)))
	ref, err := c.Ref(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	commit, err := c.Commit(ctx, ref.Commit)
	if err != nil {
		t.Errorf("commit read: %v", err)
	}
	if _, err := c.Trees(ctx, commit.Tree, 0); err != nil {
		t.Errorf("tree read: %v", err)
	}
	if _, err := c.Object(ctx, ref.Commit); err != nil {
		t.Errorf("object read: %v", err)
	}
}

func TestGitAnnotatedTags(t *testing.T) {
	g := newGitEnv(t)
	writer, reader := g.clone(g.WriteGrant(t, g.text)), g.clone(g.readGrant(g.text))
	g.commit(writer, "t.txt", "t")
	g.git(writer, true, "tag", "-a", "-m", "release", "v1")
	g.git(writer, true, "push", "-q", "origin", "v1")
	g.git(reader, true, "fetch", "-q", "--tags", "origin")
	if got := g.git(reader, true, "rev-parse", "v1^{commit}"); got != g.head(writer) {
		t.Errorf("tag v1 peels to %s", got)
	}
	g.git(reader, true, "fsck", "--strict")
}

func TestGitPushRules(t *testing.T) {
	g := newGitEnv(t)
	write := g.WriteGrant(t, g.text)
	a, b := g.clone(write), g.clone(write)
	g.commit(a, "a.txt", "a")
	g.git(a, true, "push", "-q", "origin", "HEAD:main")
	g.commit(b, "b.txt", "b")
	g.git(b, false, "push", "-q", "origin", "HEAD:main")
	g.git(b, true, "push", "-q", "--force", "origin", "HEAD:main")
	head := g.head(b)
	if got := g.git(a, true, "ls-remote", "origin", "refs/heads/main"); !strings.HasPrefix(got, head) {
		t.Errorf("after the force push main = %q, want %s", got, head)
	}
	g.git(b, true, "push", "-q", "origin", "HEAD:refs/heads/gone")
	g.git(b, true, "push", "-q", "origin", ":gone")
	if got := g.git(a, true, "ls-remote", "origin", "refs/heads/gone"); got != "" {
		t.Errorf("deleted branch is listed: %q", got)
	}
}

func TestGitRefusedPushesAreReported(t *testing.T) {
	g := newGitEnv(t)
	write := g.WriteGrant(t, g.text)
	head := g.head(g.clone(write))
	line := fmt.Sprintf("%s %s refs/heads/main\x00report-status\n", strings.Repeat("1", 40), head)
	if reply := g.rawPush(write, line, emptyPack()); !strings.Contains(reply, "ng refs/heads/main stale info") {
		t.Errorf("stale old value reply = %q", reply)
	}
	thin := fmt.Sprintf("%s %s refs/heads/thin\x00report-status\n", strings.Repeat("0", 40), head)
	reply := g.rawPush(write, thin, thinPackOnAbsentBase())
	if !strings.Contains(reply, "unpack invalid pack") || !strings.Contains(reply, "ng refs/heads/thin unpacker error") {
		t.Errorf("thin pack on an absent base reply = %q", reply)
	}
}

func emptyPack() string {
	head := "PACK\x00\x00\x00\x02\x00\x00\x00\x00"
	sum := sha1.Sum([]byte(head))
	return head + string(sum[:])
}

// thinPackOnAbsentBase is a pack of one ref delta on a base that no repository holds.
func thinPackOnAbsentBase() string {
	var z bytes.Buffer
	zw := zlib.NewWriter(&z)
	_, _ = zw.Write([]byte{5, 3, 3, 'a', 'b', 'c'})
	_ = zw.Close()
	head := "PACK\x00\x00\x00\x02\x00\x00\x00\x01" + string([]byte{0x76}) + strings.Repeat("\x09", 20) + z.String()
	sum := sha1.Sum([]byte(head))
	return head + string(sum[:])
}

func (g *gitEnv) rawPush(token, command, pack string) string {
	g.t.Helper()
	body := fmt.Sprintf("%04x%s0000%s", len(command)+4, command, pack)
	resp, err := http.Post(g.remote(g.path, token)+"/git-receive-pack", "application/x-git-receive-pack-request", strings.NewReader(body))
	if err != nil {
		g.t.Fatal(err)
	}
	defer resp.Body.Close()
	var reply bytes.Buffer
	_, _ = reply.ReadFrom(resp.Body)
	return reply.String()
}

func TestGitAuthorization(t *testing.T) {
	g := newGitEnv(t)
	refs := g.url + "/git/native/" + g.path + ".git/info/refs?service=git-upload-pack"
	resp, err := http.Get(refs)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || !strings.HasPrefix(resp.Header.Get("WWW-Authenticate"), "Basic") {
		t.Errorf("no grant: %d %q, want 401 with a Basic challenge", resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
	}
	post, err := http.NewRequest("POST", g.remote(g.path, g.readGrant(g.text))+"/git-upload-pack", strings.NewReader("0014command=ls-refs\n00010000"))
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := http.DefaultClient.Do(post); err != nil || resp.StatusCode != 400 {
		t.Errorf("upload-pack without protocol v2: %v, %v, want 400", resp, err)
	}
	scratch := t.TempDir()
	g.git(scratch, false, "clone", "-q", g.url+"/git/native/"+g.path+".git", filepath.Join(scratch, "x"))
	reader := g.clone(g.readGrant(g.text))
	g.commit(reader, "r.txt", "r")
	g.git(reader, false, "push", "-q", "origin", "HEAD:main")
	g.git(scratch, false, "clone", "-q", g.remote(g.path, g.readGrant("native:repo_other")), filepath.Join(scratch, "y"))
	bearer := g.clone("", "-c", "http.extraHeader=Authorization: Bearer "+g.readGrant(g.text))
	g.head(bearer)
	g.git(scratch, false, "ls-remote", g.remote("repo_missing", g.WriteGrant(t, "native:repo_missing")))
}

func TestGitPartialClone(t *testing.T) {
	g := newGitEnv(t)
	write := g.WriteGrant(t, g.text)
	w := g.clone(write)
	g.commit(w, "doc.txt", "lazy content\n")
	g.git(w, true, "push", "-q", "origin", "HEAD:main")
	p := g.clone(write, "--filter=blob:none", "--no-checkout")
	if missing := g.git(p, true, "rev-list", "--objects", "--missing=print", "HEAD"); !strings.Contains(missing, "?") {
		t.Errorf("a blob:none clone holds every blob: %q", missing)
	}
	if got := g.git(p, true, "show", "HEAD:doc.txt"); got != "lazy content" {
		t.Errorf("lazy blob = %q", got)
	}
	scratch := t.TempDir()
	g.git(scratch, false, "clone", "-q", "--filter=tree:0", g.remote(g.path, write), filepath.Join(scratch, "x"))
}

func TestGitServesAnImportedPack(t *testing.T) {
	g := newGitEnv(t)
	ctx := context.Background()
	blob := []byte("imported\n")
	blobSHA := gitpack.Hash(gitpack.Blob, blob)
	tree := append([]byte("100644 imp.txt\x00"), blobSHA[:]...)
	treeSHA := gitpack.Hash(gitpack.Tree, tree)
	who := "a <a@example.com> 1 +0000"
	commit := []byte(fmt.Sprintf("tree %s\nauthor %s\ncommitter %s\n\nimported\n", treeSHA, who, who))
	main, err := g.Client(g.readGrant(g.text)).Ref(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	pack, idx, _ := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: blob}, {Type: gitpack.Tree, Data: tree}, {Type: gitpack.Commit, Data: commit}})
	sum := sha256.Sum256(pack)
	if err := g.Client(g.importGrant(g.text)).ImportPack(ctx, hex.EncodeToString(sum[:]), idx, pack); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Client(g.operator()).Claim(ctx, "ws1", api.ClaimRequest{Epoch: 1, Owner: "o1", Repo: g.text}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Client(g.NodeGrant(t, "ws1", 1, g.text)).SwapRef(ctx, "main", main.Generation, gitpack.Hash(gitpack.Commit, commit).String()); err != nil {
		t.Fatal(err)
	}
	dir := g.clone(g.readGrant(g.text))
	if got, err := os.ReadFile(filepath.Join(dir, "imp.txt")); err != nil || string(got) != "imported\n" {
		t.Errorf("imported file = %q, %v", got, err)
	}
}

func TestGitOnTheLocalBackend(t *testing.T) {
	prev := startServer
	startServer = func(t *testing.T) *testserver.Server { return testserver.StartLocal(t) }
	t.Cleanup(func() { startServer = prev })
	for name, fn := range map[string]func(*testing.T){
		"clone fetch push": TestGitCloneFetchAndPush,
		"push rules":       TestGitPushRules,
		"tags":             TestGitAnnotatedTags,
		"refused pushes":   TestGitRefusedPushesAreReported,
		"authorization":    TestGitAuthorization,
		"partial clone":    TestGitPartialClone,
		"imported pack":    TestGitServesAnImportedPack,
	} {
		t.Run(name, fn)
	}
}

func TestLargeFetchesTakeTurnsAndAllFinish(t *testing.T) {
	server.LowerFetchSlots(t, 1, 1)
	ts := testserver.StartLocal(t)
	l := newLandEnv(t, &env{Server: ts, t: t, url: ts.URL, cp: ts.Operator, node: ts.Node})
	var body bytes.Buffer
	pw := pktline.NewWriter(&body)
	pw.Linef("command=fetch\n")
	pw.Delim()
	pw.Linef("want %s\n", l.root)
	pw.Linef("done\n")
	pw.Flush()
	token := l.ReadGrant(t, l.repo)
	var wg sync.WaitGroup
	status := make([]int, 6)
	for i := range status {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/git/gh/acme/widgets.git/git-upload-pack", bytes.NewReader(body.Bytes()))
			req.Header.Set("Git-Protocol", "version=2")
			req.Header.Set("Authorization", "Bearer "+token)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				status[i] = resp.StatusCode
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	for i, s := range status {
		if s != http.StatusOK {
			t.Errorf("fetch %d answered %d, want 200", i, s)
		}
	}
}
