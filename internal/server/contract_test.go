package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/testserver"
)

const (
	repoText = "gh:acme/widgets"
	commitA  = "1111111111111111111111111111111111111111"
	commitB  = "2222222222222222222222222222222222222222"
)

type env struct {
	*testserver.Server
	t    *testing.T
	url  string
	cp   grant.Signer
	node grant.Signer
}

type reply struct {
	status int
	code   string
	body   map[string]any
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ts := testserver.Start(t, "")
	return &env{Server: ts, t: t, url: ts.URL, cp: ts.Operator, node: ts.Node}
}

func (e *env) mint(s grant.Signer, c grant.Claims) string { return e.Mint(e.t, s, c) }

func (e *env) operator() string { return e.OperatorGrant(e.t) }

func (e *env) nodeGrant(ws string, epoch uint64) string { return e.NodeGrant(e.t, ws, epoch, repoText) }

func (e *env) readGrant(repo string) string { return e.ReadGrant(e.t, repo) }

func (e *env) importGrant(repo string) string { return e.ImportGrant(e.t, repo) }

// refuses fails unless err is the sentinel at the HTTP status.
func refuses(t *testing.T, err error, sentinel error, status int) {
	t.Helper()
	var ce *client.Error
	if !errors.Is(err, sentinel) || !errors.As(err, &ce) || ce.Status != status {
		t.Fatalf("error = %v, want %v with status %d", err, sentinel, status)
	}
}

func (e *env) call(method, path, token string, body any) reply {
	e.t.Helper()
	var rd bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = *bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, e.url+path, &rd)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	r := reply{status: resp.StatusCode}
	_ = json.NewDecoder(resp.Body).Decode(&r.body)
	if errBody, ok := r.body["error"].(map[string]any); ok {
		r.code, _ = errBody["code"].(string)
	}
	return r
}

func (e *env) claim(ws string, epoch uint64, repo string) reply {
	return e.call(http.MethodPut, "/v1/workspaces/"+ws+"/claim", e.operator(), map[string]any{"epoch": epoch, "owner": "o1", "repo": repo})
}

func (e *env) want(r reply, status int, code string) {
	e.t.Helper()
	if r.status != status || r.code != code {
		e.t.Fatalf("got %d %q %v; want %d %q", r.status, r.code, r.body, status, code)
	}
}

func TestStaleEpochWritesAreRefusedAfterAClaimRaise(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	old := e.nodeGrant("ws1", 1)
	e.want(e.call("PUT", "/v1/vcs/refs/keep", old, map[string]any{"expect": 0, "commit": commitA}), 200, "")
	e.want(e.call("PUT", "/v1/vcs/op-heads", old, map[string]any{"expect_generation": 0, "heads": []string{"aaaaaaaaaaaaaaaa"}}), 200, "")
	e.want(e.claim("ws1", 2, repoText), 200, "")
	writes := []struct {
		name, method, path string
		body               any
	}{
		{"ref create", "PUT", "/v1/vcs/refs/new", map[string]any{"expect": 0, "commit": commitA}},
		{"ref swap", "PUT", "/v1/vcs/refs/keep", map[string]any{"expect": 1, "commit": commitB}},
		{"ref delete", "DELETE", "/v1/vcs/refs/keep?expect=1", nil},
		{"op heads", "PUT", "/v1/vcs/op-heads", map[string]any{"expect_generation": 1, "heads": []string{"bbbbbbbbbbbbbbbb"}}},
		{"workspace report", "PUT", "/v1/vcs/workspace", map[string]any{"op": "aaaaaaaaaaaaaaaa", "commit": commitA, "generation": 1}},
		{"read", "GET", "/v1/vcs/refs", nil},
	}
	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) { e.want(e.call(w.method, w.path, old, w.body), 409, "vcs_claim_lost") })
	}
	e.want(e.claim("ws1", 1, repoText), 409, "vcs_claim_lost")
	e.want(e.call("PUT", "/v1/vcs/refs/new", e.nodeGrant("ws1", 2), map[string]any{"expect": 0, "commit": commitA}), 200, "")
}

func TestRefGenerationIsACompareAndSwapThatNeverRepeats(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	node := e.Client(e.nodeGrant("ws1", 1))
	ctx := context.Background()
	first, err := node.SwapRef(ctx, "feature/x", 0, commitA)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		expect int64
	}{{"create over an existing ref", 0}, {"stale expect", first.Generation + 100}} {
		_, err := node.SwapRef(ctx, "feature/x", c.expect, commitB)
		refuses(t, err, client.ErrGenerationConflict, 409)
		if stored, ok := client.CurrentRef(err); !ok || stored.Commit != commitA || stored.Generation != first.Generation {
			t.Errorf("%s: conflict carries %v, want the stored ref", c.name, stored)
		}
	}
	second, err := node.SwapRef(ctx, "feature/x", first.Generation, commitB)
	if err != nil || second.Generation <= first.Generation {
		t.Fatalf("swap = %v, %v; want a generation above %d", second, err, first.Generation)
	}
	refuses(t, node.DeleteRef(ctx, "feature/x", first.Generation), client.ErrGenerationConflict, 409)
	if err := node.DeleteRef(ctx, "feature/x", second.Generation); err != nil {
		t.Fatal(err)
	}
	if third, err := node.SwapRef(ctx, "feature/x", 0, commitA); err != nil || third.Generation <= second.Generation {
		t.Errorf("create after delete = %v, %v; want a generation above %d", third, err, second.Generation)
	}
}

func TestImportedPackServesObjects(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	pack, idx, names := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: []byte("hello")}})
	sum := sha256.Sum256(pack)
	id := hex.EncodeToString(sum[:])
	ctx := context.Background()
	imp := e.Client(e.importGrant(repoText))
	refuses(t, imp.ImportPack(ctx, strings.Repeat("0", 64), idx, pack), client.ErrObjectInvalid, 400)
	if err := imp.ImportPack(ctx, id, idx, pack); err != nil {
		t.Fatal(err)
	}
	sha := names[0].String()
	for name, c := range map[string]*client.Client{"node": e.Client(e.nodeGrant("ws1", 1)), "read": e.Client(e.readGrant(repoText))} {
		if o, err := c.Object(ctx, sha); err != nil || o.Type != "blob" || string(o.Data) != "hello" {
			t.Errorf("%s grant read = %v, %v", name, o, err)
		}
	}
	if p, err := imp.Has(ctx, []string{sha, commitA}); err != nil || len(p) != 2 || !p[0] || p[1] {
		t.Errorf("has = %v, %v", p, err)
	}
	_, err := e.Client(e.readGrant("gh:acme/other")).Object(ctx, sha)
	refuses(t, err, client.ErrNotFound, 404)
}

func TestDeletedWorkspaceRefusesReadsAndWrites(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	ctx := context.Background()
	node, op := e.Client(e.nodeGrant("ws1", 1)), e.Client(e.operator())
	for range 2 {
		if err := op.DeleteWorkspace(ctx, "ws1"); err != nil {
			t.Fatal(err)
		}
	}
	_, err := node.Refs(ctx)
	refuses(t, err, client.ErrUnauthorized, 401)
	_, err = node.SwapRef(ctx, "x", 0, commitA)
	refuses(t, err, client.ErrUnauthorized, 401)
	_, err = op.Claim(ctx, "ws1", api.ClaimRequest{Epoch: 2, Owner: "o1", Repo: repoText})
	refuses(t, err, client.ErrWorkspaceDeleted, 410)
}

func TestCreateRepoStartsOnARootCommitOfMain(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	op := e.Client(e.operator())
	for _, name := range []string{"", "   ", strings.Repeat("a", 101), "bad\x07name"} {
		_, err := op.CreateRepo(ctx, name, "o1")
		refuses(t, err, client.ErrInvalidBody, 400)
	}
	path, err := op.CreateRepo(ctx, "  widgets  ", "o1")
	if err != nil || !strings.HasPrefix(path, "repo_") {
		t.Fatalf("create = %q, %v", path, err)
	}
	if _, err := op.CreateRepo(ctx, "second", "o1"); err != nil {
		t.Fatal(err)
	}
	read := e.Client(e.readGrant("native:" + path))
	ref, err := read.Ref(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	c, err := read.Commit(ctx, ref.Commit)
	if emptyTree := gitpack.Hash(gitpack.Tree, nil).String(); err != nil || c.Tree != emptyTree {
		t.Errorf("root commit = %v, %v, want the empty tree %s", c, err, emptyTree)
	}
	repos, err := op.Repos(ctx)
	if err != nil || len(repos) != 2 || repos[1].Name != "widgets" {
		t.Errorf("list = %v, %v, want newest first with the trimmed name last", repos, err)
	}
	if _, err := op.Repo(ctx, path); err != nil {
		t.Error(err)
	}
	_, err = op.Repo(ctx, "repo_missing")
	refuses(t, err, client.ErrNotFound, 404)
}

func TestClaimBindsOwnerAndRepoOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	op := e.Client(e.operator())
	path, err := op.CreateRepo(ctx, "w", "o1")
	if err != nil {
		t.Fatal(err)
	}
	native := "native:" + path
	claim := func(ws, owner, repo string) error {
		_, err := op.Claim(ctx, ws, api.ClaimRequest{Epoch: 1, Owner: owner, Repo: repo})
		return err
	}
	if err := claim("ws1", "o1", native); err != nil {
		t.Fatal(err)
	}
	refuses(t, claim("ws2", "o2", native), client.ErrRepoForbidden, 403)
	refuses(t, claim("ws1", "o2", native), client.ErrRepoForbidden, 403)
	refuses(t, claim("ws3", "o1", "native:repo_unknown"), client.ErrRepoForbidden, 403)
	if err := claim("ws4", "o1", repoText); err != nil {
		t.Fatal(err)
	}
	refuses(t, claim("ws4", "o1", "gh:acme/other"), client.ErrWorkspaceConflict, 409)
}

func TestFlushOrdersByGenerationUnderTheEpochFence(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	op := e.Client(e.operator())
	claim := func(epoch uint64) api.ClaimResponse {
		r, err := op.Claim(ctx, "ws1", api.ClaimRequest{Epoch: epoch, Owner: "o1", Repo: repoText})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	flush := func(epoch uint64, gen int64, prev, id string) error {
		return op.Flush(ctx, "ws1", api.FlushRequest{Epoch: epoch, Generation: gen, Prev: prev, Op: id, Snapshot: commitA})
	}
	claim(1)
	opA, opB, opC := strings.Repeat("a", 16), strings.Repeat("b", 16), strings.Repeat("c", 16)
	for _, f := range []struct {
		gen      int64
		prev, id string
	}{{5, "", opA}, {3, "", opB}, {0, opB, opC}} {
		if err := flush(1, f.gen, f.prev, f.id); err != nil {
			t.Fatal(err)
		}
	}
	if r := claim(1); r.Op != opA || r.Snapshot != commitA {
		t.Errorf("claim after flushes = %v, want op %s: the lower generation and the mismatched prev are dropped", r, opA)
	}
	if err := flush(1, 0, opA, opC); err != nil {
		t.Fatal(err)
	}
	if r := claim(1); r.Op != opC {
		t.Errorf("claim after a matching prev = %v, want op %s", r, opC)
	}
	claim(2)
	refuses(t, flush(1, 6, "", opB), client.ErrClaimLost, 409)
}

func TestGrantScopeRefusals(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	node, read, imp, op := e.nodeGrant("ws1", 1), e.readGrant(repoText), e.importGrant(repoText), e.operator()
	nodeOther := e.mint(e.node, grant.Claims{Scope: grant.ScopeNode, WS: "ws1", Epoch: 1, Repo: "gh:acme/other"})
	cpNode := e.mint(e.cp, grant.Claims{Scope: grant.ScopeNode, WS: "ws1", Epoch: 1, Repo: repoText})
	nodeKeyOperator := func() string {
		tok, err := e.node.Mint(grant.Claims{Scope: grant.ScopeOperator, Exp: e.Expire.Unix()})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}()
	expired := e.mint(e.cp, grant.Claims{Scope: grant.ScopeRead, Repo: repoText, Exp: time.Now().Add(-time.Minute).Unix()})
	body := map[string]any{"expect": 0, "commit": commitA}
	cases := []struct {
		name, method, path, token string
		body                      any
		status                    int
		code                      string
	}{
		{"read grant writes a ref", "PUT", "/v1/vcs/refs/x", read, body, 401, "unauthorized"},
		{"import grant reads a ref", "GET", "/v1/vcs/refs", imp, nil, 401, "unauthorized"},
		{"node grant imports a pack", "PUT", "/v1/vcs/packs/" + strings.Repeat("0", 64), node, map[string]any{}, 401, "unauthorized"},
		{"node grant creates a repo", "POST", "/v1/repos", node, map[string]any{"name": "w", "owner": "o"}, 401, "unauthorized"},
		{"operator grant reads a ref", "GET", "/v1/vcs/refs", op, nil, 401, "unauthorized"},
		{"read grant claims", "PUT", "/v1/workspaces/ws1/claim", read, map[string]any{}, 401, "unauthorized"},
		{"node key signs an operator grant", "GET", "/v1/repos", nodeKeyOperator, nil, 401, "unauthorized"},
		{"expired grant", "GET", "/v1/vcs/refs", expired, nil, 401, "unauthorized"},
		{"no grant", "GET", "/v1/vcs/refs", "", nil, 401, "unauthorized"},
		{"node grant of another repo", "GET", "/v1/vcs/refs", nodeOther, nil, 403, "vcs_repo_forbidden"},
		{"node grant of the control key is valid", "GET", "/v1/vcs/refs", cpNode, nil, 200, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { e.want(e.call(c.method, c.path, c.token, c.body), c.status, c.code) })
	}
}

func TestGrantWithoutTheBearerSchemeIsRefused(t *testing.T) {
	e := newEnv(t)
	req, _ := http.NewRequest("GET", e.url+"/v1/repos", nil)
	req.Header.Set("Authorization", e.operator())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestBodyOverTheLimitAnswersTooLarge(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	r := e.call("POST", "/v1/vcs/objects/has", e.readGrant(repoText), map[string]any{"shas": []string{strings.Repeat("a", 1<<20)}})
	e.want(r, 413, "vcs_object_too_large")
	_, err := e.Client(e.nodeGrant("ws1", 1), client.WithBudget(30*time.Second)).PutObjects(context.Background(), []client.Object{{Type: "blob", Data: make([]byte, gitpack.MaxObjectBytes+1)}})
	refuses(t, err, client.ErrObjectTooLarge, 413)
}

func TestSlowUploadDoesNotStallOtherRequests(t *testing.T) {
	for method, path := range map[string]string{"POST": "/v1/vcs/objects", "PUT": "/v1/vcs/ops/operations/0123456789abcdef"} {
		t.Run(path, func(t *testing.T) {
			e := newEnv(t)
			e.want(e.claim("ws1", 1, repoText), 200, "")
			body, w := io.Pipe()
			reading := make(chan struct{})
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{Got100Continue: func() { close(reading) }})
			req, _ := http.NewRequestWithContext(ctx, method, e.url+path, body)
			req.Header.Set("Authorization", "Bearer "+e.nodeGrant("ws1", 1))
			req.Header.Set("Expect", "100-continue")
			go func() {
				if resp, err := http.DefaultClient.Do(req); err == nil {
					resp.Body.Close()
				}
			}()
			<-reading
			if _, err := e.Client(e.readGrant(repoText)).Has(context.Background(), []string{commitA}); err != nil {
				t.Fatalf("a read waited for a stalled upload: %v", err)
			}
			w.CloseWithError(errors.New("sender gone"))
		})
	}
}

func TestUnknownRouteAndMethodUseTheErrorEnvelope(t *testing.T) {
	e := newEnv(t)
	e.want(e.call("GET", "/v1/nothing", e.operator(), nil), 404, "vcs_not_found")
	e.want(e.call("DELETE", "/v1/repos", e.operator(), nil), 405, "method_not_allowed")
}

func TestLargestObjectIsStoredAndALargeTreeIsRead(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	ctx := context.Background()
	node := e.Client(e.nodeGrant("ws1", 1), client.WithBudget(30*time.Second))
	if _, err := node.PutObjects(ctx, []client.Object{{Type: "blob", Data: make([]byte, gitpack.MaxObjectBytes)}}); err != nil {
		t.Fatalf("object at the limit: %v", err)
	}
	var tree bytes.Buffer
	for i := 0; tree.Len() < 5<<20; i++ {
		fmt.Fprintf(&tree, "160000 e%07d\x00%s", i, strings.Repeat("a", 20))
	}
	shas, err := node.PutObjects(ctx, []client.Object{{Type: "tree", Data: tree.Bytes()}})
	if err != nil {
		t.Fatal(err)
	}
	trees, err := e.Client(e.readGrant(repoText), client.WithBudget(30*time.Second)).Trees(ctx, shas[0], 100000)
	if err != nil || len(trees.Trees) != 1 || len(trees.Trees[0].Entries) < 100000 {
		t.Fatalf("tree over 4 MiB: %v %v", len(trees.Trees), err)
	}
}

func TestObjectsBodyStartingWithAHugeTokenAnswersTooLarge(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	req, _ := http.NewRequest("POST", e.url+"/v1/vcs/objects", strings.NewReader(`"`+strings.Repeat("a", 25<<20)+`"`))
	req.Header.Set("Authorization", "Bearer "+e.nodeGrant("ws1", 1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}
