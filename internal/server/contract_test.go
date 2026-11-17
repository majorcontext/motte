package server_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/objstore"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/internal/store"
)

const (
	repoText = "gh:acme/widgets"
	commitA  = "1111111111111111111111111111111111111111"
	commitB  = "2222222222222222222222222222222222222222"
)

type env struct {
	t      *testing.T
	pool   *pgxpool.Pool
	url    string
	cp     grant.Signer
	node   grant.Signer
	expire time.Time
}

type reply struct {
	status int
	code   string
	body   map[string]any
}

// newEnv starts the real server on a fresh schema of the test database.
func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("MOTTE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MOTTE_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var raw [6]byte
	_, _ = rand.Read(raw[:])
	schema := "t_" + hex.EncodeToString(raw[:])
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	cpPub, cpKey, _ := ed25519.GenerateKey(rand.Reader)
	nodePub, nodeKey, _ := ed25519.GenerateKey(rand.Reader)
	db := &pg.DB{Pool: pool}
	srv := &server.Server{
		DB: db, Objects: store.New(objstore.NewMemory(), db),
		Verifier: grant.NewVerifier(
			grant.Key{Kid: "cp", Public: cpPub, Scopes: []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeImport, grant.ScopeOperator}},
			grant.Key{Kid: "node", Public: nodePub, Scopes: []grant.Scope{grant.ScopeNode}}),
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &env{t: t, pool: pool, url: ts.URL, cp: grant.Signer{Kid: "cp", Key: cpKey}, node: grant.Signer{Kid: "node", Key: nodeKey}, expire: time.Now().Add(time.Hour)}
}

func (e *env) mint(s grant.Signer, c grant.Claims) string {
	e.t.Helper()
	if c.Exp == 0 {
		c.Exp = e.expire.Unix()
	}
	tok, err := s.Mint(c)
	if err != nil {
		e.t.Fatal(err)
	}
	return tok
}

func (e *env) operator() string { return e.mint(e.cp, grant.Claims{Scope: grant.ScopeOperator}) }

func (e *env) nodeGrant(ws string, epoch uint64) string {
	return e.mint(e.node, grant.Claims{Scope: grant.ScopeNode, WS: ws, Epoch: epoch, Repo: repoText})
}

func (e *env) readGrant(repo string) string {
	return e.mint(e.cp, grant.Claims{Scope: grant.ScopeRead, Repo: repo})
}

func (e *env) importGrant(repo string) string {
	return e.mint(e.cp, grant.Claims{Scope: grant.ScopeImport, Repo: repo})
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

func TestClaimWaitsForAWriteInFlight(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id string
	if err := tx.QueryRow(ctx, `SELECT id FROM workspaces WHERE id = 'ws1' AND epoch = 1 FOR SHARE`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO refs (repo, name, commit_sha, generation) VALUES ($1, 'inflight', $2, nextval('ref_generation'))`, repoText, commitA); err != nil {
		t.Fatal(err)
	}
	done := make(chan reply, 1)
	go func() { done <- e.claim("ws1", 2, repoText) }()
	select {
	case r := <-done:
		t.Fatalf("claim finished while a write held the row: %d %v", r.status, r.body)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	e.want(<-done, 200, "")
	e.want(e.call("GET", "/v1/vcs/refs/inflight", e.readGrant(repoText), nil), 200, "")
	e.want(e.call("PUT", "/v1/vcs/refs/late", e.nodeGrant("ws1", 1), map[string]any{"expect": 0, "commit": commitA}), 409, "vcs_claim_lost")
}

func TestRefGenerationIsACompareAndSwapThatNeverRepeats(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	tok := e.nodeGrant("ws1", 1)
	put := func(expect int64, commit string) reply {
		return e.call("PUT", "/v1/vcs/refs/feature/x", tok, map[string]any{"expect": expect, "commit": commit})
	}
	gen := func(r reply) int64 { v, _ := r.body["generation"].(float64); return int64(v) }
	first := put(0, commitA)
	e.want(first, 200, "")
	for _, c := range []struct {
		name   string
		expect int64
	}{{"create over an existing ref", 0}, {"stale expect", gen(first) + 100}} {
		r := put(c.expect, commitB)
		e.want(r, 409, "vcs_generation_conflict")
		stored := r.body["error"].(map[string]any)["details"].(map[string]any)["ref"].(map[string]any)
		if stored["commit"] != commitA || int64(stored["generation"].(float64)) != gen(first) {
			t.Errorf("%s: conflict carries %v, want the stored ref", c.name, stored)
		}
	}
	second := put(gen(first), commitB)
	e.want(second, 200, "")
	if gen(second) <= gen(first) {
		t.Errorf("generation after a swap = %d, want above %d", gen(second), gen(first))
	}
	e.want(e.call("DELETE", fmt.Sprintf("/v1/vcs/refs/feature/x?expect=%d", gen(first)), tok, nil), 409, "vcs_generation_conflict")
	if r := e.call("DELETE", fmt.Sprintf("/v1/vcs/refs/feature/x?expect=%d", gen(second)), tok, nil); r.status != 204 {
		t.Fatalf("delete = %d", r.status)
	}
	if third := put(0, commitA); gen(third) <= gen(second) {
		t.Errorf("generation after delete and create = %d, want above %d", gen(third), gen(second))
	}
}

func TestImportedPackServesObjects(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	pack, idx, names := gitpack.Build([]gitpack.Object{{Type: gitpack.Blob, Data: []byte("hello")}})
	sum := sha256.Sum256(pack)
	id := hex.EncodeToString(sum[:])
	body := map[string]any{"index": base64.StdEncoding.EncodeToString(idx), "pack": base64.StdEncoding.EncodeToString(pack)}
	e.want(e.call("PUT", "/v1/vcs/packs/"+strings.Repeat("0", 64), e.importGrant(repoText), body), 400, "vcs_object_invalid")
	if r := e.call("PUT", "/v1/vcs/packs/"+id, e.importGrant(repoText), body); r.status != 204 {
		t.Fatalf("import = %d %v", r.status, r.body)
	}
	sha := names[0].String()
	for name, tok := range map[string]string{"node": e.nodeGrant("ws1", 1), "read": e.readGrant(repoText)} {
		r := e.call("GET", "/v1/vcs/objects/"+sha, tok, nil)
		if want := base64.StdEncoding.EncodeToString([]byte("hello")); r.status != 200 || r.body["data"] != want || r.body["type"] != "blob" {
			t.Errorf("%s grant read = %d %v", name, r.status, r.body)
		}
	}
	r := e.call("POST", "/v1/vcs/objects/has", e.importGrant(repoText), map[string]any{"shas": []string{sha, commitA}})
	if p, _ := r.body["present"].([]any); len(p) != 2 || p[0] != true || p[1] != false {
		t.Errorf("has = %v", r.body)
	}
	e.want(e.call("GET", "/v1/vcs/objects/"+sha, e.readGrant("gh:acme/other"), nil), 404, "vcs_not_found")
}

func TestDeletedWorkspaceRefusesReadsAndWrites(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	tok := e.nodeGrant("ws1", 1)
	for range 2 {
		if r := e.call("DELETE", "/v1/workspaces/ws1", e.operator(), nil); r.status != 204 {
			t.Fatalf("delete = %d", r.status)
		}
	}
	e.want(e.call("GET", "/v1/vcs/refs", tok, nil), 401, "unauthorized")
	e.want(e.call("PUT", "/v1/vcs/refs/x", tok, map[string]any{"expect": 0, "commit": commitA}), 401, "unauthorized")
	e.want(e.claim("ws1", 2, repoText), 410, "vcs_workspace_deleted")
}

func TestCreateRepoStartsOnARootCommitOfMain(t *testing.T) {
	e := newEnv(t)
	create := func(name string) reply {
		return e.call("POST", "/v1/repos", e.operator(), map[string]any{"name": name, "owner": "o1"})
	}
	for _, name := range []string{"", "   ", strings.Repeat("a", 101), "bad\x07name"} {
		e.want(create(name), 400, "invalid_body")
	}
	r := create("  widgets  ")
	e.want(r, 201, "")
	path := r.body["path"].(string)
	if !strings.HasPrefix(path, "repo_") {
		t.Fatalf("path = %q", path)
	}
	e.want(create("second"), 201, "")
	ref := e.call("GET", "/v1/vcs/refs/main", e.readGrant("native:"+path), nil)
	e.want(ref, 200, "")
	commit := ref.body["commit"].(string)
	c := e.call("GET", "/v1/vcs/commits/"+commit, e.readGrant("native:"+path), nil)
	if emptyTree := gitpack.Hash(gitpack.Tree, nil).String(); c.status != 200 || c.body["tree"] != emptyTree {
		t.Errorf("root commit = %d %v, want the empty tree %s", c.status, c.body, emptyTree)
	}
	list := e.call("GET", "/v1/repos", e.operator(), nil)
	repos, _ := list.body["repos"].([]any)
	if len(repos) != 2 || repos[1].(map[string]any)["name"] != "widgets" {
		t.Errorf("list = %v, want newest first with the trimmed name last", list.body)
	}
	e.want(e.call("GET", "/v1/repos/"+path, e.operator(), nil), 200, "")
	e.want(e.call("GET", "/v1/repos/repo_missing", e.operator(), nil), 404, "vcs_not_found")
}

func TestClaimBindsOwnerAndRepoOnce(t *testing.T) {
	e := newEnv(t)
	path := e.call("POST", "/v1/repos", e.operator(), map[string]any{"name": "w", "owner": "o1"}).body["path"].(string)
	native := "native:" + path
	e.want(e.claim("ws1", 1, native), 200, "")
	e.want(e.call("PUT", "/v1/workspaces/ws2/claim", e.operator(), map[string]any{"epoch": 1, "owner": "o2", "repo": native}), 403, "vcs_repo_forbidden")
	e.want(e.call("PUT", "/v1/workspaces/ws1/claim", e.operator(), map[string]any{"epoch": 1, "owner": "o2", "repo": native}), 403, "vcs_repo_forbidden")
	e.want(e.claim("ws3", 1, "native:repo_unknown"), 403, "vcs_repo_forbidden")
	e.want(e.claim("ws4", 1, repoText), 200, "")
	e.want(e.claim("ws4", 1, "gh:acme/other"), 409, "vcs_workspace_conflict")
}

func TestFlushOrdersByGenerationUnderTheEpochFence(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	flush := func(epoch uint64, gen int, prev, op string) reply {
		return e.call("POST", "/v1/workspaces/ws1/flush", e.operator(), map[string]any{"epoch": epoch, "generation": gen, "prev": prev, "op": op, "snapshot": commitA})
	}
	opA, opB, opC := strings.Repeat("a", 16), strings.Repeat("b", 16), strings.Repeat("c", 16)
	e.want(flush(1, 5, "", opA), 204, "")
	e.want(flush(1, 3, "", opB), 204, "")
	e.want(flush(1, 0, opB, opC), 204, "")
	if r := e.claim("ws1", 1, repoText); r.body["op"] != opA || r.body["snapshot"] != commitA {
		t.Errorf("claim after flushes = %v, want op %s: the lower generation and the mismatched prev are dropped", r.body, opA)
	}
	e.want(flush(1, 0, opA, opC), 204, "")
	if r := e.claim("ws1", 1, repoText); r.body["op"] != opC {
		t.Errorf("claim after a matching prev = %v, want op %s", r.body, opC)
	}
	e.want(e.claim("ws1", 2, repoText), 200, "")
	e.want(flush(1, 6, "", opB), 409, "vcs_claim_lost")
}

func TestGrantScopeRefusals(t *testing.T) {
	e := newEnv(t)
	e.want(e.claim("ws1", 1, repoText), 200, "")
	node, read, imp, op := e.nodeGrant("ws1", 1), e.readGrant(repoText), e.importGrant(repoText), e.operator()
	nodeOther := e.mint(e.node, grant.Claims{Scope: grant.ScopeNode, WS: "ws1", Epoch: 1, Repo: "gh:acme/other"})
	cpNode := e.mint(e.cp, grant.Claims{Scope: grant.ScopeNode, WS: "ws1", Epoch: 1, Repo: repoText})
	nodeKeyOperator := func() string {
		tok, err := e.node.Mint(grant.Claims{Scope: grant.ScopeOperator, Exp: e.expire.Unix()})
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

// The routes check the epoch before a write; this reaches the write statements
// as a request that passed that check and then lost the race would.
func TestWriteStatementsRefuseAStaleEpochOnTheirOwn(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	db := &pg.DB{Pool: e.pool}
	e.want(e.claim("ws1", 2, repoText), 200, "")
	ref, err := db.SwapRef(ctx, "ws1", 2, repoText, "keep", 0, commitA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SwapOpHeads(ctx, "ws1", 2, repoText, 0, []string{"aaaaaaaaaaaaaaaa"}); err != nil {
		t.Fatal(err)
	}
	writes := map[string]func() error{
		"ref create": func() error { _, err := db.SwapRef(ctx, "ws1", 1, repoText, "new", 0, commitA); return err },
		"ref swap": func() error {
			_, err := db.SwapRef(ctx, "ws1", 1, repoText, "keep", ref.Generation, commitB)
			return err
		},
		"ref delete": func() error { return db.DeleteRef(ctx, "ws1", 1, repoText, "keep", ref.Generation) },
		"op heads": func() error {
			_, err := db.SwapOpHeads(ctx, "ws1", 1, repoText, 1, []string{"bbbbbbbbbbbbbbbb"})
			return err
		},
		"first heads": func() error {
			_, err := db.SwapOpHeads(ctx, "ws1", 1, repoText, 0, []string{"bbbbbbbbbbbbbbbb"})
			return err
		},
		"report": func() error { return db.Report(ctx, "ws1", 1, repoText, 1, "", "aaaaaaaaaaaaaaaa", commitA) },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			if err := write(); !errors.Is(err, pg.ErrClaimLost) {
				t.Errorf("error = %v, want pg.ErrClaimLost, not a generation conflict", err)
			}
		})
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
