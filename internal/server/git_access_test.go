package server_test

import (
	"context"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/identitytest"
	"github.com/majorcontext/motte/identity/oidc"
	"github.com/majorcontext/motte/testserver"
)

func TestGitOIDCIdentityPushesBranchesAndNeverProtectedRefs(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("the git binary is not installed")
	}
	t.Parallel()
	iss := identitytest.NewIssuer(t, "k1")
	header := func(email string) []string {
		jwt := iss.Sign(t, map[string]any{"iss": "https://team.example.test", "aud": "aud1", "email": email, "exp": time.Now().Add(time.Hour).Unix()})
		return []string{"-c", "http.extraHeader=Cf-Access-Jwt-Assertion: " + jwt}
	}
	repo := []string{"pending"}
	a, err := oidc.New(oidc.Config{Issuer: "https://team.example.test", Audience: "aud1", KeysURL: iss.URL, Header: "Cf-Access-Jwt-Assertion"})
	if err != nil {
		t.Fatal(err)
	}
	ts := testserver.Start(t, testserver.WithIdentity(a, identity.Rules{
		{Principals: []string{"dev@example.test"}, Repos: repo, Scope: identity.ScopeWrite},
		{Principals: []string{"reader@example.test"}, Repos: repo, Scope: identity.ScopeRead},
		{Principals: []string{"admin@example.test"}, Scope: identity.ScopeAdmin}}))
	e := &env{Server: ts, t: t, url: ts.URL, cp: ts.Operator, node: ts.Node}
	ctx := context.Background()
	path, err := e.Client(e.operator()).CreateRepo(ctx, "widgets", "o1")
	if err != nil {
		t.Fatal(err)
	}
	g := &gitEnv{env: e, path: path, text: "native:" + path}
	repo[0] = g.text
	if err := e.Client(e.operator()).SetProtectedRefs(ctx, g.text, []string{"main"}); err != nil {
		t.Fatal(err)
	}
	url := ts.URL + "/git/native/" + path + ".git"
	dir := t.TempDir()
	g.git(dir, true, append(header("dev@example.test"), "clone", "-q", url, "dev")...)
	w := dir + "/dev"
	g.commit(w, "w.txt", "w")
	g.git(w, true, append(header("dev@example.test"), "push", "-q", "origin", "HEAD:refs/heads/feature")...)
	if out := g.git(w, false, append(header("dev@example.test"), "push", "origin", "HEAD:refs/heads/main")...); !strings.Contains(out, "protected") {
		t.Errorf("push to main: %q, want a refusal that says protected", out)
	}
	g.git(dir, true, append(header("reader@example.test"), "clone", "-q", url, "reader")...)
	g.git(dir+"/reader", false, append(header("reader@example.test"), "push", "origin", "HEAD:refs/heads/other")...)
	g.git(dir, false, append(header("stranger@example.test"), "clone", "-q", url, "stranger")...)
	do := func(method, path, email string) int {
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(`{"name":"w","owner":"o"}`))
		req.Header.Set("Cf-Access-Jwt-Assertion", strings.TrimPrefix(header(email)[1], "http.extraHeader=Cf-Access-Jwt-Assertion: "))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	for _, c := range []struct {
		method, email string
		want          int
	}{{"POST", "admin@example.test", 201}, {"POST", "dev@example.test", 401}, {"GET", "admin@example.test", 401}} {
		if got := do(c.method, "/v1/repos", c.email); got != c.want {
			t.Errorf("%s /v1/repos as %s = %d, want %d: only an administrator creates, and no identity reads the control plane", c.method, c.email, got, c.want)
		}
	}
}

func TestGitUserTokenPushesBranchesAndNeverProtectedRefs(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("the git binary is not installed")
	}
	t.Parallel()
	ts := testserver.StartLocal(t)
	e := &env{Server: ts, t: t, url: ts.URL, cp: ts.Operator, node: ts.Node}
	ctx := context.Background()
	op := e.Client(e.operator())
	path, err := op.CreateRepo(ctx, "widgets", "o1")
	if err != nil {
		t.Fatal(err)
	}
	g := &gitEnv{env: e, path: path, text: "native:" + path}
	if err := op.SetProtectedRefs(ctx, g.text, []string{"main"}); err != nil {
		t.Fatal(err)
	}
	issue := func(scope string, repos ...string) api.CreateUserTokenResponse {
		out, err := op.CreateUserToken(ctx, api.CreateUserTokenRequest{Principal: "dev@example.test", Scope: scope, Repos: repos})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	dev, reader := issue("write", g.text), issue("read", g.text)
	url := ts.URL + "/git/native/" + path + ".git"
	dir := t.TempDir()
	withToken := func(tok string, args ...string) []string {
		return append([]string{"-c", "http.extraHeader=Authorization: Bearer " + tok}, args...)
	}
	g.git(dir, true, withToken(dev.Token, "clone", "-q", url, "dev")...)
	w := dir + "/dev"
	g.git(dir, true, "clone", "-q", strings.Replace(url, "http://", "http://user:"+reader.Token+"@", 1), "password")
	g.commit(w, "w.txt", "w")
	g.git(w, true, withToken(dev.Token, "push", "-q", "origin", "HEAD:refs/heads/feature")...)
	if out := g.git(w, false, withToken(dev.Token, "push", "origin", "HEAD:refs/heads/main")...); !strings.Contains(out, "protected") {
		t.Errorf("push to main: %q, want a refusal that says protected", out)
	}
	g.git(w, false, withToken(reader.Token, "push", "origin", "HEAD:refs/heads/other")...)
	g.git(dir, false, withToken(issue("write", "native:repo_other").Token, "clone", "-q", url, "elsewhere")...)
	status := func(method, path, tok string) int {
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(`{"principal":"a@b","scope":"admin"}`))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if got := status("POST", "/v1/tokens", issue("admin").Token); got != 401 {
		t.Errorf("an admin user token issued a token: status %d, want 401", got)
	}
	if err := op.RevokeUserToken(ctx, dev.UserToken.ID); err != nil {
		t.Fatal(err)
	}
	g.git(dir, false, withToken(dev.Token, "clone", "-q", url, "revoked")...)
}
