package server_test

import (
	"context"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity/cfaccess"
	"github.com/majorcontext/motte/internal/identity/identitytest"
	"github.com/majorcontext/motte/internal/server"
	"github.com/majorcontext/motte/testserver"
)

func TestGitAccessIdentityPushesBranchesAndNeverProtectedRefs(t *testing.T) {
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
	ts := testserver.StartLocalWith(t, func(c *server.Config) {
		a, err := cfaccess.New(cfaccess.Config{TeamDomain: "team.example.test", Audience: "aud1", KeysURL: iss.URL, Rules: []cfaccess.Rule{
			{Principals: []string{"dev@example.test"}, Repos: repo, Scope: grant.ScopeWrite},
			{Principals: []string{"reader@example.test"}, Repos: repo, Scope: grant.ScopeRead},
			{Principals: []string{"admin@example.test"}, Scope: cfaccess.ScopeAdmin}}})
		if err != nil {
			t.Fatal(err)
		}
		c.Access = a
	})
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
