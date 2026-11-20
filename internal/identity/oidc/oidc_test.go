package oidc_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity"
	"github.com/majorcontext/motte/internal/identity/identitytest"
	"github.com/majorcontext/motte/internal/identity/oidc"
)

var _ identity.Authenticator = (*oidc.Authenticator)(nil)

const (
	issuer = "https://team.example.test"
	aud    = "aud-tag"
	header = "Cf-Access-Jwt-Assertion"
)

var now = time.Unix(1_000_000, 0)

func claims(email string, exp time.Time) map[string]any {
	return map[string]any{"iss": issuer, "aud": []string{aud}, "email": email, "exp": exp.Unix(), "nbf": now.Add(-time.Minute).Unix()}
}

func newAuth(t *testing.T, iss *identitytest.Issuer, rules ...identity.Rule) *oidc.Authenticator {
	t.Helper()
	a, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, KeysURL: iss.URL, Rules: rules, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func request(jwt string) *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	r.Header.Set(header, jwt)
	return r
}

func TestConformance(t *testing.T) {
	t.Parallel()
	identitytest.Run(t, func(t *testing.T) identitytest.Fixture {
		iss, rogue := identitytest.NewIssuer(t, "k1"), identitytest.NewIssuer(t, "k1")
		auth := newAuth(t, iss,
			identity.Rule{Principals: []string{"reader@example.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeRead},
			identity.Rule{Principals: []string{"@Dev.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite})
		who := map[grant.Scope]string{grant.ScopeRead: "reader@example.test", grant.ScopeWrite: "dev@dev.test"}
		return identitytest.Fixture{Auth: auth,
			Valid: func(s grant.Scope, _ string) *http.Request {
				return request(iss.Sign(t, claims(who[s], now.Add(time.Hour))))
			},
			Expired: func(string) *http.Request { return request(iss.Sign(t, claims("dev@dev.test", now.Add(-time.Second)))) },
			Forged:  func(string) *http.Request { return request(rogue.Sign(t, claims("dev@dev.test", now.Add(time.Hour)))) }}
	})
}

func TestRefusesClaimsOfAnotherApplication(t *testing.T) {
	t.Parallel()
	key := identitytest.NewIssuer(t, "k1")
	a := newAuth(t, key, identity.Rule{Principals: []string{"dev@example.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite})
	for name, mutate := range map[string]func(map[string]any){
		"audience": func(c map[string]any) { c["aud"] = []string{"other"} },
		"issuer":   func(c map[string]any) { c["iss"] = "https://other.example.test" },
		"not yet":  func(c map[string]any) { c["nbf"] = now.Add(time.Minute).Unix() },
		"no email": func(c map[string]any) { delete(c, "email") },
		"merge":    func(c map[string]any) {},
	} {
		c := claims("dev@example.test", now.Add(time.Hour))
		mutate(c)
		_, err := a.Authenticate(request(key.Sign(t, c)), "native:repo_A")
		if (name == "merge") != (err == nil) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestAdminRuleNamesNoRepoAndAnswersOnlyACallThatNamesNone(t *testing.T) {
	t.Parallel()
	key := identitytest.NewIssuer(t, "k1")
	a, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, KeysURL: key.URL, Now: func() time.Time { return now },
		Rules: []identity.Rule{{Principals: []string{"admin@example.test"}, Scope: identity.ScopeAdmin}}})
	if err != nil {
		t.Fatal(err)
	}
	r := request(key.Sign(t, claims("admin@example.test", now.Add(time.Hour))))
	if c, err := a.Authenticate(r, ""); err != nil || c.Scope != grant.ScopeOperator {
		t.Errorf("no repo: claims = %+v, err = %v, want operator", c, err)
	}
	if _, err := a.Authenticate(r, "native:repo_A"); err == nil {
		t.Error("an administrator rule answered a call that names a repository")
	}
	if _, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, Rules: []identity.Rule{{Principals: []string{"a@b.test"}, Repos: []string{"native:r"}, Scope: identity.ScopeAdmin}}}); err == nil {
		t.Error("an administrator rule with repos was accepted")
	}
}

func TestRulesNeverGiveMergeOrOperator(t *testing.T) {
	t.Parallel()
	for _, s := range []grant.Scope{grant.ScopeMerge, grant.ScopeOperator, grant.ScopeNode, grant.ScopeImport} {
		_, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, Rules: []identity.Rule{{Principals: []string{"a@b.test"}, Repos: []string{"native:r"}, Scope: s}}})
		if err == nil {
			t.Errorf("scope %s accepted", s)
		}
	}
}

type failableKeys struct{ down bool }

func (f *failableKeys) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.down {
		return nil, errors.New("key set unreachable")
	}
	return http.DefaultTransport.RoundTrip(r)
}

func TestRefusesKeysAnHourPastTheirLastGoodFetch(t *testing.T) {
	t.Parallel()
	iss := identitytest.NewIssuer(t, "k1")
	keys := &failableKeys{}
	at := now
	a, err := oidc.New(oidc.Config{Issuer: issuer, Header: header, Audience: aud, KeysURL: iss.URL, Client: &http.Client{Transport: keys}, Now: func() time.Time { return at },
		Rules: []identity.Rule{{Principals: []string{"dev@example.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite}}})
	if err != nil {
		t.Fatal(err)
	}
	r := request(iss.Sign(t, claims("dev@example.test", now.Add(3*time.Hour))))
	check := func(wantOK bool, after time.Duration) {
		t.Helper()
		at = now.Add(after)
		if _, err := a.Authenticate(r, "native:repo_A"); (err == nil) != wantOK {
			t.Errorf("after %s with the key set down: err = %v, want ok = %v", after, err, wantOK)
		}
	}
	check(true, 0)
	keys.down = true
	check(true, 59*time.Minute)
	check(false, 61*time.Minute)
}

func TestDiscoversTheKeySetAndReadsABearerToken(t *testing.T) {
	t.Parallel()
	iss := identitytest.NewIssuer(t, "k1")
	doc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"jwks_uri": iss.URL})
	}))
	t.Cleanup(doc.Close)
	a, err := oidc.New(oidc.Config{Issuer: doc.URL, Audience: aud, Claim: "sub", Now: func() time.Time { return now },
		Rules: []identity.Rule{{Principals: []string{"user-1"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeRead}}})
	if err != nil {
		t.Fatal(err)
	}
	tok := iss.Sign(t, map[string]any{"iss": doc.URL, "aud": aud, "sub": "user-1", "exp": now.Add(time.Hour).Unix()})
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	if c, err := a.Authenticate(r, "native:repo_A"); err != nil || c.Subject != "user-1" {
		t.Errorf("bearer: claims = %+v, err = %v", c, err)
	}
	r.Header.Del("Authorization")
	r.SetBasicAuth("x", tok)
	if _, err := a.Authenticate(r, "native:repo_A"); err != nil {
		t.Errorf("basic password: err = %v", err)
	}
}

func TestOpaqueSubjectsMatchExactly(t *testing.T) {
	t.Parallel()
	rules := []identity.Rule{{Principals: []string{"UserA"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite}}
	if got := identity.ScopeFor(rules, "usera", "native:repo_A"); got != "" {
		t.Errorf("usera got %q from a rule for UserA", got)
	}
	if got := identity.ScopeFor(rules, "UserA", "native:repo_A"); got != grant.ScopeWrite {
		t.Errorf("UserA got %q", got)
	}
}
