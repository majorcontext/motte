package cfaccess_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity"
	"github.com/majorcontext/motte/internal/identity/cfaccess"
	"github.com/majorcontext/motte/internal/identity/identitytest"
)

var _ identity.Authenticator = (*cfaccess.Authenticator)(nil)

const (
	team = "team.example.test"
	aud  = "aud-tag"
)

var now = time.Unix(1_000_000, 0)

func claims(email string, exp time.Time) map[string]any {
	return map[string]any{"iss": "https://" + team, "aud": []string{aud}, "email": email, "exp": exp.Unix(), "nbf": now.Add(-time.Minute).Unix()}
}

func newAuth(t *testing.T, iss *identitytest.Issuer, rules ...cfaccess.Rule) *cfaccess.Authenticator {
	t.Helper()
	a, err := cfaccess.New(cfaccess.Config{TeamDomain: team, Audience: aud, KeysURL: iss.URL, Rules: rules, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func request(jwt string) *http.Request {
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	r.Header.Set(cfaccess.Header, jwt)
	return r
}

func TestConformance(t *testing.T) {
	t.Parallel()
	identitytest.Run(t, func(t *testing.T) identitytest.Fixture {
		iss, rogue := identitytest.NewIssuer(t, "k1"), identitytest.NewIssuer(t, "k1")
		auth := newAuth(t, iss,
			cfaccess.Rule{Principals: []string{"reader@example.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeRead},
			cfaccess.Rule{Principals: []string{"@Dev.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite})
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
	a := newAuth(t, key, cfaccess.Rule{Principals: []string{"dev@example.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite})
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
	a, err := cfaccess.New(cfaccess.Config{TeamDomain: team, Audience: aud, KeysURL: key.URL, Now: func() time.Time { return now },
		Rules: []cfaccess.Rule{{Principals: []string{"admin@example.test"}, Scope: cfaccess.ScopeAdmin}}})
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
	if _, err := cfaccess.New(cfaccess.Config{TeamDomain: team, Audience: aud, Rules: []cfaccess.Rule{{Principals: []string{"a@b.test"}, Repos: []string{"native:r"}, Scope: cfaccess.ScopeAdmin}}}); err == nil {
		t.Error("an administrator rule with repos was accepted")
	}
}

func TestRulesNeverGiveMergeOrOperator(t *testing.T) {
	t.Parallel()
	for _, s := range []grant.Scope{grant.ScopeMerge, grant.ScopeOperator, grant.ScopeNode, grant.ScopeImport} {
		_, err := cfaccess.New(cfaccess.Config{TeamDomain: team, Audience: aud, Rules: []cfaccess.Rule{{Principals: []string{"a@b.test"}, Repos: []string{"native:r"}, Scope: s}}})
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
	a, err := cfaccess.New(cfaccess.Config{TeamDomain: team, Audience: aud, KeysURL: iss.URL, Client: &http.Client{Transport: keys}, Now: func() time.Time { return at },
		Rules: []cfaccess.Rule{{Principals: []string{"dev@example.test"}, Repos: []string{"native:repo_A"}, Scope: grant.ScopeWrite}}})
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
