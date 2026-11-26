package identity_test

import (
	"testing"

	"github.com/majorcontext/motte/identity"
)

func TestMatchGivesTheHighestScopeToTheRightPrincipals(t *testing.T) {
	t.Parallel()
	rules := identity.Rules{
		{Principals: []string{"reader@example.test", "@Dev.test", "UserA"}, Repos: []string{"r"}, Scope: identity.ScopeRead},
		{Principals: []string{"@dev.test"}, Repos: []string{"r"}, Scope: identity.ScopeWrite},
		{Principals: []string{"admin@example.test"}, Scope: identity.ScopeAdmin},
	}
	email := func(e string, verified bool) identity.Principal {
		return identity.Principal{Subject: e, Email: e, EmailVerified: verified}
	}
	for _, c := range []struct {
		name  string
		p     identity.Principal
		repo  string
		scope string
	}{
		{"email without case", email("READER@example.test", true), "r", identity.ScopeRead},
		{"domain and the higher rule", email("dev@DEV.test", true), "r", identity.ScopeWrite},
		{"unverified email", email("dev@dev.test", false), "r", ""},
		{"another repository", email("dev@dev.test", true), "s", ""},
		{"admin names no repository", email("admin@example.test", true), "", identity.ScopeAdmin},
		{"admin on a repository", email("admin@example.test", true), "r", ""},
		{"a reader on no repository", email("reader@example.test", true), "", ""},
		{"opaque subject exactly", identity.Principal{Subject: "UserA"}, "r", identity.ScopeRead},
		{"opaque subject by case", identity.Principal{Subject: "usera"}, "r", ""},
		{"domain is not an opaque subject", identity.Principal{Subject: "x@dev.test"}, "r", ""},
		{"domain text as subject", identity.Principal{Subject: "@dev.test"}, "r", ""},
	} {
		if scope, ok := rules.Match(c.p, c.repo); scope != c.scope || ok != (c.scope != "") {
			t.Errorf("%s: scope = %q, ok = %v, want %q", c.name, scope, ok, c.scope)
		}
	}
}

func TestValidateAdmitsOnlyReadWriteAndAdmin(t *testing.T) {
	t.Parallel()
	ok := func(scope string, repos ...string) error {
		return identity.Rules{{Principals: []string{"a@b.test"}, Repos: repos, Scope: scope}}.Validate()
	}
	if ok(identity.ScopeRead, "r") != nil || ok(identity.ScopeWrite, "r") != nil || ok(identity.ScopeAdmin) != nil {
		t.Error("a valid rule was refused")
	}
	for _, bad := range []error{ok(identity.ScopeAdmin, "r"), ok(identity.ScopeRead), ok("merge", "r"), ok("operator", "r")} {
		if bad == nil {
			t.Error("an invalid rule was accepted")
		}
	}
}
