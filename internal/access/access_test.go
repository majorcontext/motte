package access_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/usertoken"
	"github.com/majorcontext/motte/internal/access"
)

type fixed identity.Principal

func (f fixed) Verify(*http.Request) (identity.Principal, error) { return identity.Principal(f), nil }

type store map[string]usertoken.Record

func (s store) TokenByHash(_ context.Context, hash string) (usertoken.Record, error) {
	if r, ok := s[hash]; ok {
		return r, nil
	}
	return usertoken.Record{}, errors.New("unknown")
}

func TestHumanTurnsAScopeIntoClaims(t *testing.T) {
	t.Parallel()
	exp := time.Unix(2_000_000, 0)
	h := access.Human{Verifier: fixed{Subject: "a@b.test", Email: "a@b.test", EmailVerified: true, Exp: exp}, Rules: identity.Rules{
		{Principals: []string{"a@b.test"}, Repos: []string{"r"}, Scope: identity.ScopeWrite},
		{Principals: []string{"a@b.test"}, Scope: identity.ScopeAdmin}}}
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	if c, err := h.Authenticate(r, "r"); err != nil || c.Scope != grant.ScopeWrite || c.Repo != "r" || c.Subject != "a@b.test" || c.Exp != exp.Unix() {
		t.Errorf("repository: claims = %+v, err = %v", c, err)
	}
	if c, err := h.Authenticate(r, ""); err != nil || c.Scope != grant.ScopeOperator || c.Repo != "" {
		t.Errorf("no repository: claims = %+v, err = %v, want operator", c, err)
	}
	if _, err := h.Authenticate(r, "other"); !errors.Is(err, grant.ErrGrant) {
		t.Errorf("another repository: err = %v, want grant.ErrGrant", err)
	}
}

func TestUserTokenAnswersOnlyItsRepositories(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	tok, _, hash, _ := usertoken.New()
	v := usertoken.Verifier{Now: func() time.Time { return now }, Token: func(*http.Request) string { return tok },
		Store: store{hash: {Principal: "p", Scope: identity.ScopeRead, Repos: []string{"r"}}}}
	r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
	u := access.UserToken{Verifier: v}
	if c, err := u.Authenticate(r, "r"); err != nil || c.Scope != grant.ScopeRead || c.Subject != "p" || c.Exp != now.Add(time.Hour).Unix() {
		t.Errorf("claims = %+v, err = %v", c, err)
	}
	for _, repo := range []string{"", "other"} {
		if _, err := u.Authenticate(r, repo); !errors.Is(err, grant.ErrGrant) {
			t.Errorf("repo %q: err = %v, want grant.ErrGrant", repo, err)
		}
	}
}
