package usertoken_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity"
	"github.com/majorcontext/motte/internal/identity/identitytest"
	"github.com/majorcontext/motte/internal/identity/usertoken"
	"github.com/majorcontext/motte/meta"
)

var _ identity.Authenticator = usertoken.Authenticator{}

type memStore map[string]meta.UserToken

func (m memStore) UserTokenByHash(_ context.Context, hash string) (meta.UserToken, error) {
	if t, ok := m[hash]; ok {
		return t, nil
	}
	return meta.UserToken{}, meta.ErrNotFound
}

func TestConformance(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	identitytest.Run(t, func(t *testing.T) identitytest.Fixture {
		store := memStore{}
		issue := func(scope grant.Scope, repo string, exp time.Time, revoked bool) *http.Request {
			tok, id, hash, err := usertoken.New()
			if err != nil {
				t.Fatal(err)
			}
			rec := meta.UserToken{ID: id, Principal: "dev@example.test", Scope: string(scope), Repos: []string{repo}, ExpiresAt: exp}
			if revoked {
				rec.RevokedAt = now
			}
			store[hash] = rec
			r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
			r.SetBasicAuth("x", tok)
			return r
		}
		forged, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
		forged.SetBasicAuth("x", usertoken.Prefix+"forged")
		return identitytest.Fixture{
			Auth:    usertoken.Authenticator{Store: store, Now: func() time.Time { return now }, Token: func(r *http.Request) string { _, p, _ := r.BasicAuth(); return p }},
			Valid:   func(s grant.Scope, repo string) *http.Request { return issue(s, repo, now.Add(time.Hour), false) },
			Expired: func(repo string) *http.Request { return issue(grant.ScopeRead, repo, now.Add(-time.Second), false) },
			Revoked: func(repo string) *http.Request { return issue(grant.ScopeWrite, repo, time.Time{}, true) },
			Forged:  func(string) *http.Request { return forged }}
	})
}
