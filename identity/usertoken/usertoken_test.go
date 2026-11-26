package usertoken_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/identitytest"
	"github.com/majorcontext/motte/identity/usertoken"
)

var _ identity.Verifier = usertoken.Verifier{}

type memStore map[string]usertoken.Record

func (m memStore) TokenByHash(_ context.Context, hash string) (usertoken.Record, error) {
	if t, ok := m[hash]; ok {
		return t, nil
	}
	return usertoken.Record{}, errors.New("unknown token")
}

func TestConformance(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_000_000, 0)
	identitytest.Run(t, func(t *testing.T) identitytest.Fixture {
		store := memStore{}
		issue := func(exp time.Time, revoked bool) *http.Request {
			tok, _, hash, err := usertoken.New()
			if err != nil {
				t.Fatal(err)
			}
			rec := usertoken.Record{Principal: "dev@example.test", Scope: identity.ScopeRead, Repos: []string{"native:repo_A"}, ExpiresAt: exp}
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
			Verifier: usertoken.Verifier{Store: store, Now: func() time.Time { return now }, Token: func(r *http.Request) string { _, p, _ := r.BasicAuth(); return p }},
			Subject:  "dev@example.test",
			Valid:    func() *http.Request { return issue(now.Add(time.Hour), false) },
			Expired:  func() *http.Request { return issue(now.Add(-time.Second), false) },
			Revoked:  func() *http.Request { return issue(time.Time{}, true) },
			Forged:   func() *http.Request { return forged }}
	})
}
