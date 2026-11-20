package token_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity"
	"github.com/majorcontext/motte/internal/identity/identitytest"
	"github.com/majorcontext/motte/internal/identity/token"
)

var _ identity.Authenticator = token.Authenticator{}

func TestConformance(t *testing.T) {
	t.Parallel()
	identitytest.Run(t, func(t *testing.T) identitytest.Fixture {
		pub, priv, _ := ed25519.GenerateKey(rand.Reader)
		_, rogue, _ := ed25519.GenerateKey(rand.Reader)
		now := time.Unix(1_000_000, 0)
		scopes := []grant.Scope{grant.ScopeRead, grant.ScopeWrite}
		auth := token.Authenticator{Verifier: grant.NewVerifier(grant.Key{Kid: "k", Public: pub, Scopes: scopes}), Now: func() time.Time { return now },
			Token: func(r *http.Request) string { return r.Header.Get("X-Grant") }}
		req := func(key ed25519.PrivateKey, scope grant.Scope, repo string, exp time.Time) *http.Request {
			tok, err := grant.Signer{Kid: "k", Key: key}.Mint(grant.Claims{Scope: scope, Repo: repo, Exp: exp.Unix()})
			if err != nil {
				t.Fatal(err)
			}
			r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
			r.Header.Set("X-Grant", tok)
			return r
		}
		return identitytest.Fixture{Auth: auth,
			Valid:   func(s grant.Scope, repo string) *http.Request { return req(priv, s, repo, now.Add(time.Hour)) },
			Expired: func(repo string) *http.Request { return req(priv, grant.ScopeRead, repo, now.Add(-time.Second)) },
			Forged:  func(repo string) *http.Request { return req(rogue, grant.ScopeWrite, repo, now.Add(time.Hour)) }}
	})
}
