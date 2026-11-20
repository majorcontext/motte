// Package identitytest holds the conformance suite of identity.Authenticator and a signer of Access JWTs.
package identitytest

import (
	"errors"
	"net/http"
	"testing"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity"
)

// Fixture is an Authenticator and the requests that it must judge. Each request func returns a new request.
type Fixture struct {
	Auth identity.Authenticator
	// Valid carries a credential that gives scope on repo.
	Valid func(scope grant.Scope, repo string) *http.Request
	// Expired carries a credential that gives read on repo and has expired.
	Expired func(repo string) *http.Request
	// Forged carries a credential that gives write on repo and is signed by a key that the Authenticator does not trust.
	Forged func(repo string) *http.Request
	// Revoked carries a credential that gave write on repo and was revoked. It is nil for a credential that
	// cannot be revoked.
	Revoked func(repo string) *http.Request
}

const repoA, repoB = "native:repo_A", "native:repo_B"

// Run runs the suite. newFixture returns a fixture whose credentials cover repoA and not repoB: repoA is
// "native:repo_A" and repoB is "native:repo_B".
func Run(t *testing.T, newFixture func(t *testing.T) Fixture) {
	t.Helper()
	refuse := func(t *testing.T, f Fixture, r *http.Request, repo string) {
		t.Helper()
		if _, err := f.Auth.Authenticate(r, repo); !errors.Is(err, grant.ErrGrant) {
			t.Fatalf("error = %v, want grant.ErrGrant", err)
		}
	}
	t.Run("accepts a credential with its scope and repository", func(t *testing.T) {
		f := newFixture(t)
		for _, scope := range []grant.Scope{grant.ScopeRead, grant.ScopeWrite} {
			c, err := f.Auth.Authenticate(f.Valid(scope, repoA), repoA)
			if err != nil || c.Scope != scope || c.Repo != repoA {
				t.Errorf("%s: claims = %+v, err = %v, want scope %s on %s", scope, c, err, scope, repoA)
			}
		}
	})
	t.Run("refuses a request with no credential", func(t *testing.T) {
		f := newFixture(t)
		r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
		refuse(t, f, r, repoA)
	})
	t.Run("refuses an expired credential", func(t *testing.T) {
		f := newFixture(t)
		refuse(t, f, f.Expired(repoA), repoA)
	})
	t.Run("refuses a revoked credential", func(t *testing.T) {
		f := newFixture(t)
		if f.Revoked == nil {
			t.Skip("the credential cannot be revoked")
		}
		refuse(t, f, f.Revoked(repoA), repoA)
	})
	t.Run("refuses a forged credential", func(t *testing.T) {
		f := newFixture(t)
		refuse(t, f, f.Forged(repoA), repoA)
	})
	t.Run("never gives the claims of another repository", func(t *testing.T) {
		f := newFixture(t)
		if c, err := f.Auth.Authenticate(f.Valid(grant.ScopeWrite, repoA), repoB); err == nil && c.Repo == repoB {
			t.Errorf("claims = %+v for %s, want a refusal or the repository of the credential", c, repoB)
		}
	})
}
