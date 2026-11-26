// Package identitytest holds the conformance suite of identity.Verifier and a signer of Access JWTs.
package identitytest

import (
	"net/http"
	"testing"

	"github.com/majorcontext/motte/identity"
)

// Fixture is a Verifier and the requests that it must judge. Each request func returns a new request.
type Fixture struct {
	Verifier identity.Verifier
	// Subject is the subject of the principal that Valid gives.
	Subject string
	// Valid carries a credential that the Verifier accepts.
	Valid func() *http.Request
	// Expired carries a credential that has expired.
	Expired func() *http.Request
	// Forged carries a credential that is signed by a key that the Verifier does not trust.
	Forged func() *http.Request
	// Revoked carries a credential that was revoked. It is nil for a credential that cannot be revoked.
	Revoked func() *http.Request
}

// Run runs the suite.
func Run(t *testing.T, newFixture func(t *testing.T) Fixture) {
	t.Helper()
	refuse := func(t *testing.T, f Fixture, r *http.Request) {
		t.Helper()
		if p, err := f.Verifier.Verify(r); err == nil {
			t.Fatalf("principal = %+v, want an error", p)
		}
	}
	t.Run("accepts a valid credential", func(t *testing.T) {
		f := newFixture(t)
		p, err := f.Verifier.Verify(f.Valid())
		if err != nil || p.Subject != f.Subject || p.Exp.IsZero() {
			t.Errorf("principal = %+v, err = %v, want subject %q and an end", p, err, f.Subject)
		}
	})
	t.Run("refuses a request with no credential", func(t *testing.T) {
		f := newFixture(t)
		r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
		refuse(t, f, r)
	})
	t.Run("refuses an expired credential", func(t *testing.T) {
		f := newFixture(t)
		refuse(t, f, f.Expired())
	})
	t.Run("refuses a revoked credential", func(t *testing.T) {
		f := newFixture(t)
		if f.Revoked == nil {
			t.Skip("the credential cannot be revoked")
		}
		refuse(t, f, f.Revoked())
	})
	t.Run("refuses a forged credential", func(t *testing.T) {
		f := newFixture(t)
		refuse(t, f, f.Forged())
	})
}
