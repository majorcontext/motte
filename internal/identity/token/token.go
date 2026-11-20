// Package token authenticates a request by a signed grant.
package token

import (
	"net/http"
	"time"

	"github.com/majorcontext/motte/grant"
)

// Authenticator verifies the grant that Token reads from the request.
type Authenticator struct {
	Verifier *grant.Verifier
	Now      func() time.Time
	Token    func(*http.Request) string
}

func (a Authenticator) Authenticate(r *http.Request, _ string) (grant.Claims, error) {
	return a.Verifier.Verify(a.Token(r), a.Now())
}
