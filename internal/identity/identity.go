// Package identity maps the credential of a request to the claims of a grant.
package identity

import (
	"net/http"

	"github.com/majorcontext/motte/grant"
)

// Authenticator reads the credential of r and returns the claims that it gives. repo is the repository text that the
// route names, empty for a route with none. A credential that is missing, forged, expired or not allowed for repo
// answers grant.ErrGrant and nothing else. The claims never hold a scope above what the credential gives.
type Authenticator interface {
	Authenticate(r *http.Request, repo string) (grant.Claims, error)
}
