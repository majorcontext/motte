// Package identity verifies who sent a request and matches that person against access rules. It knows no grant and no
// repository store: each service turns a principal and a scope into its own claims.
package identity

import (
	"errors"
	"net/http"
	"time"
)

// Principal is a verified person or service.
type Principal struct {
	// Subject is the text that rules match exactly.
	Subject string
	// Email is the email claim. Rules match it without case, and only when EmailVerified is true.
	Email         string
	EmailVerified bool
	// Exp is the end of the credential.
	Exp time.Time
}

// Verifier reads the credential of r. A credential that is missing, forged, expired or revoked answers an error and
// no principal.
type Verifier interface {
	Verify(r *http.Request) (Principal, error)
}

// ErrNoPrincipal is the answer of Chain when no verifier gives a principal.
var ErrNoPrincipal = errors.New("identity: no verifier accepts the credential")

type chain []Verifier

// Chain returns a Verifier that answers with the first principal that one of vs gives.
func Chain(vs ...Verifier) Verifier {
	var live chain
	for _, v := range vs {
		if v != nil {
			live = append(live, v)
		}
	}
	return live
}

func (c chain) Verify(r *http.Request) (Principal, error) {
	for _, v := range c {
		if p, err := v.Verify(r); err == nil {
			return p, nil
		}
	}
	return Principal{}, ErrNoPrincipal
}
