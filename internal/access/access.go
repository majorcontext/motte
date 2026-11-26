// Package access turns the credential of a request into the claims of a grant.
package access

import (
	"net/http"
	"slices"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/usertoken"
)

// Authenticator reads the credential of r and returns the claims that it gives. repo is the repository text that the
// route names, empty for a route with none. A credential that is missing, forged, expired or not allowed for repo
// answers grant.ErrGrant and nothing else. The claims never hold a scope above what the credential gives.
type Authenticator interface {
	Authenticate(r *http.Request, repo string) (grant.Claims, error)
}

type chain []Authenticator

// Chain returns an Authenticator that answers with the first claims that one of auths gives, and grant.ErrGrant when
// none gives any.
func Chain(auths ...Authenticator) Authenticator {
	var live chain
	for _, a := range auths {
		if a != nil {
			live = append(live, a)
		}
	}
	return live
}

func (c chain) Authenticate(r *http.Request, repo string) (grant.Claims, error) {
	for _, a := range c {
		if cl, err := a.Authenticate(r, repo); err == nil {
			return cl, nil
		}
	}
	return grant.Claims{}, grant.ErrGrant
}

// Human gives the claims of the scope that Rules give the principal of Verifier on the repository.
type Human struct {
	Verifier identity.Verifier
	Rules    identity.Rules
}

func (h Human) Authenticate(r *http.Request, repo string) (grant.Claims, error) {
	p, err := h.Verifier.Verify(r)
	if err != nil {
		return grant.Claims{}, grant.ErrGrant
	}
	scope, ok := h.Rules.Match(p, repo)
	if !ok {
		return grant.Claims{}, grant.ErrGrant
	}
	return humanClaims(scope, repo, p.Subject, p.Exp), nil
}

// UserToken gives the claims of the scope and repositories that the record of a per-user token holds.
type UserToken struct{ Verifier usertoken.Verifier }

func (u UserToken) Authenticate(r *http.Request, repo string) (grant.Claims, error) {
	rec, err := u.Verifier.Lookup(r)
	if err != nil || (repo == "") != (rec.Scope == identity.ScopeAdmin) || repo != "" && !slices.Contains(rec.Repos, repo) {
		return grant.Claims{}, grant.ErrGrant
	}
	p := u.Verifier.Principal(rec)
	return humanClaims(rec.Scope, repo, p.Subject, p.Exp), nil
}

// humanClaims is the claims of a human identity that holds scope on repo. The scope admin becomes operator.
func humanClaims(scope, repo, subject string, exp time.Time) grant.Claims {
	if scope == identity.ScopeAdmin {
		scope = string(grant.ScopeOperator)
	}
	return grant.Claims{Scope: grant.Scope(scope), Repo: repo, Subject: subject, Exp: exp.Unix()}
}
