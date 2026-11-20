package identity

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/majorcontext/motte/grant"
)

// ScopeAdmin is the scope of a rule or a token that lets its principal create a repository. It names no repository,
// and an Authenticator answers it only for a call that names none, with the claims of scope operator.
const ScopeAdmin grant.Scope = "admin"

// Rule gives Scope on Repos to Principals. A principal is a subject text, or @domain for every email of the domain.
type Rule struct {
	Principals []string    `json:"principals"`
	Repos      []string    `json:"repos"`
	Scope      grant.Scope `json:"scope"`
}

// ValidScope reports whether a human identity may hold scope on repos: read or write on at least one repository, or
// admin on none. A human identity never holds merge, operator, import or node.
func ValidScope(scope grant.Scope, repos []string) bool {
	return scope == ScopeAdmin && len(repos) == 0 || (scope == grant.ScopeRead || scope == grant.ScopeWrite) && len(repos) > 0
}

// Validate checks the rules.
func Validate(rules []Rule) error {
	for i, r := range rules {
		if !ValidScope(r.Scope, r.Repos) || len(r.Principals) == 0 {
			return fmt.Errorf("rule %d needs principals and the scope admin with no repos, or read or write with repos", i)
		}
	}
	return nil
}

// ScopeFor returns the highest scope that the rules give subject on repo, or "" for none. repo is empty for a call
// that names no repository. A subject that holds an @ is an email and matches without case. Any other subject
// matches exactly.
func ScopeFor(rules []Rule, subject, repo string) grant.Scope {
	fold := func(v string) string { return v }
	domain := ""
	if i := strings.LastIndex(subject, "@"); i >= 0 {
		fold = strings.ToLower
		domain = fold(subject[i:])
	}
	subject = fold(subject)
	var best grant.Scope
	for _, r := range rules {
		if (repo == "") != (r.Scope == ScopeAdmin) || repo != "" && !slices.Contains(r.Repos, repo) ||
			!slices.ContainsFunc(r.Principals, func(p string) bool { p = fold(p); return p == subject || domain != "" && p == domain }) {
			continue
		}
		if best == "" || r.Scope == grant.ScopeWrite {
			best = r.Scope
		}
	}
	return best
}

// Claims is the claims of a human identity that holds scope on repo. ScopeAdmin becomes operator.
func Claims(scope grant.Scope, repo, subject string, exp int64) grant.Claims {
	if scope == ScopeAdmin {
		scope = grant.ScopeOperator
	}
	return grant.Claims{Scope: scope, Repo: repo, Subject: subject, Exp: exp}
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
