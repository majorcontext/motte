package identity

import (
	"fmt"
	"slices"
	"strings"
)

// The scopes that a rule gives.
const (
	ScopeRead  = "read"
	ScopeWrite = "write"
	// ScopeAdmin lets its principal create a repository. It names no repository.
	ScopeAdmin = "admin"
)

// Rule gives Scope on Repos to Principals. A principal is a subject text, or @domain for every verified email of the
// domain.
type Rule struct {
	Principals []string `json:"principals"`
	Repos      []string `json:"repos"`
	Scope      string   `json:"scope"`
}

// Rules is an ordered list of rules.
type Rules []Rule

// ValidScope reports whether a human identity may hold scope on repos: read or write on at least one repository, or
// admin on none.
func ValidScope(scope string, repos []string) bool {
	return scope == ScopeAdmin && len(repos) == 0 || (scope == ScopeRead || scope == ScopeWrite) && len(repos) > 0
}

// Validate checks the rules.
func (rs Rules) Validate() error {
	for i, r := range rs {
		if !ValidScope(r.Scope, r.Repos) || len(r.Principals) == 0 {
			return fmt.Errorf("rule %d needs principals and the scope admin with no repos, or read or write with repos", i)
		}
	}
	return nil
}

// Match returns the highest scope that the rules give p on repo. repo is empty for a call that names no repository,
// and only a rule of scope admin answers it. The subject matches exactly. A verified email matches without case, and
// a @domain principal matches the domain of a verified email.
func (rs Rules) Match(p Principal, repo string) (scope string, ok bool) {
	email, domain := "", ""
	if i := strings.LastIndex(p.Email, "@"); p.EmailVerified && i >= 0 {
		email, domain = strings.ToLower(p.Email), strings.ToLower(p.Email[i:])
	}
	for _, r := range rs {
		if (repo == "") != (r.Scope == ScopeAdmin) || repo != "" && !slices.Contains(r.Repos, repo) ||
			!slices.ContainsFunc(r.Principals, func(pr string) bool {
				if pr == p.Subject && p.Subject != "" && !strings.HasPrefix(pr, "@") {
					return true
				}
				pr = strings.ToLower(pr)
				return email != "" && (pr == email || pr == domain)
			}) {
			continue
		}
		if !ok || r.Scope == ScopeWrite {
			scope, ok = r.Scope, true
		}
	}
	return scope, ok
}
