// Package usertoken authenticates a request by a per-user token that an administrator issued. The service keeps only
// the hash of the token, so the identity needs no external provider.
package usertoken

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/identity"
	"github.com/majorcontext/motte/meta"
)

// Prefix starts every token, so that a credential of another kind never reaches the store.
const Prefix = "mtu_"

// Store is what the Authenticator reads.
type Store interface {
	UserTokenByHash(ctx context.Context, hash string) (meta.UserToken, error)
}

// Authenticator reads the token that Token returns, finds its record by hash, and refuses a token that is unknown,
// revoked or expired.
type Authenticator struct {
	Store Store
	Now   func() time.Time
	Token func(*http.Request) string
}

// New returns a token text, the ID of its record and the hash that the store keeps.
func New() (token, id, hash string, err error) {
	raw := make([]byte, 40)
	if _, err = rand.Read(raw); err != nil {
		return "", "", "", err
	}
	token = Prefix + base64.RawURLEncoding.EncodeToString(raw[8:])
	return token, "ut_" + hex.EncodeToString(raw[:8]), Hash(token), nil
}

// Hash is the lowercase hex SHA-256 of token.
func Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (a Authenticator) Authenticate(r *http.Request, repo string) (grant.Claims, error) {
	tok := a.Token(r)
	if !strings.HasPrefix(tok, Prefix) {
		return grant.Claims{}, grant.ErrGrant
	}
	rec, err := a.Store.UserTokenByHash(r.Context(), Hash(tok))
	now := a.Now()
	scope := grant.Scope(rec.Scope)
	if err != nil || !rec.RevokedAt.IsZero() || !rec.ExpiresAt.IsZero() && !now.Before(rec.ExpiresAt) || !identity.ValidScope(scope, rec.Repos) ||
		(repo == "") != (scope == identity.ScopeAdmin) || repo != "" && !slices.Contains(rec.Repos, repo) {
		return grant.Claims{}, grant.ErrGrant
	}
	exp := now.Add(time.Hour)
	if !rec.ExpiresAt.IsZero() {
		exp = rec.ExpiresAt
	}
	return identity.Claims(scope, repo, rec.Principal, exp.Unix()), nil
}
