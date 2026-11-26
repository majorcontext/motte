// Package usertoken authenticates a request by a per-user token that an administrator issued. The service keeps only
// the hash of the token, so the identity needs no external provider.
package usertoken

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/majorcontext/motte/identity"
)

// Prefix starts every token, so that a credential of another kind never reaches the store.
const Prefix = "mtu_"

// Record is a stored token: who holds it, what it allows, and when it ends. Repos is empty for the scope admin.
type Record struct {
	Principal string
	Scope     string
	Repos     []string
	ExpiresAt time.Time
	RevokedAt time.Time
}

// Store finds the record of a token hash. It returns an error for an unknown hash.
type Store interface {
	TokenByHash(ctx context.Context, hash string) (Record, error)
}

// Verifier reads the token that Token returns, finds its record by hash, and refuses a token that is unknown, revoked
// or expired.
type Verifier struct {
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

// Lookup returns the record of the token of r, or an error for a token that is unknown, revoked, expired or of a
// scope that a person may not hold.
func (v Verifier) Lookup(r *http.Request) (Record, error) {
	tok := v.Token(r)
	if !strings.HasPrefix(tok, Prefix) {
		return Record{}, errors.New("usertoken: not a user token")
	}
	rec, err := v.Store.TokenByHash(r.Context(), Hash(tok))
	if err != nil || !rec.RevokedAt.IsZero() || !rec.ExpiresAt.IsZero() && !v.Now().Before(rec.ExpiresAt) ||
		!identity.ValidScope(rec.Scope, rec.Repos) {
		return Record{}, errors.New("usertoken: the token is refused")
	}
	return rec, nil
}

// Verify gives the principal of the token.
func (v Verifier) Verify(r *http.Request) (identity.Principal, error) {
	rec, err := v.Lookup(r)
	if err != nil {
		return identity.Principal{}, err
	}
	return v.Principal(rec), nil
}

// Principal is the principal of rec. A token with no end lasts an hour from now.
func (v Verifier) Principal(rec Record) identity.Principal {
	exp := v.Now().Add(time.Hour)
	if !rec.ExpiresAt.IsZero() {
		exp = rec.ExpiresAt
	}
	return identity.Principal{Subject: rec.Principal, Exp: exp}
}
