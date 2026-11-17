// Package grant signs and verifies the bearer grants of the store.
package grant

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

// ErrGrant is the one error of a rejected grant. It never says why.
var ErrGrant = errors.New("grant: not accepted")

type Scope string

const (
	ScopeNode     Scope = "node"
	ScopeRead     Scope = "read"
	ScopeWrite    Scope = "write"
	ScopeImport   Scope = "import"
	ScopeOperator Scope = "operator"
	ScopeMerge    Scope = "merge"
)

// Valid reports whether s is one of the six scopes.
func (s Scope) Valid() bool {
	switch s {
	case ScopeNode, ScopeRead, ScopeWrite, ScopeImport, ScopeOperator, ScopeMerge:
		return true
	}
	return false
}

const prefix = "mg1."

// Claims name what a bearer may do. Epoch is the claim epoch of the workspace
// when the grant was minted. Subject is for traces and grants nothing.
type Claims struct {
	Kid     string `json:"kid"`
	Scope   Scope  `json:"scope"`
	WS      string `json:"ws,omitempty"`
	Epoch   uint64 `json:"epoch,omitempty"`
	Repo    string `json:"repo,omitempty"`
	Owner   string `json:"owner,omitempty"`
	Subject string `json:"subject,omitempty"`
	Exp     int64  `json:"exp"`
}

func (c Claims) valid() bool {
	switch c.Scope {
	case ScopeNode:
		return c.WS != "" && c.Epoch != 0 && c.Repo != ""
	case ScopeRead, ScopeWrite, ScopeImport, ScopeMerge:
		return c.Repo != ""
	case ScopeOperator:
		return true
	}
	return false
}

// Signer mints grants with an Ed25519 private key.
type Signer struct {
	Kid string
	Key ed25519.PrivateKey
}

// Mint signs c under the kid of the signer.
func (s Signer) Mint(c Claims) (string, error) {
	c.Kid = s.Kid
	if !c.valid() {
		return "", errors.New("grant: claims are incomplete for the scope")
	}
	body, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(body)
	sig := ed25519.Sign(s.Key, []byte(payload))
	return prefix + payload + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// Key is a public key and the scopes that it may sign.
type Key struct {
	Kid    string
	Public ed25519.PublicKey
	Scopes []Scope
}

// Verifier accepts grants signed by one of its keys.
type Verifier struct {
	keys map[string]Key
}

func NewVerifier(keys ...Key) *Verifier {
	v := &Verifier{keys: make(map[string]Key, len(keys))}
	for _, k := range keys {
		v.keys[k.Kid] = k
	}
	return v
}

// Verify returns the claims of token when a known key signed it, the scope is
// allowed for that key, the claims are complete and now is before Exp.
func (v *Verifier) Verify(token string, now time.Time) (Claims, error) {
	rest, ok := strings.CutPrefix(token, prefix)
	payload, sigText, found := strings.Cut(rest, ".")
	if !ok || !found {
		return Claims{}, ErrGrant
	}
	body, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Claims{}, ErrGrant
	}
	var c Claims
	if json.Unmarshal(body, &c) != nil {
		return Claims{}, ErrGrant
	}
	key, ok := v.keys[c.Kid]
	sig, err := base64.RawURLEncoding.DecodeString(sigText)
	if !ok || err != nil || len(key.Public) != ed25519.PublicKeySize || !ed25519.Verify(key.Public, []byte(payload), sig) {
		return Claims{}, ErrGrant
	}
	if !slices.Contains(key.Scopes, c.Scope) || now.Unix() >= c.Exp || !c.valid() {
		return Claims{}, ErrGrant
	}
	return c, nil
}
