// Package cfaccess authenticates a request by the signed JWT that Cloudflare Access adds in the header
// Cf-Access-Jwt-Assertion, and maps the email of the user to a scope on a repository with an allowlist.
package cfaccess

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/majorcontext/motte/grant"
)

// Header is the header that carries the JWT to the origin.
const Header = "Cf-Access-Jwt-Assertion"

const (
	refreshAfter = time.Hour
	// unknownKidWait limits the fetches that a token with an unknown kid can cause.
	unknownKidWait = time.Minute
	keysMax        = 16 << 10
)

// ScopeAdmin is the scope of a rule that lets Principals create a repository. It names no repository, and an
// Authenticator answers it only for a call that names none, with the claims of scope operator.
const ScopeAdmin grant.Scope = "admin"

// Rule gives Scope on Repos to Principals. A principal is an email, or @domain for every email of the domain.
type Rule struct {
	Principals []string
	Repos      []string
	Scope      grant.Scope
}

// Config is the deployment of one Access application.
type Config struct {
	// TeamDomain is the host name of the Access team. The key set is at https://<TeamDomain>/cdn-cgi/access/certs
	// unless KeysURL is set, and the issuer is https://<TeamDomain>.
	TeamDomain string
	// Audience is the AUD tag of the application.
	Audience string
	KeysURL  string
	Rules    []Rule
	Client   *http.Client
	Now      func() time.Time
}

// Authenticator verifies the JWT and applies the rules.
type Authenticator struct {
	cfg Config

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	fetched time.Time
	good    time.Time
}

// New checks cfg. A rule may give only read, write or admin: an Access identity never mints a grant, and never merges.
func New(cfg Config) (*Authenticator, error) {
	if cfg.TeamDomain == "" || cfg.Audience == "" {
		return nil, errors.New("cfaccess: the team domain and the audience are required")
	}
	for i, r := range cfg.Rules {
		ok := r.Scope == ScopeAdmin && len(r.Repos) == 0 || (r.Scope == grant.ScopeRead || r.Scope == grant.ScopeWrite) && len(r.Repos) > 0
		if !ok || len(r.Principals) == 0 {
			return nil, fmt.Errorf("cfaccess: rule %d needs principals and the scope admin with no repos, or read or write with repos", i)
		}
	}
	if cfg.KeysURL == "" {
		cfg.KeysURL = "https://" + cfg.TeamDomain + "/cdn-cgi/access/certs"
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Authenticator{cfg: cfg}, nil
}

type jwtClaims struct {
	Iss   string          `json:"iss"`
	Aud   json.RawMessage `json:"aud"`
	Exp   int64           `json:"exp"`
	Nbf   int64           `json:"nbf"`
	Email string          `json:"email"`
}

func (c jwtClaims) audiences() []string {
	var one string
	if json.Unmarshal(c.Aud, &one) == nil {
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(c.Aud, &many)
	return many
}

func (a *Authenticator) Authenticate(r *http.Request, repo string) (grant.Claims, error) {
	jc, err := a.verify(r.Header.Get(Header))
	if err != nil {
		return grant.Claims{}, grant.ErrGrant
	}
	scope := a.scopeFor(jc.Email, repo)
	switch scope {
	case "":
		return grant.Claims{}, grant.ErrGrant
	case ScopeAdmin:
		scope = grant.ScopeOperator
	}
	return grant.Claims{Scope: scope, Repo: repo, Subject: jc.Email, Exp: jc.Exp}, nil
}

func (a *Authenticator) scopeFor(email, repo string) grant.Scope {
	email = strings.ToLower(email)
	domain := email[strings.LastIndex(email, "@"):]
	var best grant.Scope
	for _, r := range a.cfg.Rules {
		if (repo == "") != (r.Scope == ScopeAdmin) || !slices.Contains(r.Repos, repo) && repo != "" || !slices.ContainsFunc(r.Principals, func(p string) bool { p = strings.ToLower(p); return p == email || p == domain }) {
			continue
		}
		if best == "" || r.Scope == grant.ScopeWrite {
			best = r.Scope
		}
	}
	return best
}

func (a *Authenticator) verify(token string) (jwtClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return jwtClaims{}, errors.New("not a JWT")
	}
	var head struct{ Alg, Kid string }
	var jc jwtClaims
	if decode(parts[0], &head) != nil || decode(parts[1], &jc) != nil || head.Alg != "RS256" {
		return jwtClaims{}, errors.New("bad JWT")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	key, err2 := a.key(head.Kid)
	if err != nil || err2 != nil {
		return jwtClaims{}, errors.New("bad signature or key")
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig) != nil {
		return jwtClaims{}, errors.New("signature mismatch")
	}
	now := a.cfg.Now().Unix()
	if jc.Iss != "https://"+a.cfg.TeamDomain || !slices.Contains(jc.audiences(), a.cfg.Audience) ||
		jc.Exp == 0 || now >= jc.Exp || now < jc.Nbf || !strings.Contains(jc.Email, "@") {
		return jwtClaims{}, errors.New("claims refused")
	}
	return jc, nil
}

func decode(part string, v any) error {
	raw, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// key returns the public key of kid. It fetches the key set when the cache is older than an hour or kid is unknown,
// at most once a minute, and it refuses a cache that an hour has passed since the last good fetch.
func (a *Authenticator) key(kid string) (*rsa.PublicKey, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.cfg.Now()
	k, known := a.keys[kid]
	if (now.Sub(a.good) >= refreshAfter || !known) && (a.fetched.IsZero() || now.Sub(a.fetched) >= unknownKidWait) {
		_ = a.fetch(now)
		k, known = a.keys[kid]
	}
	if !known || now.Sub(a.good) >= refreshAfter {
		return nil, errors.New("unknown or stale key")
	}
	return k, nil
}

func (a *Authenticator) fetch(now time.Time) error {
	a.fetched = now
	resp, err := a.cfg.Client.Get(a.cfg.KeysURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("key set: status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct{ Kid, Kty, N, E string } `json:"keys"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, keysMax)).Decode(&set); err != nil {
		return err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		n, errN := base64.RawURLEncoding.DecodeString(k.N)
		e, errE := base64.RawURLEncoding.DecodeString(k.E)
		if k.Kty != "RSA" || k.Kid == "" || errN != nil || errE != nil || len(e) > 4 {
			continue
		}
		exp := new(big.Int).SetBytes(e)
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(exp.Int64())}
	}
	if len(keys) == 0 {
		return errors.New("key set: no RSA key")
	}
	a.keys, a.good = keys, now
	return nil
}
