package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/oidc"
	"github.com/majorcontext/motte/internal/bucket/s3"
	"github.com/majorcontext/motte/internal/store"
)

func loadS3(getenv func(string) string, c *config) error {
	c.s3 = s3.Options{Endpoint: getenv("MOTTE_S3_ENDPOINT"), Region: getenv("MOTTE_S3_REGION")}
	style := getenv("MOTTE_S3_PATH_STYLE")
	switch style {
	case "true":
		c.s3.PathStyle = true
	case "", "false":
	default:
		return errors.New("MOTTE_S3_PATH_STYLE must be true or false")
	}
	if c.bucketKind != "s3" && (c.s3 != s3.Options{} || style != "") {
		return errors.New("MOTTE_S3_* variables need MOTTE_BUCKET_KIND=s3")
	}
	return nil
}

type config struct {
	listen       string
	meta         string
	databaseURL  string
	sqlitePath   string
	bucketKind   string
	bucket       string
	s3           s3.Options
	keys         []grant.Key
	otlp         bool
	retention    time.Duration
	oidc         *oidc.Config
	oidcRules    identity.Rules
	tlsCert      string
	tlsKey       string
	compaction   bool
	compactEvery time.Duration
	compactGrace time.Duration
	pprof        string
	// hedge starts a duplicate of a slow read; the gcs backend only.
	hedge bool
	// warnings name each variable that is set and that this release ignores or reads differently.
	warnings []string
	// rangeCacheMiB is the memory of the range cache in MiB; 0 turns it off.
	rangeCacheMiB int
}

// loadConfig reads the configuration from the environment.
//
//	MOTTE_LISTEN        listen address, default :8080
//	MOTTE_META          postgres (default) or sqlite
//	MOTTE_DATABASE_URL  PostgreSQL DSN, for postgres
//	MOTTE_SQLITE_PATH   database file, for sqlite
//	MOTTE_BUCKET_KIND   gcs, s3 or fs
//	MOTTE_BUCKET        bucket name for gcs and s3, directory for fs
//	MOTTE_S3_ENDPOINT   optional, s3 only: base URL of an S3-compatible service
//	MOTTE_S3_REGION     s3 only: bucket region, default from the AWS SDK chain
//	MOTTE_S3_PATH_STYLE optional, s3 only: true or false, default false
//	MOTTE_TRUSTED_KEYS  JSON list of {"kid","key","scopes"}; key is the base64 Ed25519 public key
//	MOTTE_OIDC_ISSUER   optional; enables OIDC identities on the git routes: the exact iss claim
//	MOTTE_OIDC_AUDIENCE the aud claim; required with the issuer
//	MOTTE_OIDC_RULES    JSON list of {"principals","repos","scope"}; scope is read or write, or admin with no repos; required with the issuer
//	MOTTE_OIDC_KEYS_URL optional; the address of the key set, default the jwks_uri of the discovery document of the issuer
//	MOTTE_OIDC_HEADER   optional; the header that carries the JWT, default Authorization
//	MOTTE_OIDC_CLAIM    optional; the claim that the rules match, default email
//	MOTTE_OIDC_ALGS     optional; comma-separated algorithms, default RS256
//	MOTTE_TLS_CERT      optional; PEM certificate chain; serves TLS with MOTTE_TLS_KEY
//	MOTTE_TLS_KEY       optional; PEM private key
//	MOTTE_EVENT_RETENTION  how long a ref event is kept, a Go duration, default 336h
//	MOTTE_COMPACTION       false (default) or true; true runs pack compaction, layer merges and file deletion on this replica
//	MOTTE_COMPACTION_INTERVAL  wait between maintenance passes, a Go duration, default 30s
//	MOTTE_COMPACTION_GRACE     how long a retired file stays before deletion, a Go duration, default 1h
//	MOTTE_RANGE_CACHE_MIB      memory of the cache of pack ranges that requests share, in MiB, default 256; 0 turns it off
//	MOTTE_PPROF                optional; address of a listener that serves /debug/pprof with the mutex and block profiles, apart from MOTTE_LISTEN
//	MOTTE_GCS_HEDGE            gcs only: on (default) or off; on starts a duplicate of a slow range read
//	OTEL_EXPORTER_OTLP_ENDPOINT  optional; enables trace export
func loadConfig(getenv func(string) string) (config, error) {
	c := config{
		listen:      getenv("MOTTE_LISTEN"),
		meta:        getenv("MOTTE_META"),
		databaseURL: getenv("MOTTE_DATABASE_URL"),
		sqlitePath:  getenv("MOTTE_SQLITE_PATH"),
		bucketKind:  getenv("MOTTE_BUCKET_KIND"),
		bucket:      getenv("MOTTE_BUCKET"),
		otlp:        getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "",
	}
	if c.listen == "" {
		c.listen = ":8080"
	}
	if c.meta == "" {
		c.meta = "postgres"
	}
	mb, ok := metaBackendOf(c.meta)
	switch {
	case !ok:
		return config{}, errors.New("MOTTE_META must be postgres or sqlite")
	case getenv(mb.required) == "":
		return config{}, errors.New(mb.required + " is required")
	case c.bucket == "":
		return config{}, errors.New("MOTTE_BUCKET is required")
	}
	if _, ok := bucketBackendOf(c.bucketKind); !ok {
		return config{}, errors.New("MOTTE_BUCKET_KIND must be gcs, s3 or fs")
	}
	if err := loadS3(getenv, &c); err != nil {
		return config{}, err
	}
	if text := getenv("MOTTE_EVENT_RETENTION"); text != "" {
		d, err := time.ParseDuration(text)
		if err != nil || d <= 0 {
			return config{}, errors.New("MOTTE_EVENT_RETENTION must be a positive duration")
		}
		c.retention = d
	}
	switch getenv("MOTTE_COMPACTION") {
	case "", "false":
	case "true":
		c.compaction = true
	default:
		return config{}, errors.New("MOTTE_COMPACTION must be true or false")
	}
	for _, v := range []struct {
		name string
		to   *time.Duration
	}{{"MOTTE_COMPACTION_INTERVAL", &c.compactEvery}, {"MOTTE_COMPACTION_GRACE", &c.compactGrace}} {
		if text := getenv(v.name); text != "" {
			d, err := time.ParseDuration(text)
			if err != nil || d <= 0 {
				return config{}, errors.New(v.name + " must be a positive duration")
			}
			*v.to = d
		}
	}
	c.rangeCacheMiB = store.DefaultRangeCacheBytes >> 20
	if text := getenv("MOTTE_RANGE_CACHE_MIB"); text != "" {
		n, err := strconv.Atoi(text)
		if err != nil || n < 0 || n > 4096 {
			return config{}, errors.New("MOTTE_RANGE_CACHE_MIB must be a number of MiB from 0 to 4096")
		}
		c.rangeCacheMiB = n
	}
	if c.pprof = getenv("MOTTE_PPROF"); c.pprof != "" && c.pprof == c.listen {
		return config{}, errors.New("MOTTE_PPROF needs an address other than MOTTE_LISTEN")
	}
	for _, b := range bucketBackends {
		for _, v := range b.vars {
			if b.kind != c.bucketKind && getenv(v) != "" {
				return config{}, errors.New(v + " needs MOTTE_BUCKET_KIND=" + b.kind)
			}
		}
	}
	hedge, err := loadHedge(getenv, &c)
	if err != nil {
		return config{}, err
	}
	c.hedge = hedge
	for _, r := range removedVariables {
		if getenv(r.name) != "" {
			c.warnings = append(c.warnings, r.name+" is no longer read: "+r.use)
		}
	}
	keys, err := parseKeys(getenv("MOTTE_TRUSTED_KEYS"))
	if err != nil {
		return config{}, err
	}
	c.keys = keys
	if c.oidc, c.oidcRules, err = loadOIDC(getenv); err != nil {
		return config{}, err
	}
	c.tlsCert, c.tlsKey = getenv("MOTTE_TLS_CERT"), getenv("MOTTE_TLS_KEY")
	if (c.tlsCert == "") != (c.tlsKey == "") {
		return config{}, errors.New("MOTTE_TLS_CERT and MOTTE_TLS_KEY go together")
	}
	return c, nil
}

// removedVariables are the variables of earlier releases that nothing reads now.
var removedVariables = []struct{ name, use string }{
	{"MOTTE_GCS_CLIENTS", "the pool of 8 clients is fixed, and MOTTE_GCS_HEDGE is on or off"},
	{"MOTTE_GCS_HEDGE_MIN", "the hedge delays are fixed; MOTTE_GCS_HEDGE is on or off"},
	{"MOTTE_GCS_HEDGE_MAX", "the hedge delays are fixed; MOTTE_GCS_HEDGE is on or off"},
	{"MOTTE_PPROF_CONTENTION", "MOTTE_PPROF always serves the mutex and block profiles"},
}

// loadHedge reads MOTTE_GCS_HEDGE. Hedging is on by default. A number from an earlier release, a percent, is read as
// off for 0 and as on for the rest, with a warning, and never stops the server.
func loadHedge(getenv func(string) string, c *config) (bool, error) {
	text := getenv("MOTTE_GCS_HEDGE")
	switch text {
	case "", "on":
		return true, nil
	case "off":
		return false, nil
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		return false, errors.New("MOTTE_GCS_HEDGE must be on or off")
	}
	c.warnings = append(c.warnings, "MOTTE_GCS_HEDGE is no longer a percent: use on or off; "+text+" is read as "+map[bool]string{true: "on", false: "off"}[n != 0])
	return n != 0, nil
}

func loadOIDC(getenv func(string) string) (*oidc.Config, identity.Rules, error) {
	issuer := getenv("MOTTE_OIDC_ISSUER")
	if issuer == "" {
		for _, k := range []string{"AUDIENCE", "RULES", "KEYS_URL", "HEADER", "CLAIM", "ALGS"} {
			if getenv("MOTTE_OIDC_"+k) != "" {
				return nil, nil, errors.New("MOTTE_OIDC_* variables need MOTTE_OIDC_ISSUER")
			}
		}
		return nil, nil, nil
	}
	cfg := &oidc.Config{Issuer: issuer, Audience: getenv("MOTTE_OIDC_AUDIENCE"), KeysURL: getenv("MOTTE_OIDC_KEYS_URL"),
		Header: getenv("MOTTE_OIDC_HEADER"), Claim: getenv("MOTTE_OIDC_CLAIM")}
	if algs := getenv("MOTTE_OIDC_ALGS"); algs != "" {
		cfg.Algs = strings.Split(algs, ",")
	}
	var rules identity.Rules
	if err := json.Unmarshal([]byte(getenv("MOTTE_OIDC_RULES")), &rules); err != nil || len(rules) == 0 {
		return nil, nil, errors.New("MOTTE_OIDC_RULES must be a non-empty JSON list")
	}
	if err := rules.Validate(); err != nil {
		return nil, nil, fmt.Errorf("MOTTE_OIDC_RULES: %w", err)
	}
	if _, err := oidc.New(*cfg); err != nil {
		return nil, nil, err
	}
	return cfg, rules, nil
}

func parseKeys(text string) ([]grant.Key, error) {
	var raw []struct {
		Kid    string        `json:"kid"`
		Key    string        `json:"key"`
		Scopes []grant.Scope `json:"scopes"`
	}
	if err := json.Unmarshal([]byte(text), &raw); err != nil || len(raw) == 0 {
		return nil, errors.New("MOTTE_TRUSTED_KEYS must be a non-empty JSON list")
	}
	keys := make([]grant.Key, len(raw))
	for i, k := range raw {
		pub, err := base64.StdEncoding.DecodeString(k.Key)
		if err != nil || len(pub) != ed25519.PublicKeySize || k.Kid == "" || len(k.Scopes) == 0 {
			return nil, fmt.Errorf("MOTTE_TRUSTED_KEYS entry %d is not a kid, an Ed25519 public key and scopes", i)
		}
		for _, sc := range k.Scopes {
			if !sc.Valid() {
				return nil, fmt.Errorf("MOTTE_TRUSTED_KEYS entry %d has the unknown scope %q", i, sc)
			}
		}
		keys[i] = grant.Key{Kid: k.Kid, Public: pub, Scopes: k.Scopes}
	}
	return keys, nil
}

func getenv(k string) string { return os.Getenv(k) }
