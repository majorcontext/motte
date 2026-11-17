package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/majorcontext/motte/grant"
)

type config struct {
	listen      string
	databaseURL string
	bucketKind  string
	bucket      string
	keys        []grant.Key
	otlp        bool
}

// loadConfig reads the configuration from the environment.
//
//	MOTTE_LISTEN        listen address, default :8080
//	MOTTE_DATABASE_URL  PostgreSQL DSN
//	MOTTE_BUCKET_KIND   gcs or fs
//	MOTTE_BUCKET        bucket name for gcs, directory for fs
//	MOTTE_TRUSTED_KEYS  JSON list of {"kid","key","scopes"}; key is the base64 Ed25519 public key
//	OTEL_EXPORTER_OTLP_ENDPOINT  optional; enables trace export
func loadConfig(getenv func(string) string) (config, error) {
	c := config{
		listen:      getenv("MOTTE_LISTEN"),
		databaseURL: getenv("MOTTE_DATABASE_URL"),
		bucketKind:  getenv("MOTTE_BUCKET_KIND"),
		bucket:      getenv("MOTTE_BUCKET"),
		otlp:        getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "",
	}
	if c.listen == "" {
		c.listen = ":8080"
	}
	if c.databaseURL == "" || c.bucket == "" {
		return config{}, errors.New("MOTTE_DATABASE_URL and MOTTE_BUCKET are required")
	}
	if c.bucketKind != "gcs" && c.bucketKind != "fs" {
		return config{}, errors.New("MOTTE_BUCKET_KIND must be gcs or fs")
	}
	keys, err := parseKeys(getenv("MOTTE_TRUSTED_KEYS"))
	if err != nil {
		return config{}, err
	}
	c.keys = keys
	return c, nil
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
