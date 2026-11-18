package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/majorcontext/motte/bucket/s3"
	"github.com/majorcontext/motte/grant"
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
	listen      string
	meta        string
	databaseURL string
	sqlitePath  string
	bucketKind  string
	bucket      string
	s3          s3.Options
	keys        []grant.Key
	otlp        bool
	retention   time.Duration
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
//	MOTTE_EVENT_RETENTION  how long a ref event is kept, a Go duration, default 336h
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
	switch {
	case c.meta == "postgres" && c.databaseURL == "":
		return config{}, errors.New("MOTTE_DATABASE_URL is required")
	case c.meta == "sqlite" && c.sqlitePath == "":
		return config{}, errors.New("MOTTE_SQLITE_PATH is required")
	case c.meta != "postgres" && c.meta != "sqlite":
		return config{}, errors.New("MOTTE_META must be postgres or sqlite")
	case c.bucket == "":
		return config{}, errors.New("MOTTE_BUCKET is required")
	}
	if c.bucketKind != "gcs" && c.bucketKind != "s3" && c.bucketKind != "fs" {
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
