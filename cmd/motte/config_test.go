package main

import (
	"strings"
	"testing"
)

func TestParseKeysRefusesAnUnknownScope(t *testing.T) {
	t.Parallel()
	key := strings.Repeat("A", 43) + "="
	for scopes, ok := range map[string]bool{`["node","operator"]`: true, `["read","write"]`: true, `["merge"]`: true, `["node","admin"]`: false, `["Node"]`: false} {
		_, err := parseKeys(`[{"kid":"k","key":"` + key + `","scopes":` + scopes + `}]`)
		if (err == nil) != ok {
			t.Errorf("scopes %s: error = %v, want ok = %v", scopes, err, ok)
		}
	}
}

func TestLoadConfigChoosesBackendsAndRefusesUnknownOnes(t *testing.T) {
	t.Parallel()
	keys := `[{"kid":"k","key":"` + strings.Repeat("A", 43) + `=","scopes":["node"]}]`
	rangeCacheMiB := map[string]int{"postgres by default": 256, "range cache set": 512}
	hedge := map[string]bool{"postgres by default": true, "hedge on": true, "hedge off": false, "hedge old percent": true, "hedge old zero": false}
	warned := map[string]int{"gcs clients set": 1, "hedge old percent": 1, "hedge old zero": 1, "hedge delays set": 2, "pprof contention set": 1}
	for name, c := range map[string]struct {
		env map[string]string
		ok  bool
	}{
		"postgres by default":  {map[string]string{"MOTTE_DATABASE_URL": "postgres://x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b"}, true},
		"sqlite and fs":        {map[string]string{"MOTTE_META": "sqlite", "MOTTE_SQLITE_PATH": "m.db", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d"}, true},
		"sqlite needs a path":  {map[string]string{"MOTTE_META": "sqlite", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d"}, false},
		"unknown meta":         {map[string]string{"MOTTE_META": "spanner", "MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d"}, false},
		"s3 with options":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "s3", "MOTTE_BUCKET": "d", "MOTTE_S3_ENDPOINT": "http://m:9000", "MOTTE_S3_PATH_STYLE": "true"}, true},
		"s3 variables need s3": {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "d", "MOTTE_S3_REGION": "auto"}, false},
		"bad path style":       {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "s3", "MOTTE_BUCKET": "d", "MOTTE_S3_PATH_STYLE": "yes"}, false},
		"unknown bucket kind":  {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "r2", "MOTTE_BUCKET": "d"}, false},
		"oidc on":              {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_OIDC_ISSUER": "https://t.example", "MOTTE_OIDC_AUDIENCE": "a", "MOTTE_OIDC_RULES": `[{"principals":["a@b"],"repos":["native:r"],"scope":"write"}]`}, true},
		"oidc never merges":    {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_OIDC_ISSUER": "https://t.example", "MOTTE_OIDC_AUDIENCE": "a", "MOTTE_OIDC_RULES": `[{"principals":["a@b"],"repos":["native:r"],"scope":"merge"}]`}, false},
		"oidc needs an issuer": {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_OIDC_AUDIENCE": "a"}, false},
		"tls needs both":       {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_TLS_CERT": "c.pem"}, false},
		"range cache set":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_RANGE_CACHE_MIB": "512"}, true},
		"range cache off":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_RANGE_CACHE_MIB": "0"}, true},
		"bad range cache size": {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_RANGE_CACHE_MIB": "-5"}, false},
		"gcs clients set":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_GCS_CLIENTS": "3"}, true},
		"hedge on":             {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "on"}, true},
		"hedge off":            {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "off"}, true},
		"hedge old percent":    {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "10"}, true},
		"hedge old zero":       {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "0"}, true},
		"hedge unknown word":   {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "maybe"}, false},
		"hedge delays set":     {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE_MIN": "5s", "MOTTE_GCS_HEDGE_MAX": "1s"}, true},
		"hedge needs gcs":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_GCS_HEDGE": "on"}, false},
		"pprof contention set": {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_PPROF_CONTENTION": "1"}, true},
		"postgres needs a DSN": {map[string]string{"MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d"}, false},
	} {
		c.env["MOTTE_TRUSTED_KEYS"] = keys
		cfg, err := loadConfig(func(k string) string { return c.env[k] })
		if (err == nil) != c.ok {
			t.Errorf("%s: error = %v, want ok = %v", name, err, c.ok)
		}
		if want, ok := rangeCacheMiB[name]; ok && cfg.rangeCacheMiB != want {
			t.Errorf("%s: range cache = %d MiB, want %d", name, cfg.rangeCacheMiB, want)
		}
		if want, ok := hedge[name]; ok && cfg.hedge != want {
			t.Errorf("%s: hedge = %v, want %v", name, cfg.hedge, want)
		}
		if len(cfg.warnings) != warned[name] {
			t.Errorf("%s: warnings = %q, want %d", name, cfg.warnings, warned[name])
		}
	}
}

func TestEachBucketVariableNeedsItsOwnKind(t *testing.T) {
	t.Parallel()
	keys := `[{"kid":"k","key":"` + strings.Repeat("A", 43) + `=","scopes":["node"]}]`
	for _, b := range bucketBackends {
		for _, v := range b.vars {
			for _, other := range bucketBackends {
				env := map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET": "d", "MOTTE_TRUSTED_KEYS": keys, "MOTTE_BUCKET_KIND": other.kind, v: "on"}
				if _, err := loadConfig(func(k string) string { return env[k] }); (err != nil) == (other.kind == b.kind) && v == "MOTTE_GCS_HEDGE" || other.kind != b.kind && err == nil {
					t.Errorf("%s with kind %s: error = %v", v, other.kind, err)
				}
			}
		}
	}
}

func TestBackendVariableErrorsKeepTheirMessages(t *testing.T) {
	t.Parallel()
	keys := `[{"kid":"k","key":"` + strings.Repeat("A", 43) + `=","scopes":["node"]}]`
	for want, extra := range map[string]map[string]string{
		"MOTTE_S3_PATH_STYLE must be true or false":      {"MOTTE_BUCKET_KIND": "fs", "MOTTE_S3_PATH_STYLE": "yes"},
		"MOTTE_S3_* variables need MOTTE_BUCKET_KIND=s3": {"MOTTE_BUCKET_KIND": "fs", "MOTTE_S3_REGION": "auto"},
		"MOTTE_GCS_HEDGE needs MOTTE_BUCKET_KIND=gcs":    {"MOTTE_BUCKET_KIND": "fs", "MOTTE_GCS_HEDGE": "on"},
		"MOTTE_BUCKET_KIND must be gcs, s3 or fs":        {"MOTTE_BUCKET_KIND": "r2"},
		"MOTTE_DATABASE_URL is required":                 {"MOTTE_BUCKET_KIND": "fs", "MOTTE_DATABASE_URL": ""},
		"MOTTE_META must be postgres or sqlite":          {"MOTTE_BUCKET_KIND": "fs", "MOTTE_META": "spanner"},
	} {
		env := map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET": "d", "MOTTE_TRUSTED_KEYS": keys}
		for k, v := range extra {
			env[k] = v
		}
		if _, err := loadConfig(func(k string) string { return env[k] }); err == nil || err.Error() != want {
			t.Errorf("error = %v, want %q", err, want)
		}
	}
}
