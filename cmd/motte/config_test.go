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
	gcsClients := map[string]int{"postgres by default": 8, "gcs clients set": 3}
	hedge := map[string]float64{"postgres by default": 0.05, "hedge set": 0.1, "hedge off": 0}
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
		"gcs clients set":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_CLIENTS": "3"}, true},
		"no gcs clients":       {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_CLIENTS": "0"}, false},
		"gcs clients need gcs": {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_GCS_CLIENTS": "2"}, false},
		"hedge set":            {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "10", "MOTTE_GCS_HEDGE_MAX": "1s"}, true},
		"hedge off":            {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "0"}, true},
		"hedge too much":       {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE": "80"}, false},
		"hedge min above max":  {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "gcs", "MOTTE_BUCKET": "b", "MOTTE_GCS_HEDGE_MIN": "5s"}, false},
		"hedge needs gcs":      {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_GCS_HEDGE": "5"}, false},
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
		if want, ok := gcsClients[name]; ok && cfg.gcsClients != want {
			t.Errorf("%s: gcs clients = %d, want %d", name, cfg.gcsClients, want)
		}
		if want, ok := hedge[name]; ok && cfg.hedge.Ratio != want {
			t.Errorf("%s: hedge ratio = %v, want %v", name, cfg.hedge.Ratio, want)
		}
	}
}
