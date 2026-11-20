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
		"access on":            {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_ACCESS_TEAM_DOMAIN": "t.example", "MOTTE_ACCESS_AUD": "a", "MOTTE_ACCESS_RULES": `[{"principals":["a@b"],"repos":["native:r"],"scope":"write"}]`}, true},
		"access never merges":  {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_ACCESS_TEAM_DOMAIN": "t.example", "MOTTE_ACCESS_AUD": "a", "MOTTE_ACCESS_RULES": `[{"principals":["a@b"],"repos":["native:r"],"scope":"merge"}]`}, false},
		"access needs a team":  {map[string]string{"MOTTE_DATABASE_URL": "x", "MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d", "MOTTE_ACCESS_AUD": "a"}, false},
		"postgres needs a DSN": {map[string]string{"MOTTE_BUCKET_KIND": "fs", "MOTTE_BUCKET": "d"}, false},
	} {
		c.env["MOTTE_TRUSTED_KEYS"] = keys
		_, err := loadConfig(func(k string) string { return c.env[k] })
		if (err == nil) != c.ok {
			t.Errorf("%s: error = %v, want ok = %v", name, err, c.ok)
		}
	}
}
