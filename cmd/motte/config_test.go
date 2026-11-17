package main

import (
	"strings"
	"testing"
)

func TestParseKeysRefusesAnUnknownScope(t *testing.T) {
	key := strings.Repeat("A", 43) + "="
	for scopes, ok := range map[string]bool{`["node","operator"]`: true, `["node","admin"]`: false, `["Node"]`: false} {
		_, err := parseKeys(`[{"kid":"k","key":"` + key + `","scopes":` + scopes + `}]`)
		if (err == nil) != ok {
			t.Errorf("scopes %s: error = %v, want ok = %v", scopes, err, ok)
		}
	}
}
