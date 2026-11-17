// Package refname holds the ref name rule of the store.
package refname

import (
	"strings"
	"unicode/utf8"
)

// Max is the longest ref name in bytes.
const Max = 255

// Valid applies the git ref-name rules that bind a name to one path.
func Valid(name string) bool {
	if name == "" || len(name) > Max || name == "@" || !utf8.ValidString(name) ||
		strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".") ||
		strings.Contains(name, "..") || strings.Contains(name, "//") || strings.Contains(name, "@{") {
		return false
	}
	for _, c := range name {
		if c < 0x20 || c == 0x7f || strings.ContainsRune(" ~^:?*[\\", c) {
			return false
		}
	}
	for _, part := range strings.Split(name, "/") {
		if strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

// ValidPattern reports whether p is a ref name, or a prefix that ends in "*".
func ValidPattern(p string) bool {
	prefix, star := strings.CutSuffix(p, "*")
	if !star {
		return Valid(p)
	}
	return prefix == "" || Valid(prefix+"x")
}

// Match reports whether name is p, or starts with the prefix of a pattern that ends in "*".
func Match(p, name string) bool {
	if prefix, star := strings.CutSuffix(p, "*"); star {
		return strings.HasPrefix(name, prefix)
	}
	return p == name
}
