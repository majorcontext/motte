// Package repos holds the rules of a repository that the store keeps.
package repos

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/majorcontext/motte/internal/gitpack"
)

// Prefix is the repository prefix of a repository that the store keeps.
const Prefix = "native"

// DefaultBranch is the ref that creation points at the root commit.
const DefaultBranch = "main"

const maxNameLen = 100

var ErrInvalidName = errors.New("repos: name must be 1 to 100 bytes with no control character")

// NewPath mints the path of a repository: repo_ and random base32 text.
func NewPath() string { return "repo_" + rand.Text() }

// Text returns the repository text of a path.
func Text(path string) string { return Prefix + ":" + path }

// ValidName trims name and applies the name rule: 1 to 100 bytes, no control character.
func ValidName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxNameLen || strings.ContainsFunc(name, unicode.IsControl) {
		return "", ErrInvalidName
	}
	return name, nil
}

// RootObjects returns the empty tree and a parentless commit of it, with the
// fixed author motte and at as the time.
func RootObjects(at time.Time) []gitpack.Object {
	tree := gitpack.Hash(gitpack.Tree, nil)
	who := fmt.Sprintf("motte <motte@motte.invalid> %d +0000", at.Unix())
	commit := fmt.Sprintf("tree %s\nauthor %s\ncommitter %s\n\nInitial commit\n", tree, who, who)
	return []gitpack.Object{{Type: gitpack.Tree}, {Type: gitpack.Commit, Data: []byte(commit)}}
}
