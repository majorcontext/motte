// Package gitproto speaks the git smart HTTP protocol over an object reader: ref advertisement, protocol v2 fetch
// and ls-refs, and the commands and report of receive-pack. It has no HTTP, grant or storage code.
package gitproto

import (
	"context"
	"errors"
	"strings"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/refname"
)

// Agent is the agent capability of both services.
const Agent = "motte"

// MaxTracked bounds the objects that one fetch holds in its sets.
const MaxTracked = 1000000

var ErrNotFound = errors.New("gitproto: object not found")

var ErrTooLarge = errors.New("gitproto: fetch has too many objects")

// RequestError is a request that the protocol refuses. Its text is safe to send to the client.
type RequestError string

func (e RequestError) Error() string { return string(e) }

// Objects is the object reader of one repository. Get answers ErrNotFound for an absent object.
type Objects interface {
	Get(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error)
	Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error)
}

// Ref is a ref under its git name.
type Ref struct {
	Name string
	SHA  gitpack.SHA
}

const headsPrefix = "refs/heads/"

// GitRefName is the git name of a stored ref. A stored name that does not start with refs/ is a branch. A stored
// name under refs/heads/ has no git name, because the git name would address the branch of the rest.
func GitRefName(stored string) (string, bool) {
	switch {
	case strings.HasPrefix(stored, headsPrefix):
		return "", false
	case strings.HasPrefix(stored, "refs/"):
		return stored, true
	}
	return headsPrefix + stored, true
}

// StoredRefName is the stored name of a git ref, or false when no push may write it.
func StoredRefName(git string) (string, bool) {
	name := git
	if rest, ok := strings.CutPrefix(git, headsPrefix); ok {
		name = rest
		if strings.HasPrefix(rest, "refs/") {
			return "", false
		}
	} else if !strings.HasPrefix(git, "refs/") {
		return "", false
	}
	return name, refname.Valid(name)
}

// Zero is the all-zero name that a ref command uses for an absent ref.
var Zero gitpack.SHA
