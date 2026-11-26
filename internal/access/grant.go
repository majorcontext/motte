package access

import (
	"net/http"
	"time"

	"github.com/majorcontext/motte/grant"
)

// Grant verifies the grant that Token reads from the request.
type Grant struct {
	Verifier *grant.Verifier
	Now      func() time.Time
	Token    func(*http.Request) string
}

func (a Grant) Authenticate(r *http.Request, _ string) (grant.Claims, error) {
	return a.Verifier.Verify(a.Token(r), a.Now())
}
