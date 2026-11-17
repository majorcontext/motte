package client

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/majorcontext/motte/api"
)

var (
	ErrNotFound           = errors.New("motte: not found")
	ErrGenerationConflict = errors.New("motte: generation conflict")
	ErrClaimLost          = errors.New("motte: claim lost")
	ErrObjectInvalid      = errors.New("motte: object invalid")
	ErrObjectTooLarge     = errors.New("motte: object too large")
	ErrRepoForbidden      = errors.New("motte: repository forbidden")
	ErrWorkspaceConflict  = errors.New("motte: workspace conflict")
	ErrWorkspaceDeleted   = errors.New("motte: workspace deleted")
	ErrRefProtected       = errors.New("motte: ref protected")
	ErrEventsPruned       = errors.New("motte: events pruned")
	ErrUnauthorized       = errors.New("motte: unauthorized")
	ErrInvalidBody        = errors.New("motte: invalid body")
	ErrInternal           = errors.New("motte: internal error")
	// ErrRedirect is the error of a 3xx answer. The client follows no redirect, so a grant never leaves the base URL.
	ErrRedirect = errors.New("motte: unexpected redirect")
	// ErrUnexpected is the error of a failure answer that carries no wire error body.
	ErrUnexpected = errors.New("motte: unexpected response")
)

var sentinels = map[api.Code]error{
	api.CodeNotFound:           ErrNotFound,
	api.CodeGenerationConflict: ErrGenerationConflict,
	api.CodeClaimLost:          ErrClaimLost,
	api.CodeObjectInvalid:      ErrObjectInvalid,
	api.CodeObjectTooLarge:     ErrObjectTooLarge,
	api.CodeRepoForbidden:      ErrRepoForbidden,
	api.CodeWorkspaceConflict:  ErrWorkspaceConflict,
	api.CodeWorkspaceDeleted:   ErrWorkspaceDeleted,
	api.CodeRefProtected:       ErrRefProtected,
	api.CodeEventsPruned:       ErrEventsPruned,
	api.CodeUnauthorized:       ErrUnauthorized,
	api.CodeInvalidBody:        ErrInvalidBody,
	api.CodeInternal:           ErrInternal,
}

// Error is a failure answer of the store. errors.Is matches the sentinel of its code.
type Error struct {
	Status  int
	Code    api.Code
	Message string
	Details map[string]any
	cause   error
}

func (e *Error) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("motte: status %d", e.Status)
	}
	return fmt.Sprintf("motte: status %d: %s: %s", e.Status, e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

// CurrentRef returns the stored ref that a ref generation conflict carries.
func CurrentRef(err error) (api.Ref, bool) {
	var e *Error
	if !errors.As(err, &e) {
		return api.Ref{}, false
	}
	m, ok := e.Details["ref"].(map[string]any)
	if !ok {
		return api.Ref{}, false
	}
	name, _ := m["name"].(string)
	commit, _ := m["commit"].(string)
	gen, _ := m["generation"].(json.Number)
	n, err := gen.Int64()
	return api.Ref{Name: name, Commit: commit, Generation: n}, err == nil
}
