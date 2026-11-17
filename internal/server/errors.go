package server

import (
	"errors"
	"fmt"
	"net/http"
	"syscall"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/store"
)

func (s *Server) writeError(w http.ResponseWriter, code api.Code, msg string, details map[string]any) {
	if details == nil {
		details = map[string]any{}
	}
	s.writeJSON(w, code.Status(), api.ErrorBody{Error: api.Error{Code: code, Message: msg, Details: details}})
}

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	var refConflict *pg.RefConflict
	var opConflict *pg.OpHeadsConflict
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, pg.ErrNotFound):
		s.writeError(w, api.CodeNotFound, "not found", nil)
	case errors.Is(err, store.ErrInvalid):
		s.writeError(w, api.CodeObjectInvalid, "invalid object name or content", nil)
	case errors.Is(err, store.ErrTooLarge):
		s.writeError(w, api.CodeObjectTooLarge, "object too large", nil)
	case errors.Is(err, pg.ErrClaimLost):
		s.writeError(w, api.CodeClaimLost, "the claim epoch is not the live one", nil)
	case errors.Is(err, pg.ErrGone):
		s.writeError(w, api.CodeWorkspaceDeleted, "the workspace is deleted", nil)
	case errors.Is(err, pg.ErrWorkspaceConflict):
		s.writeError(w, api.CodeWorkspaceConflict, "the workspace has another owner or repository", nil)
	case errors.Is(err, errForbidden):
		s.writeError(w, api.CodeRepoForbidden, "the repository is not readable", nil)
	case errors.Is(err, errUnauthorized):
		s.writeError(w, api.CodeUnauthorized, "grant not accepted", nil)
	case errors.Is(err, errBody):
		s.writeError(w, api.CodeInvalidBody, "request body is not valid", nil)
	case errors.As(err, &refConflict):
		var d map[string]any
		if refConflict.Current != nil {
			d = map[string]any{"ref": refView(*refConflict.Current)}
		}
		s.writeError(w, api.CodeGenerationConflict, "ref generation conflict", d)
	case errors.As(err, &opConflict):
		s.writeError(w, api.CodeGenerationConflict, "op heads generation conflict", nil)
	default:
		s.logger.ErrorContext(r.Context(), "request failed", "route", r.Pattern, "error", logSafe(err))
		s.writeError(w, api.CodeInternal, "internal error", nil)
	}
}

var (
	errForbidden    = errors.New("server: repository forbidden")
	errBody         = errors.New("server: invalid body")
	errUnauthorized = errors.New("server: grant not accepted")
)

// logSafe names an error by its type chain and its innermost cause, which a log line may hold. The message of a
// wrapper can carry a path or a URL, so it stays out.
func logSafe(err error) string {
	root := err
	for u := errors.Unwrap(root); u != nil; u = errors.Unwrap(u) {
		root = u
	}
	if _, ok := root.(syscall.Errno); ok {
		return fmt.Sprintf("%T: %v", err, root)
	}
	return fmt.Sprintf("%T: %T", err, root)
}

// authError maps the failure of a node grant check: an unknown or deleted workspace is an unaccepted grant.
func authError(err error) error {
	switch {
	case errors.Is(err, pg.ErrNotFound), errors.Is(err, pg.ErrGone):
		return errUnauthorized
	case errors.Is(err, pg.ErrWrongRepo):
		return errForbidden
	}
	return err
}

// ownerError is the claim check of a stored repository: a missing row or another owner is forbidden.
func ownerError(row pg.Repo, err error, owner string) error {
	if errors.Is(err, pg.ErrNotFound) || err == nil && row.Owner != owner {
		return errForbidden
	}
	return err
}

// errClaimLost is the answer to a claim below the stored epoch.
var errClaimLost = pg.ErrClaimLost
