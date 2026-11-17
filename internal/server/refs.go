package server

import (
	"context"
	"net/http"
	"slices"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/refname"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

const maxProtectedPatterns = 100

// refWrite is one change of a ref. A store-plane write compares on the generation Expect and sets the commit SHA, or
// removes the ref. A git push has ByCommit set and compares on the commit Old, and an empty Old or New means that
// the ref is absent.
type refWrite struct {
	Name     string
	Expect   int64
	SHA      string
	Remove   bool
	ByCommit bool
	Old, New string
}

// writeRefAs is the one path that moves a ref, for every route. A merge grant writes with no workspace fence and may
// write a protected ref. A write grant writes the same way, by commit, and may not write a protected ref. A node
// grant writes by generation under its epoch fence and may not write a protected ref. Any other grant, and a mode that
// a scope does not use, moves no ref. A write that succeeds wakes the delivery worker.
func (s *Server) writeRefAs(ctx context.Context, c call, w refWrite) (meta.Ref, error) {
	ref, err := s.moveRef(ctx, c, w)
	if err == nil {
		s.notify()
	}
	return ref, err
}

func (s *Server) moveRef(ctx context.Context, c call, w refWrite) (meta.Ref, error) {
	repo, scope := c.claims.Repo, c.claims.Scope
	switch {
	case scope == grant.ScopeMerge, scope == grant.ScopeWrite && w.ByCommit, scope == grant.ScopeNode && !w.ByCommit:
	default:
		return meta.Ref{}, errUnauthorized
	}
	if err := s.refProtected(ctx, scope, repo, w.Name); err != nil {
		return meta.Ref{}, err
	}
	switch {
	case w.ByCommit:
		return s.index.UpdateRef(ctx, repo, w.Name, w.Old, w.New, scopeKind(scope))
	case scope == grant.ScopeMerge && w.Remove:
		return meta.Ref{}, s.index.MergeDeleteRef(ctx, repo, w.Name, w.Expect)
	case scope == grant.ScopeMerge:
		return s.index.MergeRef(ctx, repo, w.Name, w.Expect, w.SHA)
	case w.Remove:
		return meta.Ref{}, s.index.DeleteRef(ctx, c.claims.WS, c.claims.Epoch, repo, w.Name, w.Expect)
	}
	return s.index.SwapRef(ctx, c.claims.WS, c.claims.Epoch, repo, w.Name, w.Expect, w.SHA)
}

// refProtected is errProtected when a protected pattern of the repo matches the ref and the scope may not write a
// protected ref. A merge grant may.
func (s *Server) refProtected(ctx context.Context, scope grant.Scope, repo, name string) error {
	if scope == grant.ScopeMerge {
		return nil
	}
	patterns, err := s.index.ProtectedRefs(ctx, repo)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(patterns, func(p string) bool { return refname.Match(p, name) }) {
		return errProtected
	}
	return nil
}

// scopeKind is the writer kind that the event of a commit-compared write records.
func scopeKind(scope grant.Scope) string {
	if scope == grant.ScopeMerge {
		return meta.WriterMerge
	}
	return meta.WriterWrite
}

func (s *Server) registerRefPolicy(mux *http.ServeMux) {
	s.handle(mux, "PUT /v1/repos/{repo}/protected-refs", s.setProtectedRefs, grant.ScopeOperator)
	s.handle(mux, "GET /v1/repos/{repo}/protected-refs", s.getProtectedRefs, grant.ScopeOperator)
}

func (s *Server) setProtectedRefs(w http.ResponseWriter, r *http.Request, _ call) error {
	var req api.ProtectedRefs
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if _, err := store.ParseRepo(r.PathValue("repo")); err != nil || len(req.Patterns) > maxProtectedPatterns || slices.ContainsFunc(req.Patterns, func(p string) bool { return !refname.ValidPattern(p) }) {
		return errBody
	}
	if err := s.index.SetProtectedRefs(r.Context(), r.PathValue("repo"), req.Patterns); err != nil {
		return err
	}
	return s.writePatterns(w, r)
}

func (s *Server) getProtectedRefs(w http.ResponseWriter, r *http.Request, _ call) error {
	if _, err := store.ParseRepo(r.PathValue("repo")); err != nil {
		return errBody
	}
	return s.writePatterns(w, r)
}

func (s *Server) writePatterns(w http.ResponseWriter, r *http.Request) error {
	patterns, err := s.index.ProtectedRefs(r.Context(), r.PathValue("repo"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, api.ProtectedRefs{Patterns: append([]string{}, patterns...)})
	return nil
}
