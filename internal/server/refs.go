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

// writeRefAs is the one path that moves a ref, for every route. A merge grant writes with no workspace fence. A node
// grant writes under its epoch fence and may not write a protected ref. Any other grant moves no ref.
func (s *Server) writeRefAs(ctx context.Context, c call, name string, expect int64, sha string, remove bool) (meta.Ref, error) {
	ref, err := s.moveRef(ctx, c, name, expect, sha, remove)
	if err == nil {
		s.notify()
	}
	return ref, err
}

func (s *Server) moveRef(ctx context.Context, c call, name string, expect int64, sha string, remove bool) (meta.Ref, error) {
	switch c.claims.Scope {
	case grant.ScopeMerge:
		if remove {
			return meta.Ref{}, s.index.MergeDeleteRef(ctx, c.claims.Repo, name, expect)
		}
		return s.index.MergeRef(ctx, c.claims.Repo, name, expect, sha)
	case grant.ScopeNode:
		patterns, err := s.index.ProtectedRefs(ctx, c.claims.Repo)
		if err != nil {
			return meta.Ref{}, err
		}
		if slices.ContainsFunc(patterns, func(p string) bool { return refname.Match(p, name) }) {
			return meta.Ref{}, errProtected
		}
		if remove {
			return meta.Ref{}, s.index.DeleteRef(ctx, c.claims.WS, c.claims.Epoch, c.claims.Repo, name, expect)
		}
		return s.index.SwapRef(ctx, c.claims.WS, c.claims.Epoch, c.claims.Repo, name, expect, sha)
	}
	return meta.Ref{}, errUnauthorized
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
