package server

import (
	"net/http"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/repos"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

func (s *Server) registerControlPlane(mux *http.ServeMux) {
	s.handle(mux, "POST /v1/repos", s.createRepo, grant.ScopeOperator)
	s.handle(mux, "GET /v1/repos", s.listRepos, grant.ScopeOperator)
	s.handle(mux, "GET /v1/repos/{path}", s.getRepo, grant.ScopeOperator)
	s.handle(mux, "PUT /v1/workspaces/{id}/claim", s.claim, grant.ScopeOperator)
	s.handle(mux, "POST /v1/workspaces/{id}/flush", s.flush, grant.ScopeOperator)
	s.handle(mux, "DELETE /v1/workspaces/{id}", s.deleteWorkspace, grant.ScopeOperator)
}

func repoView(r meta.Repo) api.Repo {
	return api.Repo{Path: r.Path, Name: r.Name, Owner: r.Owner, CreatedAt: r.CreatedAt.UTC().Format(time.RFC3339)}
}

func (s *Server) createRepo(w http.ResponseWriter, r *http.Request, _ call) error {
	var req api.CreateRepoRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	name, err := repos.ValidName(req.Name)
	if err != nil || len(req.Owner) == 0 || len(req.Owner) > 256 {
		return errBody
	}
	path := repos.NewPath()
	names, err := s.objects.PutObjects(r.Context(), store.Repo{Prefix: repos.Prefix, Path: path}, repos.RootObjects(s.now()))
	if err != nil {
		return err
	}
	if err := s.index.CreateRepo(r.Context(), meta.Repo{Path: path, Name: name, Owner: req.Owner}, repos.Text(path), names[1].String()); err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, api.CreateRepoResponse{Path: path})
	return nil
}

func (s *Server) listRepos(w http.ResponseWriter, r *http.Request, _ call) error {
	rows, err := s.index.Repos(r.Context())
	if err != nil {
		return err
	}
	out := api.Repos{Repos: make([]api.Repo, len(rows))}
	for i, row := range rows {
		out.Repos[i] = repoView(row)
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getRepo(w http.ResponseWriter, r *http.Request, _ call) error {
	row, err := s.index.Repo(r.Context(), r.PathValue("path"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, repoView(row))
	return nil
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request, _ call) error {
	id := r.PathValue("id")
	var req api.ClaimRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	repo, err := store.ParseRepo(req.Repo)
	if !store.ValidWorkspaceID(id) || err != nil || req.Epoch == 0 || req.Owner == "" {
		return errBody
	}
	if repo.Prefix == repos.Prefix {
		row, err := s.index.Repo(r.Context(), repo.Path)
		if err := ownerError(row, err, req.Owner); err != nil {
			return err
		}
	}
	ws, err := s.index.Claim(r.Context(), id, req.Epoch, req.Owner, req.Repo)
	if err != nil {
		return err
	}
	if ws.Epoch != req.Epoch {
		return errClaimLost
	}
	s.writeJSON(w, http.StatusOK, api.ClaimResponse{Epoch: ws.Epoch, Op: ws.Op, Snapshot: ws.Snapshot})
	return nil
}

func (s *Server) flush(w http.ResponseWriter, r *http.Request, _ call) error {
	var req api.FlushRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if !store.ValidWorkspaceID(r.PathValue("id")) || req.Epoch == 0 || req.Generation < 0 ||
		!lowerHex(req.Op, 16, 128) || (req.Prev != "" && !lowerHex(req.Prev, 16, 128)) || (len(req.Snapshot) != 40 && len(req.Snapshot) != 64) || !lowerHex(req.Snapshot, 40, 64) {
		return errBody
	}
	if err := s.index.Flush(r.Context(), r.PathValue("id"), req.Epoch, req.Generation, req.Prev, req.Op, req.Snapshot); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) deleteWorkspace(w http.ResponseWriter, r *http.Request, _ call) error {
	if !store.ValidWorkspaceID(r.PathValue("id")) {
		return errBody
	}
	if err := s.index.Delete(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
