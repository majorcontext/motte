package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/usertoken"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

const userTokenReposMax = 64

// registerUserTokens serves the administration of per-user tokens. Only a signed operator grant reaches it: a person
// never issues a token, whatever the identity.
func (s *Server) registerUserTokens(mux *http.ServeMux) {
	s.handle(mux, "POST /v1/tokens", s.createUserToken, grant.ScopeOperator)
	s.handle(mux, "GET /v1/tokens", s.listUserTokens, grant.ScopeOperator)
	s.handle(mux, "DELETE /v1/tokens/{id}", s.revokeUserToken, grant.ScopeOperator)
}

func userTokenView(t meta.UserToken) api.UserToken {
	at := func(v time.Time) string {
		if v.IsZero() {
			return ""
		}
		return v.UTC().Format(time.RFC3339)
	}
	return api.UserToken{ID: t.ID, Principal: t.Principal, Scope: t.Scope, Repos: append([]string{}, t.Repos...),
		CreatedAt: at(t.CreatedAt), ExpiresAt: at(t.ExpiresAt), RevokedAt: at(t.RevokedAt)}
}

func (s *Server) createUserToken(w http.ResponseWriter, r *http.Request, _ call) error {
	var req api.CreateUserTokenRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	bad := req.Principal == "" || len(req.Principal) > 256 || strings.ContainsAny(req.Principal, " \t\r\n") ||
		len(req.Repos) > userTokenReposMax || req.TTLSeconds < 0 || !identity.ValidScope(req.Scope, req.Repos)
	for _, repo := range req.Repos {
		if _, err := store.ParseRepo(repo); err != nil {
			bad = true
		}
	}
	if bad {
		return errBody
	}
	tok, id, hash, err := usertoken.New()
	if err != nil {
		return err
	}
	rec := meta.UserToken{ID: id, Principal: req.Principal, Scope: req.Scope, Repos: req.Repos}
	if req.TTLSeconds > 0 {
		rec.ExpiresAt = s.now().Add(time.Duration(req.TTLSeconds) * time.Second)
	}
	if err := s.index.CreateUserToken(r.Context(), hash, rec); err != nil {
		return err
	}
	got, err := s.index.UserTokenByHash(r.Context(), hash)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusCreated, api.CreateUserTokenResponse{Token: tok, UserToken: userTokenView(got)})
	return nil
}

func (s *Server) listUserTokens(w http.ResponseWriter, r *http.Request, _ call) error {
	rows, err := s.index.UserTokens(r.Context())
	if err != nil {
		return err
	}
	out := api.UserTokens{Tokens: make([]api.UserToken, len(rows))}
	for i, row := range rows {
		out.Tokens[i] = userTokenView(row)
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) revokeUserToken(w http.ResponseWriter, r *http.Request, _ call) error {
	if err := s.index.RevokeUserToken(r.Context(), r.PathValue("id")); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// tokenStore reads the per-user tokens of the metadata store as the records of the verifier.
type tokenStore struct{ meta.UserTokenStore }

func (s tokenStore) TokenByHash(ctx context.Context, hash string) (usertoken.Record, error) {
	t, err := s.UserTokenByHash(ctx, hash)
	return usertoken.Record{Principal: t.Principal, Scope: t.Scope, Repos: t.Repos, ExpiresAt: t.ExpiresAt, RevokedAt: t.RevokedAt}, err
}
