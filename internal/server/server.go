// Package server is the HTTP surface of the store: the store plane that the
// node daemon and the jj backend call, and the control plane of the caller.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/store"
)

type Server struct {
	DB       *pg.DB
	Objects  *store.Service
	Verifier *grant.Verifier
	Logger   *slog.Logger
	// Now is the clock of grant expiry and the root commit. It defaults to time.Now.
	Now func() time.Time

	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	registry *prometheus.Registry
}

// Handler returns the mux of every route.
func (s *Server) Handler() http.Handler {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.Logger == nil {
		s.Logger = slog.New(slog.DiscardHandler)
	}
	s.registry = prometheus.NewRegistry()
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "motte_http_requests_total", Help: "Requests by route and status."}, []string{"route", "status"})
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "motte_http_request_duration_seconds", Help: "Request time by route.", Buckets: prometheus.DefBuckets}, []string{"route"})
	s.registry.MustRegister(s.requests, s.latency)

	mux := http.NewServeMux()
	s.registerStorePlane(mux)
	s.registerControlPlane(mux)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if s.DB.Pool.Ping(ctx) != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return mux
}

func bearerToken(r *http.Request) string {
	tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return tok
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

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
			d = map[string]any{"ref": refConflict.Current}
		}
		s.writeError(w, api.CodeGenerationConflict, "ref generation conflict", d)
	case errors.As(err, &opConflict):
		s.writeError(w, api.CodeGenerationConflict, "op heads generation conflict", nil)
	default:
		s.Logger.ErrorContext(r.Context(), "request failed", "route", r.Pattern, "error", err)
		s.writeError(w, api.CodeInternal, "internal error", nil)
	}
}

var (
	errForbidden    = errors.New("server: repository forbidden")
	errBody         = errors.New("server: invalid body")
	errUnauthorized = errors.New("server: grant not accepted")
)

func decodeBody(r *http.Request, limit int, out any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, int64(limit)+1))
	if err != nil || len(raw) == 0 || len(raw) > limit || json.Unmarshal(raw, out) != nil {
		return errBody
	}
	return nil
}
