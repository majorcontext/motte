// Package server is the HTTP surface of the store: the store plane that the
// node daemon and the jj backend call, and the control plane of the caller.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/pg"
	"github.com/majorcontext/motte/internal/store"
)

// Config is the dependencies of a Server. New builds the index and the object service from Pool and Bucket, so
// every composition shares one graph.
type Config struct {
	Pool     *pgxpool.Pool
	Bucket   bucket.Store
	Verifier *grant.Verifier
	Logger   *slog.Logger
	// Now is the clock of grant expiry and the root commit. It defaults to time.Now.
	Now func() time.Time
}

type Server struct {
	index    index
	objects  objects
	verifier *grant.Verifier
	logger   *slog.Logger
	now      func() time.Time

	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	registry *prometheus.Registry
	mux      *http.ServeMux
	readers  chan struct{}
}

// New returns a Server over the database of cfg.Pool and the object store cfg.Bucket.
func New(cfg Config) *Server {
	db := &pg.DB{Pool: cfg.Pool}
	s := &Server{index: db, objects: store.New(cfg.Bucket, db), verifier: cfg.Verifier, logger: cfg.Logger, now: cfg.Now, readers: make(chan struct{}, uploadReaders)}
	if s.now == nil {
		s.now = time.Now
	}
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	s.registry = prometheus.NewRegistry()
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "motte_http_requests_total", Help: "Requests by route and status."}, []string{"route", "status"})
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "motte_http_request_duration_seconds", Help: "Request time by route.", Buckets: prometheus.DefBuckets}, []string{"route"})
	s.registry.MustRegister(s.requests, s.latency)

	s.mux = http.NewServeMux()
	s.registerStorePlane(s.mux)
	s.registerControlPlane(s.mux)
	s.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	s.mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if s.index.Ping(ctx) != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return s
}

// Handler returns the handler of every route. An unknown path or method answers with the error envelope.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := s.mux.Handler(r)
		if pattern != "" {
			s.mux.ServeHTTP(w, r)
			return
		}
		probe := &statusProbe{header: w.Header()}
		h.ServeHTTP(probe, r)
		code := api.CodeNotFound
		if probe.status == http.StatusMethodNotAllowed {
			code = api.CodeMethodNotAllowed
		}
		s.writeError(w, code, http.StatusText(probe.status), nil)
	})
}

// statusProbe records the status that a mux error handler picks and drops its body.
type statusProbe struct {
	header http.Header
	status int
}

func (p *statusProbe) Header() http.Header         { return p.header }
func (p *statusProbe) WriteHeader(status int)      { p.status = status }
func (p *statusProbe) Write(b []byte) (int, error) { return len(b), nil }

func bearerToken(r *http.Request) string {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return ""
	}
	return tok
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
