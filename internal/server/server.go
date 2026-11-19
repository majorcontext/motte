// Package server is the HTTP surface of the store: the store plane that the
// node daemon and the jj backend call, and the control plane of the caller.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/bucket"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/clock"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

// Config is the dependencies of a Server. New builds the object service from Meta and Bucket, so every composition
// shares one graph.
type Config struct {
	Meta     meta.Store
	Bucket   bucket.Store
	Verifier *grant.Verifier
	Logger   *slog.Logger
	// Now is the clock of grant expiry and the root commit. It defaults to time.Now.
	Now func() time.Time
	// Events tunes the delivery and the retention of ref events. Zero values take the defaults.
	Events EventsConfig
	// Lands tunes the batched land. Zero values take the defaults.
	Lands LandsConfig
	// Warm makes the server answer /ready with 503 until Warm has opened every pack index.
	Warm bool
	// Clock paces the retry of Warm. It defaults to the real clock.
	Clock clock.Clock
}

type Server struct {
	index    meta.Store
	objects  objects
	verifier *grant.Verifier
	logger   *slog.Logger
	now      func() time.Time
	events   EventsConfig
	lands    LandsConfig
	landing  landing
	wake     chan struct{}
	clock    clock.Clock
	cold     atomic.Bool

	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	registry *prometheus.Registry
	mux      *http.ServeMux
	readers  chan struct{}
}

// New returns a Server over the metadata of cfg.Meta and the object store cfg.Bucket.
func New(cfg Config) *Server {
	s := &Server{index: cfg.Meta, objects: store.New(cfg.Bucket, cfg.Meta), verifier: cfg.Verifier, logger: cfg.Logger, now: cfg.Now, readers: make(chan struct{}, uploadReaders), wake: make(chan struct{}, 1), clock: clock.Or(cfg.Clock)}
	s.cold.Store(cfg.Warm)
	if s.now == nil {
		s.now = time.Now
	}
	s.events = cfg.Events.withDefaults()
	s.lands = cfg.Lands.withDefaults()
	s.landing = landing{leaders: map[meta.LandKey]*leader{}, waiters: map[landWaiter][]chan landOutcome{}}
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
	s.registerGit(s.mux)
	s.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	s.mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if s.cold.Load() || s.index.Ping(ctx) != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return s
}

// notify wakes the delivery worker of this replica. It never blocks, and a wake that is already pending covers this one.
func (s *Server) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
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
