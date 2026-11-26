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
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/identity"
	"github.com/majorcontext/motte/identity/usertoken"
	"github.com/majorcontext/motte/internal/access"
	"github.com/majorcontext/motte/internal/bucket"
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
	// Identity, when set, verifies a person on the git routes and the creation of a repository, and Rules give that
	// person a scope. A grant is tried first, then Identity with Rules, then the per-user tokens of Meta.
	Identity identity.Verifier
	Rules    identity.Rules
	Logger   *slog.Logger
	// Now is the clock of grant expiry and the root commit. It defaults to time.Now.
	Now func() time.Time
	// Events tunes the delivery and the retention of ref events. Zero values take the defaults.
	Events EventsConfig
	// Lands tunes the batched land. Zero values take the defaults.
	Lands LandsConfig
	// Compaction tunes Compact. Zero values take the defaults.
	Compaction CompactionConfig
	// RangeCacheBytes is the memory of the cache of pack ranges that requests share. Zero takes the default, and a
	// negative value turns the cache off.
	RangeCacheBytes int64
	// Warm makes the server answer /ready with 503 until Warm has opened every pack index.
	Warm bool
	// Clock paces the retry of Warm. It defaults to the real clock.
	Clock clock.Clock
}

type Server struct {
	index      meta.Store
	objects    objects
	bearer     authenticate
	bearerGit  authenticate
	gitHuman   access.Authenticator
	adminHuman access.Authenticator
	logger     *slog.Logger
	now        func() time.Time
	events     EventsConfig
	lands      LandsConfig
	compaction CompactionConfig
	landing    landing
	hub        *landHub
	wake       chan struct{}
	clock      clock.Clock
	cold       atomic.Bool

	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	registry *prometheus.Registry
	mux      *http.ServeMux
	readers  chan struct{}
	// largeFetches holds a value for each fetch that passed fetchLarge.
	largeFetches chan struct{}
	// warmers holds a value for each land that is warming its objects.
	warmers chan struct{}
}

// New returns a Server over the metadata of cfg.Meta and the object store cfg.Bucket.
func New(cfg Config) *Server {
	readLatency := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "motte_bucket_read_duration_seconds", Help: "Time of reads from the bucket, by read kind.", Buckets: prometheus.ExponentialBuckets(0.005, 2, 14)}, []string{"op"})
	readErrors := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "motte_bucket_read_errors_total", Help: "Failed reads from the bucket, by kind of failure."}, []string{"kind"})
	for _, k := range []string{bucket.KindTimeout, bucket.Kind5xx, bucket.KindOther} {
		readErrors.WithLabelValues(k)
	}
	metered := bucket.Observe(cfg.Bucket, func(op string, d time.Duration, err error) {
		if kind, failed := bucket.ErrorKind(err); failed {
			readErrors.WithLabelValues(kind).Inc()
		} else if err == nil {
			readLatency.WithLabelValues(op).Observe(d.Seconds())
		}
	})
	s := &Server{index: cfg.Meta, objects: store.New(metered, cfg.Meta, store.WithClock(clock.Or(cfg.Clock)), store.WithGrace(cfg.Compaction.withDefaults().Grace), rangeCacheOption(cfg.RangeCacheBytes)), compaction: cfg.Compaction.withDefaults(), logger: cfg.Logger, now: cfg.Now, readers: make(chan struct{}, uploadReaders), largeFetches: make(chan struct{}, fetchSlots), warmers: make(chan struct{}, landWarmers), wake: make(chan struct{}, 1), clock: clock.Or(cfg.Clock)}
	s.cold.Store(cfg.Warm)
	if s.now == nil {
		s.now = time.Now
	}
	bearerAuth := access.Grant{Verifier: cfg.Verifier, Now: s.now, Token: bearerToken}
	s.bearer = func(r *http.Request) (grant.Claims, error) { return bearerAuth.Authenticate(r, "") }
	gitAuth := access.Grant{Verifier: cfg.Verifier, Now: s.now, Token: gitToken}
	s.bearerGit = func(r *http.Request) (grant.Claims, error) { return gitAuth.Authenticate(r, "") }
	var human access.Authenticator
	if cfg.Identity != nil {
		human = access.Human{Verifier: cfg.Identity, Rules: cfg.Rules}
	}
	userTokens := tokenStore{cfg.Meta}
	s.gitHuman = access.Chain(human, access.UserToken{Verifier: usertoken.Verifier{Store: userTokens, Now: s.now, Token: gitToken}})
	s.adminHuman = access.Chain(human, access.UserToken{Verifier: usertoken.Verifier{Store: userTokens, Now: s.now, Token: bearerToken}})
	s.events = cfg.Events.withDefaults()
	s.lands = cfg.Lands.withDefaults()
	s.hub = newLandHub()
	s.landing = landing{waiters: map[landWaiter][]chan landOutcome{}}
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	s.registry = prometheus.NewRegistry()
	s.requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "motte_http_requests_total", Help: "Requests by route and status."}, []string{"route", "status"})
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "motte_http_request_duration_seconds", Help: "Request time by route.", Buckets: prometheus.DefBuckets}, []string{"route"})
	s.registry.MustRegister(s.requests, s.latency, readLatency, readErrors)
	if h, ok := cfg.Bucket.(interface{ HedgeStats() bucket.HedgeStats }); ok {
		for _, m := range []struct {
			name, help string
			get        func(bucket.HedgeStats) int64
		}{
			{"motte_bucket_hedges_total", "Duplicate reads started on a slow bucket read.", func(h bucket.HedgeStats) int64 { return h.Hedges }},
			{"motte_bucket_hedge_wins_total", "Duplicate reads whose answer came before the first read's.", func(h bucket.HedgeStats) int64 { return h.Wins }},
			{"motte_bucket_hedges_skipped_total", "Slow bucket reads that got no duplicate for lack of a token or budget.", func(h bucket.HedgeStats) int64 { return h.Skipped }},
		} {
			s.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: m.name, Help: m.help}, func() float64 { return float64(m.get(h.HedgeStats())) }))
		}
	}
	s.registerCacheMetrics()

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

// Registry returns the metrics that /metrics serves, so an exporter can read the same values.
func (s *Server) Registry() prometheus.Gatherer { return s.registry }

func (s *Server) registerCacheMetrics() {
	counter := func(name, help string, f func(store.CacheStats) int64) {
		s.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 { return float64(f(s.objects.CacheStats())) }))
	}
	gauge := func(name, help string, f func(store.CacheStats) int64) {
		s.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return float64(f(s.objects.CacheStats())) }))
	}
	counter("motte_pack_cache_hits_total", "Pack opens served by an open pack.", func(c store.CacheStats) int64 { return c.Hits })
	counter("motte_pack_cache_misses_total", "Pack opens that made a new open pack.", func(c store.CacheStats) int64 { return c.Misses })
	counter("motte_pack_cache_evictions_total", "Open packs evicted for the byte budget.", func(c store.CacheStats) int64 { return c.Evictions })
	counter("motte_bucket_index_read_bytes_total", "Bytes of index layers read from the bucket.", func(c store.CacheStats) int64 { return c.IndexBytes })
	counter("motte_bucket_range_reads_total", "Range reads of packs from the bucket.", func(c store.CacheStats) int64 { return c.RangeReads })
	counter("motte_bucket_range_read_bytes_total", "Bytes of range reads of packs from the bucket.", func(c store.CacheStats) int64 { return c.RangeBytes })
	counter("motte_range_cache_hits_total", "Range reads served by the range cache.", func(c store.CacheStats) int64 { return c.RangeCacheHits })
	counter("motte_range_cache_misses_total", "Range reads that the range cache did not hold.", func(c store.CacheStats) int64 { return c.RangeCacheMisses })
	counter("motte_range_cache_evictions_total", "Ranges evicted for the byte budget of the range cache.", func(c store.CacheStats) int64 { return c.RangeCacheEvictions })
	gauge("motte_range_cache_bytes", "Bytes charged to the range cache.", func(c store.CacheStats) int64 { return c.RangeCacheBytes })
	counter("motte_index_cache_hits_total", "Lookups of a layer or a block of records served by the layer cache.", func(c store.CacheStats) int64 { return c.IndexHits })
	counter("motte_index_cache_misses_total", "Lookups of a layer or a block of records that read the bucket.", func(c store.CacheStats) int64 { return c.IndexMisses })
	counter("motte_index_cache_evictions_total", "Layers and blocks evicted for the byte budget of the layer cache.", func(c store.CacheStats) int64 { return c.IndexEvictions })
	gauge("motte_index_cache_bytes", "Bytes charged to the layer cache.", func(c store.CacheStats) int64 { return c.IndexResident })
	gauge("motte_pack_cache_open_packs", "Packs in the open-pack cache.", func(c store.CacheStats) int64 { return c.OpenPacks })
	gauge("motte_pack_cache_open_bytes", "Bytes charged to the open-pack cache.", func(c store.CacheStats) int64 { return c.OpenBytes })
	mcounter := func(name, help string, f func(store.MaintStats) int64) {
		s.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help}, func() float64 { return float64(f(s.objects.MaintStats())) }))
	}
	mgauge := func(name, help string, f func(store.MaintStats) int64) {
		s.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 { return float64(f(s.objects.MaintStats())) }))
	}
	mcounter("motte_compaction_runs_total", "Compactions of packs into one pack.", func(m store.MaintStats) int64 { return m.CompactRuns })
	mcounter("motte_compaction_bytes_total", "Bytes of the packs that compaction wrote.", func(m store.MaintStats) int64 { return m.CompactBytes })
	mcounter("motte_compaction_objects_total", "Objects that compaction copied.", func(m store.MaintStats) int64 { return m.CompactObjects })
	mcounter("motte_layer_merge_runs_total", "Merges of layers into one layer.", func(m store.MaintStats) int64 { return m.MergeRuns })
	mcounter("motte_layer_merge_objects_total", "Records that layer merges read.", func(m store.MaintStats) int64 { return m.MergeObjects })
	mcounter("motte_retired_files_deleted_total", "Retired pack and layer files deleted after their grace.", func(m store.MaintStats) int64 { return m.Deleted })
	mcounter("motte_maintenance_failures_total", "Maintenance passes of a repository that failed.", func(m store.MaintStats) int64 { return m.Failures })
	mgauge("motte_manifest_layers_max", "Most layers of any repository that maintenance has seen.", func(m store.MaintStats) int64 { return m.LayersMax })
	mgauge("motte_manifest_layers_sum", "Layers of all repositories that maintenance has seen.", func(m store.MaintStats) int64 { return m.LayersSum })
	mgauge("motte_manifest_repos", "Repositories that maintenance has seen.", func(m store.MaintStats) int64 { return m.Repos })
}

// rangeCacheOption maps Config.RangeCacheBytes to the option of the store: zero keeps the default.
func rangeCacheOption(n int64) store.Option {
	if n == 0 {
		return func(*store.Service) {}
	}
	return store.WithRangeCache(max(n, 0))
}
