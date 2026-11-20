package server

import (
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/store"
)

var tracer = otel.Tracer("github.com/majorcontext/motte/internal/server")

// call is an authenticated request. repo is set for every scope but operator.
type call struct {
	claims grant.Claims
	repo   store.Repo
}

type handlerFunc func(w http.ResponseWriter, r *http.Request, c call) error

// opName names the route of a request for spans: the literal path segments after /v1, led by the method of a write.
func opName(pattern string) string {
	method, path, _ := strings.Cut(pattern, " ")
	var parts []string
	for _, seg := range strings.Split(strings.TrimPrefix(path, "/v1/"), "/") {
		if seg != "vcs" && !strings.HasPrefix(seg, "{") && seg != "" {
			parts = append(parts, seg)
		}
	}
	op := strings.Join(parts, ".")
	if method != http.MethodGet {
		op = strings.ToLower(method) + "." + op
	}
	return op
}

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	w.status, w.wrote = code, true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// handle registers fn for pattern. It checks the grant against scopes, then
// for a node grant the workspace fence, runs fn and answers a failure. The
// span and the metrics carry the route, never the grant or a path.
func (s *Server) handle(mux *http.ServeMux, pattern string, fn handlerFunc, scopes ...grant.Scope) {
	s.register(mux, pattern, fn, scopes, s.bearer, "")
}

// register is handle with the way that the route reads its credential. A route with a challenge answers a refused
// credential with a WWW-Authenticate header of it.
func (s *Server) register(mux *http.ServeMux, pattern string, fn handlerFunc, scopes []grant.Scope, authn authenticate, challenge string) {
	op := opName(pattern)
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()
		if remote := (propagation.TraceContext{}).Extract(ctx, propagation.HeaderCarrier(r.Header)); trace.SpanContextFromContext(remote).IsValid() {
			ctx = remote
		}
		ctx, span := tracer.Start(ctx, "vcs.serve."+op, trace.WithAttributes(attribute.String("route", pattern)))
		ctx, stats := store.WithStats(ctx)
		r = r.WithContext(ctx)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			span.SetAttributes(attribute.Int("http.response.status_code", sw.status),
				attribute.Int64("objects", stats.Objects.Load()), attribute.Int64("bytes", stats.Bytes.Load()),
				attribute.Int64("packs_read", stats.Packs.Load()), attribute.Int64("range_reads", stats.RangeReads.Load()),
				attribute.Float64("slot_wait_ms", float64(stats.SlotWaitNanos.Load())/1e6))
			span.End()
			s.requests.WithLabelValues(pattern, strconv.Itoa(sw.status)).Inc()
			s.latency.WithLabelValues(pattern).Observe(time.Since(start).Seconds())
		}()
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				s.logger.ErrorContext(r.Context(), "handler panic", "route", pattern, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
				if !sw.wrote {
					s.writeError(sw, api.CodeInternal, "internal error", nil)
				}
			}
		}()
		c, err := s.auth(r, authn, scopes)
		if err == nil {
			err = fn(sw, r, c)
		}
		if err != nil {
			if challenge != "" && errors.Is(err, errUnauthorized) {
				sw.Header().Set("WWW-Authenticate", challenge)
			}
			s.writeErr(sw, r, err)
		}
	})
}

// authenticate turns the credential of a request into claims.
type authenticate func(r *http.Request) (grant.Claims, error)

func (s *Server) auth(r *http.Request, authn authenticate, scopes []grant.Scope) (call, error) {
	claims, err := authn(r)
	if err != nil || !slices.Contains(scopes, claims.Scope) {
		return call{}, errUnauthorized
	}
	c := call{claims: claims}
	if claims.Scope == grant.ScopeOperator {
		return c, nil
	}
	if c.repo, err = store.ParseRepo(claims.Repo); err != nil {
		return call{}, errUnauthorized
	}
	if claims.Scope != grant.ScopeNode {
		return c, nil
	}
	return c, authError(s.index.Live(r.Context(), claims.WS, claims.Epoch, claims.Repo))
}

// reserve takes bytes of the memory budget and adds the wait to the stats of the request.
func (s *Server) reserve(r *http.Request, bytes int64) (func(), error) {
	start := time.Now()
	release, err := s.objects.Reserve(r.Context(), bytes)
	store.StatsFrom(r.Context()).AddSlotWait(time.Since(start))
	return release, err
}
