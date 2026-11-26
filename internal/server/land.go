package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/land"
	"github.com/majorcontext/motte/internal/refname"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

// LandsConfig tunes the batched land. A zero field takes its default.
type LandsConfig struct {
	// Lease is the time that a leader holds a ref before it must renew. Default 10 seconds. The clocks of the replicas
	// must differ by much less.
	Lease time.Duration
	// Poll is the wait between two reads of the queue by a waiting request, and the wait of an idle leader. Default 50
	// milliseconds.
	Poll time.Duration
	// Timeout is how long a request waits for its result. Default 30 seconds.
	Timeout time.Duration
	// MaxBatch is the most lands that one ref move holds. Default 256.
	MaxBatch int
}

func (c LandsConfig) withDefaults() LandsConfig {
	if c.Lease <= 0 {
		c.Lease = 10 * time.Second
	}
	if c.Poll <= 0 {
		c.Poll = 50 * time.Millisecond
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	return c
}

var landID = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)

// landOutcome is the stored result of a land: an error answer, or the response of a land that moved the ref.
type landOutcome struct {
	Code    api.Code          `json:"code,omitempty"`
	Message string            `json:"message,omitempty"`
	Details map[string]any    `json:"details,omitempty"`
	Land    *api.LandResponse `json:"land,omitempty"`
}

type landWaiter struct{ repo, id string }

type landing struct {
	mu      sync.Mutex
	leaders map[meta.LandKey]*leader
	waiters map[landWaiter][]chan landOutcome
}

func (s *Server) registerLands(mux *http.ServeMux) {
	s.handle(mux, "POST /v1/repos/{repo}/lands", s.land, grant.ScopeMerge, grant.ScopeWrite)
	s.handle(mux, "GET /v1/repos/{repo}/files/{commit}/{path...}", s.getFile, grant.ScopeRead)
}

func (s *Server) land(w http.ResponseWriter, r *http.Request, c call) error {
	var req api.LandRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	repo := r.PathValue("repo")
	if repo != c.claims.Repo {
		return errForbidden
	}
	if !refname.Valid(req.Ref) || !lowerHex(req.Base, 40, 40) || !lowerHex(req.Head, 40, 40) || req.ID != "" && !landID.MatchString(req.ID) || req.ExpectTree != "" && !lowerHex(req.ExpectTree, 40, 40) {
		return errBody
	}
	known := false
	if req.ID == "" {
		req.ID = rand.Text()
	} else if _, err := s.index.LandItem(r.Context(), repo, req.ID); err == nil {
		known = true
	} else if !errors.Is(err, meta.ErrNotFound) {
		return err
	}
	if !known {
		if err := s.refProtected(r.Context(), c.claims.Scope, repo, req.Ref); err != nil {
			return err
		}
	}
	out, err := s.enqueueAndWait(r.Context(), meta.LandRequest{ID: req.ID, Repo: repo, Ref: req.Ref, Base: req.Base, Head: req.Head, ExpectTree: req.ExpectTree, Kind: scopeKind(c.claims.Scope)})
	if err != nil {
		return err
	}
	if out.Code != "" {
		s.writeError(w, out.Code, out.Message, out.Details)
		return nil
	}
	s.writeJSON(w, http.StatusOK, out.Land)
	return nil
}

// enqueueAndWait queues the land and waits for its result. The request that finds a free ref leads it, and else the
// leader of another replica ends the item, and this request finds the result by its poll.
func (s *Server) enqueueAndWait(ctx context.Context, req meta.LandRequest) (res landOutcome, waitErr error) {
	key := meta.LandKey{Repo: req.Repo, Ref: req.Ref}
	ch := make(chan landOutcome, 1)
	unwatch := s.watchLand(landWaiter{req.Repo, req.ID}, ch)
	defer unwatch()
	clk := s.events.Clock
	_, endEnqueue := store.StartSpan(ctx, "vcs.land.enqueue")
	item, err := s.index.EnqueueLand(ctx, req, clk.Now())
	endEnqueue(err)
	if err != nil {
		return landOutcome{}, err
	}
	if item.Base != req.Base || item.Head != req.Head || item.Ref != req.Ref || item.ExpectTree != req.ExpectTree {
		return landOutcome{Code: api.CodeInvalidBody, Message: "the land id is used by another change"}, nil
	}
	if !item.Done {
		select {
		case s.warmers <- struct{}{}:
			warm, stop := context.WithCancel(ctx)
			defer stop()
			go func() {
				defer func() { <-s.warmers }()
				s.warmLand(warm, req)
			}()
		default:
		}
	}
	timeout := clk.NewTimer(s.lands.Timeout)
	defer timeout.Stop()
	_, endWait := store.StartSpan(ctx, "vcs.land.wait")
	defer func() { endWait(waitErr, attribute.Bool("leader_here", s.leadingHere(key))) }()
	for !item.Done {
		s.kickLeader(key)
		poll := clk.NewTimer(s.lands.Poll)
		select {
		case out := <-ch:
			poll.Stop()
			return out, nil
		case <-poll.C:
			if s.leadingHere(key) {
				continue
			}
			if item, err = s.index.LandItem(ctx, req.Repo, req.ID); err != nil {
				return landOutcome{}, err
			}
		case <-timeout.C:
			poll.Stop()
			return landOutcome{Code: api.CodeLandTimeout, Message: "the land is queued and was not ended in time: ask again with the same id"}, nil
		case <-ctx.Done():
			poll.Stop()
			return landOutcome{}, ctx.Err()
		}
	}
	return decodeOutcome(item)
}

// warmLand reads the objects that the leader will read for the land, while the leader works on the lands before it.
func (s *Server) warmLand(ctx context.Context, req meta.LandRequest) {
	repo, err := store.ParseRepo(req.Repo)
	if err != nil {
		return
	}
	base, err := gitpack.ParseSHA(req.Base)
	if err != nil {
		return
	}
	head, err := gitpack.ParseSHA(req.Head)
	if err != nil {
		return
	}
	if rd, err := s.objects.Reader(ctx, repo); err == nil {
		ctx, end := store.StartSpan(ctx, "vcs.land.warm")
		end(land.Warm(ctx, landSource{rd}, base, head))
	}
}

func decodeOutcome(item meta.LandItem) (landOutcome, error) {
	var out landOutcome
	if err := json.Unmarshal([]byte(item.Result), &out); err != nil {
		return landOutcome{}, err
	}
	if out.Land != nil && item.Generation > 0 {
		out.Land.Generation = item.Generation
	}
	return out, nil
}

func (s *Server) watchLand(k landWaiter, ch chan landOutcome) func() {
	s.landing.mu.Lock()
	defer s.landing.mu.Unlock()
	s.landing.waiters[k] = append(s.landing.waiters[k], ch)
	return func() {
		s.landing.mu.Lock()
		defer s.landing.mu.Unlock()
		rest := s.landing.waiters[k][:0]
		for _, c := range s.landing.waiters[k] {
			if c != ch {
				rest = append(rest, c)
			}
		}
		if len(rest) == 0 {
			delete(s.landing.waiters, k)
			return
		}
		s.landing.waiters[k] = rest
	}
}

func (s *Server) tellLand(k landWaiter, out landOutcome) {
	s.landing.mu.Lock()
	defer s.landing.mu.Unlock()
	for _, ch := range s.landing.waiters[k] {
		select {
		case ch <- out:
		default:
		}
	}
}

// outcomeOf turns the error of one land into its stored result. It returns false for an error that no result can
// stand for, such as a failed read of the store.
func outcomeOf(err error) (landOutcome, bool) {
	var conflict *land.ConflictError
	var invalid land.Invalid
	var stale *land.StaleError
	switch {
	case errors.As(err, &stale):
		return landOutcome{Code: api.CodeLandStale, Message: "the land would make another tree than the expected tree", Details: map[string]any{"tree": stale.Tree.String()}}, true
	case errors.As(err, &conflict):
		return landOutcome{Code: api.CodeLandConflict, Message: "the change touches paths that changed since its base", Details: map[string]any{"paths": conflict.Paths}}, true
	case errors.As(err, &invalid):
		return landOutcome{Code: api.CodeLandInvalid, Message: string(invalid)}, true
	case errors.Is(err, land.ErrTooLarge), errors.Is(err, store.ErrTooLarge):
		return landOutcome{Code: api.CodeObjectTooLarge, Message: "the change is too large to land"}, true
	case errors.Is(err, store.ErrNotFound):
		return landOutcome{Code: api.CodeNotFound, Message: "an object of the change is not stored"}, true
	case errors.Is(err, store.ErrInvalid):
		return landOutcome{Code: api.CodeObjectInvalid, Message: "an object of the change is not valid"}, true
	}
	return landOutcome{}, false
}

func mustJSON(out landOutcome) string {
	raw, _ := json.Marshal(out)
	return string(raw)
}
