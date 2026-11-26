package server

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/land"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

const (
	lingerPolls         = 4
	softBatchBytes      = 16 << 20
	landSlack           = 16 << 20
	historyRows         = 1000
	landAttempts        = 8
	landFailures        = 3
	landResultRetention = 24 * time.Hour
	// landWarmers bounds the lands that read their objects while they wait.
	landWarmers = 8
)

var errLeaseLost = errors.New("server: land lease lost")

// leader is the worker of one ref on this replica. holding is true while it holds the lease of the ref.
type leader struct {
	wake    chan struct{}
	holding atomic.Bool
}

// kickLeader makes sure that this replica works for the ref. It wakes the leader that runs here, or starts one. A
// leader that finds another replica holding the lease waits for the rest of the lease and then claims again.
func (s *Server) kickLeader(key meta.LandKey) {
	h := s.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if l, ok := h.leaders[key]; ok {
		select {
		case l.wake <- struct{}{}:
		default:
		}
		return
	}
	l := &leader{wake: make(chan struct{}, 1)}
	h.start(key, l, func(ctx context.Context) { s.lead(ctx, key, l) })
}

// leadingHere reports whether a leader of this replica holds the lease of the ref. Such a leader hands each result to
// its waiting request, so the request needs no poll of the store.
func (s *Server) leadingHere(key meta.LandKey) bool {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	l, ok := s.hub.leaders[key]
	return ok && l.holding.Load()
}

// dropLead unregisters l at a terminal exit. A pending wake is dropped with it: the next poll of a waiting request
// starts a new leader.
func (s *Server) dropLead(key meta.LandKey, l *leader) {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if s.hub.leaders[key] == l {
		delete(s.hub.leaders, key)
	}
}

// stopLead unregisters l unless a kick came in since the last claim.
func (s *Server) stopLead(key meta.LandKey, l *leader) bool {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if len(l.wake) > 0 {
		return false
	}
	delete(s.hub.leaders, key)
	return true
}

func (s *Server) lead(ctx context.Context, key meta.LandKey, l *leader) {
	owner := s.events.Instance
	fails := 0
	defer l.holding.Store(false)
	for idle := 0; ; {
		now := s.events.Clock.Now()
		items, heldUntil, err := s.claimLands(ctx, key, owner, now)
		l.holding.Store(err == nil)
		if errors.Is(err, meta.ErrLeaseHeld) {
			if !s.waitLease(ctx, l, heldUntil.Sub(s.events.Clock.Now())) {
				s.dropLead(key, l)
				return
			}
			continue
		}
		if err != nil {
			if ctx.Err() == nil {
				s.logger.ErrorContext(ctx, "land claim failed", "error", logSafe(err))
			}
			s.dropLead(key, l)
			return
		}
		if len(items) == 0 {
			if idle++; idle > lingerPolls {
				l.holding.Store(false)
				_ = s.index.ReleaseLandLease(ctx, key.Repo, key.Ref, owner)
				if s.stopLead(key, l) {
					return
				}
				idle = 0
				continue
			}
			s.idleLead(ctx, l)
			continue
		}
		idle = 0
		err = s.landBatch(ctx, key, owner, items)
		if err != nil && ctx.Err() != nil {
			s.dropLead(key, l)
			return
		}
		switch {
		case errors.Is(err, errLeaseLost):
			s.dropLead(key, l)
			return
		case err != nil && fails+1 >= landFailures:
			s.failLands(ctx, key, owner, items)
			fails = 0
		case err != nil:
			fails++
			s.logger.ErrorContext(ctx, "land batch failed", "error", logSafe(err))
			s.idleLead(ctx, l)
		default:
			fails = 0
		}
	}
}

func (s *Server) idleLead(ctx context.Context, l *leader) {
	t := s.events.Clock.NewTimer(s.lands.Poll)
	defer t.Stop()
	select {
	case <-t.C:
	case <-l.wake:
	case <-ctx.Done():
	}
}

// waitLease waits for d, the rest of the lease of another replica, and ends early when a request kicks the leader. It
// waits one poll when the end has passed, so it makes no busy loop. It reports false when the hub closed first.
func (s *Server) waitLease(ctx context.Context, l *leader, d time.Duration) bool {
	if d <= 0 {
		d = s.lands.Poll
	}
	t := s.events.Clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-l.wake:
	case <-ctx.Done():
		return false
	}
	return true
}

// claimLands takes the lease and claims the items. When another owner holds the lease it returns ErrLeaseHeld and the
// end of that lease.
func (s *Server) claimLands(ctx context.Context, key meta.LandKey, owner string, now time.Time) ([]meta.LandItem, time.Time, error) {
	until, err := s.index.AcquireLandLease(ctx, key.Repo, key.Ref, owner, now, now.Add(s.lands.Lease))
	if err != nil {
		return nil, until, err
	}
	items, err := s.index.ClaimLands(ctx, key.Repo, key.Ref, owner, now, s.lands.MaxBatch)
	return items, now, err
}

func (s *Server) failLands(ctx context.Context, key meta.LandKey, owner string, items []meta.LandItem) {
	out := mustJSON(landOutcome{Code: api.CodeInternal, Message: "the land failed: ask again with a new id"})
	done := make([]meta.LandDone, len(items))
	for i, it := range items {
		done[i] = meta.LandDone{ID: it.ID, Result: out}
	}
	if err := s.index.FinishLands(ctx, key.Repo, key.Ref, owner, s.events.Clock.Now(), done); err != nil {
		s.logger.ErrorContext(ctx, "land failure not recorded", "error", logSafe(err))
		return
	}
	for _, it := range items {
		s.tellLand(landWaiter{key.Repo, it.ID}, landOutcome{Code: api.CodeInternal, Message: "the land failed: ask again with a new id"})
	}
}

func (r *reservation) growLand(retained int64) error {
	return r.grow(min(landSlack+4*retained, store.LandBudget))
}

type landSource struct{ r *store.Reader }

func (l landSource) Get(ctx context.Context, sha gitpack.SHA, before func(int64) error) (gitpack.Type, []byte, error) {
	return l.r.GetSized(ctx, sha, before)
}

func (s *Server) landBatch(ctx context.Context, key meta.LandKey, owner string, items []meta.LandItem) (err error) {
	ctx, _ = store.WithStats(ctx)
	ctx, end := store.StartRootSpan(ctx, "vcs.land.batch", attribute.Int("items", len(items)))
	defer func() { end(err) }()
	repo, err := store.ParseRepo(key.Repo)
	if err != nil {
		return err
	}
	res := &reservation{take: func(n int64) (func(), error) { return s.objects.ReserveLand(ctx, n) }}
	defer res.drop()
	for range landAttempts {
		retry, err := (&batchRun{s: s, key: key, owner: owner, repo: repo, items: items, res: res}).attempt(ctx)
		if !retry {
			return err
		}
		trace.SpanFromContext(ctx).AddEvent("ref_moved")
	}
	return errors.New("server: the ref moved on every attempt of a land batch")
}

type batchRun struct {
	s     *Server
	key   meta.LandKey
	owner string
	repo  store.Repo
	items []meta.LandItem
	res   *reservation

	results []meta.LandDone
	told    map[string]landOutcome
	rows    []meta.LandLogRow
	kind    string
}

func (b *batchRun) end(it meta.LandItem, out landOutcome, landed bool) {
	b.results = append(b.results, meta.LandDone{ID: it.ID, Result: mustJSON(out), Landed: landed})
	b.told[it.ID] = out
}

func (b *batchRun) attempt(ctx context.Context) (retry bool, err error) {
	s := b.s
	b.results, b.rows, b.kind, b.told = nil, nil, "", map[string]landOutcome{}
	ref, err := s.index.Ref(ctx, b.key.Repo, b.key.Ref)
	if errors.Is(err, meta.ErrNotFound) {
		for _, it := range b.items {
			b.end(it, landOutcome{Code: api.CodeNotFound, Message: "the ref does not exist"}, false)
		}
		return false, b.finish(ctx)
	}
	if err != nil {
		return false, err
	}
	protected := s.refProtected(ctx, grant.ScopeWrite, b.key.Repo, b.key.Ref)
	if protected != nil && !errors.Is(protected, errProtected) {
		return false, protected
	}
	writeRefused := protected != nil
	if err := b.res.growLand(0); err != nil {
		return false, err
	}
	reader, err := s.objects.Reader(ctx, b.repo)
	if err != nil {
		return false, err
	}
	tip, err := gitpack.ParseSHA(ref.Commit)
	if err != nil {
		return false, err
	}
	batch := land.NewBatch(landSource{reader}, b.res.growLand, tip, s.landHistory(b.key, tip))
	when := s.events.Clock.Now()
	for _, it := range b.items {
		if batch.Bytes() >= softBatchBytes {
			break
		}
		if it.Kind == meta.WriterWrite && writeRefused {
			b.end(it, landOutcome{Code: api.CodeRefProtected, Message: "the ref is protected"}, false)
			continue
		}
		if err := b.land(ctx, batch, it, ref, when); err != nil {
			return false, err
		}
	}
	if len(b.rows) == 0 {
		return false, b.finish(ctx)
	}
	if objs := batch.Objects(); len(objs) > 0 {
		if err := b.res.growLand(int64(batch.Bytes())); err != nil {
			return false, err
		}
		_, endWrite := store.StartSpan(ctx, "vcs.land.write", attribute.Int("objects", len(objs)), attribute.Int("batch_bytes", batch.Bytes()))
		for _, part := range splitObjects(objs, store.MaxBatch/2, store.MaxPackBytes/2) {
			if _, err := s.objects.PutObjects(ctx, b.repo, part); err != nil {
				endWrite(err)
				return false, err
			}
		}
		endWrite(nil)
	}
	_, endSwap := store.StartSpan(ctx, "vcs.land.swap", attribute.Int("rows", len(b.rows)))
	moved, err := s.index.LandRef(ctx, meta.LandMove{Repo: b.key.Repo, Ref: b.key.Ref, Old: ref.Commit, New: batch.Tip().String(), Kind: b.kind, Rows: b.rows, Results: b.results}, b.owner, s.events.Clock.Now())
	var conflict *meta.RefConflict
	isConflict := errors.As(err, &conflict)
	if isConflict {
		endSwap(nil, attribute.Bool("conflict", true))
	} else {
		endSwap(err, attribute.Bool("conflict", false))
	}
	switch {
	case errors.As(err, &conflict):
		return true, nil
	case errors.Is(err, meta.ErrLeaseHeld):
		return false, errLeaseLost
	case err != nil:
		return false, err
	}
	s.notify()
	b.tell(moved.Generation)
	return false, nil
}

func (b *batchRun) land(ctx context.Context, batch *land.Batch, it meta.LandItem, ref meta.Ref, when time.Time) error {
	base, err := gitpack.ParseSHA(it.Base)
	if err != nil {
		return err
	}
	head, err := gitpack.ParseSHA(it.Head)
	if err != nil {
		return err
	}
	var expect *gitpack.SHA
	if it.ExpectTree != "" {
		tree, err := gitpack.ParseSHA(it.ExpectTree)
		if err != nil {
			return err
		}
		expect = &tree
	}
	_, endMerge := store.StartSpan(ctx, "vcs.land.merge", attribute.Bool("expect_tree", expect != nil))
	l, err := batch.LandExpecting(ctx, base, head, when, expect)
	if err != nil {
		out, ok := outcomeOf(err)
		if !ok {
			endMerge(err)
			return err
		}
		endMerge(nil, attribute.String("outcome", string(out.Code)))
		b.end(it, out, false)
		return nil
	}
	endMerge(nil, attribute.String("outcome", landKind(l)), attribute.Int("commits", len(l.Commits)))
	resp := &api.LandResponse{ID: it.ID, Ref: b.key.Ref, Tip: l.New.String(), FastForward: l.FastForward, Commits: []api.LandedCommit{}}
	for _, c := range l.Commits {
		resp.Commits = append(resp.Commits, api.LandedCommit{Old: c.Old.String(), New: c.New.String()})
	}
	if l.Noop {
		moved := batch.Tip().String() != ref.Commit
		resp.Generation = ref.Generation
		b.end(it, landOutcome{Land: resp}, moved)
		return nil
	}
	paths := l.Paths
	if paths == nil && !l.Overflow {
		paths = []string{}
	}
	b.rows = append(b.rows, meta.LandLogRow{Old: l.Old.String(), New: l.New.String(), Paths: paths})
	if b.kind != meta.WriterMerge {
		b.kind = it.Kind
	}
	b.end(it, landOutcome{Land: resp}, true)
	return nil
}

func landKind(l land.Landed) string {
	switch {
	case l.Noop:
		return "noop"
	case l.FastForward:
		return "fast_forward"
	}
	return "rebase"
}

func (b *batchRun) finish(ctx context.Context) error {
	err := b.s.index.FinishLands(ctx, b.key.Repo, b.key.Ref, b.owner, b.s.events.Clock.Now(), b.results)
	if errors.Is(err, meta.ErrLeaseHeld) {
		return errLeaseLost
	}
	if err == nil {
		b.tell(0)
	}
	return err
}

func (b *batchRun) tell(generation int64) {
	for _, d := range b.results {
		out := b.told[d.ID]
		if d.Landed {
			out.Land.Generation = generation
		}
		b.s.tellLand(landWaiter{b.key.Repo, d.ID}, out)
	}
}

// landHistory answers the paths that the stored lands changed since a base, from the land log. It answers only when
// the rows form one unbroken chain from base to the tip at the start of the batch: a write of another writer on the
// ref breaks the chain, and a row that kept no paths hides them.
func (s *Server) landHistory(key meta.LandKey, start gitpack.SHA) land.History {
	type answer struct {
		rows []meta.LandLogRow
		ok   bool
	}
	cache := map[gitpack.SHA]answer{}
	return func(ctx context.Context, base gitpack.SHA, emit func(string)) (bool, error) {
		a, seen := cache[base]
		if !seen {
			rows, err := s.index.LandLogSince(ctx, key.Repo, key.Ref, base.String(), historyRows)
			if err != nil {
				return false, err
			}
			a = answer{rows, chained(rows, base, start)}
			cache[base] = a
		}
		if a.ok {
			for _, r := range a.rows {
				for _, p := range r.Paths {
					emit(p)
				}
			}
		}
		return a.ok, nil
	}
}

// chained reports whether rows form one unbroken chain from base to start with every path kept.
func chained(rows []meta.LandLogRow, base, start gitpack.SHA) bool {
	prev, total := base.String(), 0
	for _, r := range rows {
		if r.Old != prev || r.Paths == nil {
			return false
		}
		prev, total = r.New, total+len(r.Paths)
	}
	return len(rows) > 0 && prev == start.String() && total <= land.MaxChangedPaths
}

// sweepLands starts a leader here for every ref that has queued lands, so a land that a crashed replica accepted does
// not wait for a request. It also removes old results.
func (s *Server) sweepLands(ctx context.Context, prune bool) {
	if prune {
		if err := s.index.PruneLands(ctx, s.events.Clock.Now().Add(-landResultRetention)); err != nil && ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "land prune failed", "error", logSafe(err))
		}
	}
	keys, err := s.index.LandPending(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.ErrorContext(ctx, "land sweep failed", "error", logSafe(err))
		}
		return
	}
	for _, k := range keys {
		s.kickLeader(k)
	}
}

// splitObjects cuts objs into parts of at most count objects and bytes of data, in order, so each part is a valid put.
func splitObjects(objs []gitpack.Object, count, bytes int) [][]gitpack.Object {
	var parts [][]gitpack.Object
	start, size := 0, 0
	for i, o := range objs {
		if i > start && (i-start == count || size+len(o.Data) > bytes) {
			parts = append(parts, objs[start:i])
			start, size = i, 0
		}
		size += len(o.Data)
	}
	return append(parts, objs[start:])
}
