// Package metatest is the conformance suite of meta.Store. A backend passes
// it before a server uses the backend.
package metatest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/majorcontext/motte/meta"
)

const (
	repoText = "gh:acme/widgets"
	commitA  = "1111111111111111111111111111111111111111"
	commitB  = "2222222222222222222222222222222222222222"
	opA      = "aaaaaaaaaaaaaaaa"
	opB      = "bbbbbbbbbbbbbbbb"
	opC      = "cccccccccccccccc"
	racers   = 8
)

var ctx = context.Background()

// Run runs the suite. newStore returns an empty store for one test; cases run in parallel, so each store is its own.
func Run(t *testing.T, newStore func(t *testing.T) meta.Store) {
	for _, c := range []struct {
		name string
		fn   func(*testing.T, meta.Store)
	}{
		{"ping", ping},
		{"create repo is atomic", createRepoIsAtomic},
		{"repos list newest first", reposNewestFirst},
		{"one racer of many creates a repo", oneRepoCreatorWins},
		{"claim takes the highest epoch and binds owner and repo", claimRules},
		{"racing claims end on the highest epoch", racingClaims},
		{"live reports the first failed condition", liveOrder},
		{"delete is idempotent and fences every write", deleteRules},
		{"a stale epoch never writes", staleEpochNeverWrites},
		{"a claim waits for a write in flight and then refuses the old epoch", claimFencesWritesInFlight},
		{"report orders by generation", reportOrdering},
		{"report and flush answer by fence", reportAndFlushFence},
		{"op heads swap is a compare and swap", opHeadsSwap},
		{"one racer of many swaps op heads", oneOpHeadsSwapWins},
		{"ref swap and delete are compare and swap", refSwapAndDelete},
		{"ref generation never repeats", refGenerationNeverRepeats},
		{"one racer of many swaps a ref", oneRefSwapWins},
		{"update ref is a compare and swap on the commit", updateRefRules},
		{"one racer of many updates a ref", oneRefUpdateWins},
		{"refs list by name within a repo", refsList},
		{"packs list oldest first and add once", packs},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, newStore(t))
		})
	}
	for _, c := range append(append(policyCases, landCases...), userTokenCases...) {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, newStore(t))
		})
	}
}

func is(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func ok(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func claim(t *testing.T, s meta.Store, ws string, epoch uint64) meta.Workspace {
	t.Helper()
	w, err := s.Claim(ctx, ws, epoch, "o1", repoText)
	ok(t, err)
	return w
}

func refConflict(t *testing.T, err error) *meta.RefConflict {
	t.Helper()
	var c *meta.RefConflict
	if !errors.As(err, &c) {
		t.Fatalf("error = %v, want *meta.RefConflict", err)
	}
	return c
}

func opHeadsConflict(t *testing.T, err error) *meta.OpHeadsConflict {
	t.Helper()
	var c *meta.OpHeadsConflict
	if !errors.As(err, &c) {
		t.Fatalf("error = %v, want *meta.OpHeadsConflict", err)
	}
	return c
}

// race runs fn on racers goroutines at once and returns their errors.
func race(fn func(i int) error) []error {
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = fn(i)
		}()
	}
	close(start)
	wg.Wait()
	return errs
}

// onlyOneWins fails unless exactly one error is nil and the others satisfy lost.
func onlyOneWins(t *testing.T, errs []error, lost func(error) bool) {
	t.Helper()
	wins := 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case !lost(err):
			t.Errorf("a loser got %v", err)
		}
	}
	if wins != 1 {
		t.Errorf("%d racers won, want exactly 1: %v", wins, errs)
	}
}

func ping(t *testing.T, s meta.Store) { ok(t, s.Ping(ctx)) }

func createRepoIsAtomic(t *testing.T, s meta.Store) {
	_, err := s.Repo(ctx, "repo_a")
	is(t, err, meta.ErrNotFound)
	ok(t, s.CreateRepo(ctx, meta.Repo{Path: "repo_a", Name: "widgets", Owner: "o1"}, "native:repo_a", commitA))
	r, err := s.Repo(ctx, "repo_a")
	if err != nil || r.Path != "repo_a" || r.Name != "widgets" || r.Owner != "o1" || r.CreatedAt.IsZero() {
		t.Fatalf("repo = %+v, %v", r, err)
	}
	main, err := s.Ref(ctx, "native:repo_a", "main")
	if err != nil || main.Commit != commitA || main.Generation <= 0 {
		t.Fatalf("main = %+v, %v", main, err)
	}
	is(t, s.CreateRepo(ctx, meta.Repo{Path: "repo_a", Name: "again", Owner: "o2"}, "native:repo_a", commitB), meta.ErrExists)
	if again, _ := s.Repo(ctx, "repo_a"); again.Name != "widgets" {
		t.Errorf("a refused create changed the repo: %+v", again)
	}

	w, err := s.Claim(ctx, "ws1", 1, "o1", "native:repo_b")
	ok(t, err)
	_, err = s.SwapRef(ctx, "ws1", w.Epoch, "native:repo_b", "main", 0, commitA)
	ok(t, err)
	is(t, s.CreateRepo(ctx, meta.Repo{Path: "repo_b", Name: "b", Owner: "o1"}, "native:repo_b", commitB), meta.ErrExists)
	_, err = s.Repo(ctx, "repo_b")
	is(t, err, meta.ErrNotFound)
}

func reposNewestFirst(t *testing.T, s meta.Store) {
	for _, p := range []string{"repo_a", "repo_b", "repo_c"} {
		ok(t, s.CreateRepo(ctx, meta.Repo{Path: p, Name: p, Owner: "o1"}, "native:"+p, commitA))
		time.Sleep(5 * time.Millisecond)
	}
	got, err := s.Repos(ctx)
	if err != nil || len(got) != 3 || got[0].Path != "repo_c" || got[1].Path != "repo_b" || got[2].Path != "repo_a" {
		t.Errorf("repos = %+v, %v, want c, b, a", got, err)
	}
}

func oneRepoCreatorWins(t *testing.T, s meta.Store) {
	errs := race(func(i int) error {
		return s.CreateRepo(ctx, meta.Repo{Path: "repo_a", Name: fmt.Sprint(i), Owner: "o1"}, "native:repo_a", commitA)
	})
	onlyOneWins(t, errs, func(err error) bool { return errors.Is(err, meta.ErrExists) })
	refs, err := s.Refs(ctx, "native:repo_a")
	if err != nil || len(refs) != 1 {
		t.Errorf("refs = %+v, %v, want main once", refs, err)
	}
}

func claimRules(t *testing.T, s meta.Store) {
	if w := claim(t, s, "ws1", 3); w.Epoch != 3 || w.Op != "" || w.Snapshot != "" {
		t.Fatalf("first claim = %+v", w)
	}
	for _, c := range []struct{ asked, want uint64 }{{3, 3}, {5, 5}, {4, 5}, {1, 5}} {
		if w := claim(t, s, "ws1", c.asked); w.Epoch != c.want {
			t.Errorf("claim %d returned epoch %d, want %d", c.asked, w.Epoch, c.want)
		}
	}
	_, err := s.Claim(ctx, "ws1", 6, "o2", repoText)
	is(t, err, meta.ErrWorkspaceConflict)
	_, err = s.Claim(ctx, "ws1", 6, "o1", "gh:acme/other")
	is(t, err, meta.ErrWorkspaceConflict)
	if w := claim(t, s, "ws1", 5); w.Epoch != 5 {
		t.Errorf("a refused claim raised the epoch to %d", w.Epoch)
	}
}

func racingClaims(t *testing.T, s meta.Store) {
	epochs := make([]uint64, racers)
	errs := race(func(i int) error {
		w, err := s.Claim(ctx, "ws1", uint64(i+1), "o1", repoText)
		epochs[i] = w.Epoch
		return err
	})
	for i, err := range errs {
		ok(t, err)
		if epochs[i] < uint64(i+1) {
			t.Errorf("claim %d returned the lower epoch %d", i+1, epochs[i])
		}
	}
	ok(t, s.Live(ctx, "ws1", racers, repoText))
	is(t, s.Live(ctx, "ws1", racers-1, repoText), meta.ErrClaimLost)
}

func liveOrder(t *testing.T, s meta.Store) {
	is(t, s.Live(ctx, "ws1", 1, repoText), meta.ErrNotFound)
	claim(t, s, "ws1", 2)
	ok(t, s.Live(ctx, "ws1", 2, repoText))
	is(t, s.Live(ctx, "ws1", 1, "gh:acme/other"), meta.ErrWrongRepo)
	is(t, s.Live(ctx, "ws1", 1, repoText), meta.ErrClaimLost)
	is(t, s.Live(ctx, "ws1", 3, repoText), meta.ErrClaimLost)
	ok(t, s.Delete(ctx, "ws1"))
	is(t, s.Live(ctx, "ws1", 1, "gh:acme/other"), meta.ErrGone)
}

func deleteRules(t *testing.T, s meta.Store) {
	ok(t, s.Delete(ctx, "unknown"))
	claim(t, s, "ws1", 1)
	ref, err := s.SwapRef(ctx, "ws1", 1, repoText, "keep", 0, commitA)
	ok(t, err)
	for range 2 {
		ok(t, s.Delete(ctx, "ws1"))
	}
	_, err = s.Claim(ctx, "ws1", 2, "o1", repoText)
	is(t, err, meta.ErrGone)
	_, err = s.Claim(ctx, "ws1", 2, "o2", repoText)
	is(t, err, meta.ErrGone)
	_, err = s.SwapRef(ctx, "ws1", 1, repoText, "new", 0, commitA)
	is(t, err, meta.ErrClaimLost)
	is(t, s.DeleteRef(ctx, "ws1", 1, repoText, "keep", ref.Generation), meta.ErrClaimLost)
	_, err = s.SwapOpHeads(ctx, "ws1", 1, repoText, 0, []string{opA})
	is(t, err, meta.ErrClaimLost)
	is(t, s.Report(ctx, "ws1", 1, repoText, 1, "", opA, commitA), meta.ErrClaimLost)
	if got, err := s.Ref(ctx, repoText, "keep"); err != nil || got != ref {
		t.Errorf("keep = %+v, %v after refused writes, want %+v", got, err, ref)
	}
}

// staleEpochNeverWrites reaches the write statements as a request that passed an epoch check and then lost the race
// would.
func staleEpochNeverWrites(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 2)
	ref, err := s.SwapRef(ctx, "ws1", 2, repoText, "keep", 0, commitA)
	ok(t, err)
	heads, err := s.SwapOpHeads(ctx, "ws1", 2, repoText, 0, []string{opA})
	ok(t, err)
	writes := map[string]func() error{
		"ref create": func() error { _, err := s.SwapRef(ctx, "ws1", 1, repoText, "new", 0, commitA); return err },
		"ref swap": func() error {
			_, err := s.SwapRef(ctx, "ws1", 1, repoText, "keep", ref.Generation, commitB)
			return err
		},
		"ref swap with a wrong generation": func() error {
			_, err := s.SwapRef(ctx, "ws1", 1, repoText, "keep", ref.Generation+100, commitB)
			return err
		},
		"ref delete": func() error { return s.DeleteRef(ctx, "ws1", 1, repoText, "keep", ref.Generation) },
		"ref delete with a wrong generation": func() error {
			return s.DeleteRef(ctx, "ws1", 1, repoText, "keep", ref.Generation+100)
		},
		"op heads": func() error {
			_, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, heads.Generation, []string{opB})
			return err
		},
		"op heads with a wrong generation": func() error {
			_, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, heads.Generation+100, []string{opB})
			return err
		},
		"first heads": func() error { _, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, 0, []string{opB}); return err },
		"report":      func() error { return s.Report(ctx, "ws1", 1, repoText, 1, "", opA, commitA) },
		"flush":       func() error { return s.Flush(ctx, "ws1", 1, 1, "", opA, commitA) },
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) { is(t, write(), meta.ErrClaimLost) })
	}
	if got, err := s.Refs(ctx, repoText); err != nil || len(got) != 1 || got[0].Commit != commitA || got[0].Generation != ref.Generation {
		t.Errorf("refs = %+v, %v after refused writes", got, err)
	}
	if got, err := s.OpHeads(ctx, "ws1"); err != nil || got.Generation != heads.Generation || got.Heads[0] != opA {
		t.Errorf("op heads = %+v, %v after refused writes", got, err)
	}
}

func claimFencesWritesInFlight(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	var started sync.WaitGroup
	started.Add(racers)
	final := make([]error, racers)
	var wg sync.WaitGroup
	for g := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				if _, err := s.SwapRef(ctx, "ws1", 1, repoText, fmt.Sprintf("w%d-%d", g, i), 0, commitA); err != nil {
					final[g] = err
					return
				}
				if i == 2 {
					started.Done()
				}
			}
		}()
	}
	started.Wait()
	claim(t, s, "ws1", 2)
	atClaim, err := s.Refs(ctx, repoText)
	ok(t, err)
	wg.Wait()
	for g, err := range final {
		if !errors.Is(err, meta.ErrClaimLost) {
			t.Errorf("writer %d stopped on %v, want meta.ErrClaimLost", g, err)
		}
	}
	if after, err := s.Refs(ctx, repoText); err != nil || len(after) != len(atClaim) {
		t.Errorf("%d refs after the claim returned, %d when it returned: a write of the old epoch landed later (%v)", len(after), len(atClaim), err)
	}
}

func reportOrdering(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	report := func(gen int64, prev, op, snap string) {
		t.Helper()
		ok(t, s.Report(ctx, "ws1", 1, repoText, gen, prev, op, snap))
	}
	state := func() meta.Workspace { return claim(t, s, "ws1", 1) }
	report(0, "", opA, commitA)
	if w := state(); w.Op != opA || w.Snapshot != commitA {
		t.Fatalf("state = %+v, want the generation 0 write at an empty op", w)
	}
	report(0, opB, opC, commitB)
	if w := state(); w.Op != opA {
		t.Errorf("a generation 0 report with another prev wrote: %+v", w)
	}
	report(5, "", opB, commitA)
	report(3, "", opC, commitB)
	if w := state(); w.Op != opB || w.Snapshot != commitA {
		t.Errorf("state = %+v, want the generation 5 write: a lower generation is dropped", w)
	}
	report(5, "", opC, commitB)
	if w := state(); w.Op != opC || w.Snapshot != commitB {
		t.Errorf("state = %+v, want the second generation 5 write: an equal generation writes again", w)
	}
	report(0, opC, opA, commitA)
	if w := state(); w.Op != opA {
		t.Errorf("state = %+v, want a generation 0 write that matches prev", w)
	}
	report(4, "", opB, commitB)
	if w := state(); w.Op != opA {
		t.Errorf("state = %+v, want a lower generation dropped after a generation 0 write", w)
	}
}

func reportAndFlushFence(t *testing.T, s meta.Store) {
	is(t, s.Flush(ctx, "ws1", 1, 1, "", opA, commitA), meta.ErrNotFound)
	is(t, s.Report(ctx, "ws1", 1, repoText, 1, "", opA, commitA), meta.ErrClaimLost)
	claim(t, s, "ws1", 2)
	is(t, s.Report(ctx, "ws1", 2, "gh:acme/other", 1, "", opA, commitA), meta.ErrClaimLost)
	is(t, s.Report(ctx, "ws1", 3, repoText, 1, "", opA, commitA), meta.ErrClaimLost)
	is(t, s.Flush(ctx, "ws1", 3, 1, "", opA, commitA), meta.ErrClaimLost)
	ok(t, s.Flush(ctx, "ws1", 2, 7, "", opB, commitB))
	if w := claim(t, s, "ws1", 2); w.Op != opB || w.Snapshot != commitB {
		t.Errorf("state = %+v, want the flush", w)
	}
	ok(t, s.Flush(ctx, "ws1", 2, 6, "", opA, commitA))
	if w := claim(t, s, "ws1", 2); w.Op != opB {
		t.Errorf("state = %+v, want a lower generation dropped without an error", w)
	}
	ok(t, s.Delete(ctx, "ws1"))
	is(t, s.Flush(ctx, "ws1", 2, 8, "", opA, commitA), meta.ErrGone)
	is(t, s.Flush(ctx, "ws1", 1, 8, "", opA, commitA), meta.ErrGone)
}

func opHeadsSwap(t *testing.T, s meta.Store) {
	_, err := s.OpHeads(ctx, "ws1")
	is(t, err, meta.ErrNotFound)
	claim(t, s, "ws1", 1)
	c := opHeadsConflict(t, func() error { _, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, 4, []string{opA}); return err }())
	if c.Current != nil {
		t.Errorf("a swap with no stored heads carries %+v, want nil", c.Current)
	}
	h, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, 0, []string{opA})
	if err != nil || h.Generation != 1 || h.Epoch != 1 || len(h.Heads) != 1 {
		t.Fatalf("first swap = %+v, %v", h, err)
	}
	for _, expect := range []int64{0, 7} {
		_, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, expect, []string{opB})
		if c := opHeadsConflict(t, err); c.Current == nil || c.Current.Generation != 1 || c.Current.Heads[0] != opA {
			t.Errorf("expect %d: conflict carries %+v, want the stored heads", expect, c.Current)
		}
	}
	claim(t, s, "ws1", 2)
	h, err = s.SwapOpHeads(ctx, "ws1", 2, repoText, 1, []string{opB, opC})
	if err != nil || h.Generation != 2 || h.Epoch != 2 {
		t.Fatalf("second swap = %+v, %v", h, err)
	}
	if got, err := s.OpHeads(ctx, "ws1"); err != nil || got.Epoch != 2 || got.Generation != 2 || len(got.Heads) != 2 || got.Heads[0] != opB || got.Heads[1] != opC {
		t.Errorf("op heads = %+v, %v", got, err)
	}
	_, err = s.SwapOpHeads(ctx, "ws1", 2, "gh:acme/other", 2, []string{opA})
	is(t, err, meta.ErrClaimLost)
}

func oneOpHeadsSwapWins(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	isConflict := func(err error) bool { var c *meta.OpHeadsConflict; return errors.As(err, &c) }
	for expect := int64(0); expect < 2; expect++ {
		errs := race(func(i int) error {
			_, err := s.SwapOpHeads(ctx, "ws1", 1, repoText, expect, []string{fmt.Sprintf("%016x", i)})
			return err
		})
		onlyOneWins(t, errs, isConflict)
	}
	if h, err := s.OpHeads(ctx, "ws1"); err != nil || h.Generation != 2 {
		t.Errorf("op heads = %+v, %v, want generation 2", h, err)
	}
}

func refSwapAndDelete(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	_, err := s.Ref(ctx, repoText, "x")
	is(t, err, meta.ErrNotFound)
	if c := refConflict(t, s.DeleteRef(ctx, "ws1", 1, repoText, "x", 5)); c.Current != nil {
		t.Errorf("delete of a missing ref carries %+v, want nil", c.Current)
	}
	_, err = s.SwapRef(ctx, "ws1", 1, repoText, "x", 5, commitA)
	if c := refConflict(t, err); c.Current != nil {
		t.Errorf("swap of a missing ref carries %+v, want nil", c.Current)
	}
	first, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", 0, commitA)
	if err != nil || first.Name != "x" || first.Commit != commitA || first.Generation <= 0 {
		t.Fatalf("create = %+v, %v", first, err)
	}
	for _, expect := range []int64{0, first.Generation + 100} {
		_, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", expect, commitB)
		if c := refConflict(t, err); c.Current == nil || *c.Current != first {
			t.Errorf("expect %d: conflict carries %+v, want %+v", expect, c.Current, first)
		}
	}
	if c := refConflict(t, s.DeleteRef(ctx, "ws1", 1, repoText, "x", first.Generation+100)); c.Current == nil || *c.Current != first {
		t.Errorf("delete conflict carries %+v, want %+v", c.Current, first)
	}
	second, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", first.Generation, commitB)
	if err != nil || second.Commit != commitB || second.Generation == first.Generation {
		t.Fatalf("swap = %+v, %v", second, err)
	}
	if got, err := s.Ref(ctx, repoText, "x"); err != nil || got != second {
		t.Errorf("ref = %+v, %v, want %+v", got, err, second)
	}
	refConflict(t, s.DeleteRef(ctx, "ws1", 1, repoText, "x", first.Generation))
	ok(t, s.DeleteRef(ctx, "ws1", 1, repoText, "x", second.Generation))
	_, err = s.Ref(ctx, repoText, "x")
	is(t, err, meta.ErrNotFound)
	_, err = s.SwapRef(ctx, "ws1", 1, "gh:acme/other", "x", 0, commitA)
	is(t, err, meta.ErrClaimLost)
}

func refGenerationNeverRepeats(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	seen := map[int64]bool{}
	for i := range 4 {
		r, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", 0, commitA)
		ok(t, err)
		if r.Generation <= 0 || seen[r.Generation] {
			t.Fatalf("create %d took generation %d, which is not positive or was taken: %v", i, r.Generation, seen)
		}
		seen[r.Generation] = true
		other, err := s.SwapRef(ctx, "ws1", 1, repoText, fmt.Sprint("y", i), 0, commitA)
		ok(t, err)
		if seen[other.Generation] {
			t.Fatalf("generation %d was taken twice", other.Generation)
		}
		seen[other.Generation] = true
		ok(t, s.DeleteRef(ctx, "ws1", 1, repoText, "x", r.Generation))
	}
}

func oneRefSwapWins(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	isConflict := func(err error) bool { var c *meta.RefConflict; return errors.As(err, &c) }
	errs := race(func(i int) error {
		_, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", 0, fmt.Sprintf("%040x", i))
		return err
	})
	onlyOneWins(t, errs, isConflict)
	first, err := s.Ref(ctx, repoText, "x")
	ok(t, err)
	errs = race(func(i int) error {
		_, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", first.Generation, fmt.Sprintf("%040x", i))
		return err
	})
	onlyOneWins(t, errs, isConflict)
	errs = race(func(int) error { return s.DeleteRef(ctx, "ws1", 1, repoText, "x", first.Generation) })
	for _, err := range errs {
		refConflict(t, err)
	}
	cur, err := s.Ref(ctx, repoText, "x")
	ok(t, err)
	errs = race(func(int) error { return s.DeleteRef(ctx, "ws1", 1, repoText, "x", cur.Generation) })
	onlyOneWins(t, errs, isConflict)
}

func updateRefRules(t *testing.T, s meta.Store) {
	_, err := s.UpdateRef(ctx, repoText, "x", commitA, commitB, meta.WriterWrite)
	if c := refConflict(t, err); c.Current != nil {
		t.Errorf("update of a missing ref carries %+v, want nil", c.Current)
	}
	if c := refConflict(t, deleteVia(s, "x", commitA)); c.Current != nil {
		t.Errorf("delete of a missing ref carries %+v, want nil", c.Current)
	}
	first, err := s.UpdateRef(ctx, repoText, "x", "", commitA, meta.WriterWrite)
	if err != nil || first.Name != "x" || first.Commit != commitA || first.Generation <= 0 {
		t.Fatalf("create = %+v, %v", first, err)
	}
	_, err = s.UpdateRef(ctx, repoText, "x", "", commitB, meta.WriterWrite)
	if c := refConflict(t, err); c.Current == nil || *c.Current != first {
		t.Errorf("create over a ref carries %+v, want %+v", c.Current, first)
	}
	_, err = s.UpdateRef(ctx, repoText, "x", commitB, commitB, meta.WriterWrite)
	if c := refConflict(t, err); c.Current == nil || *c.Current != first {
		t.Errorf("stale update carries %+v, want %+v", c.Current, first)
	}
	if c := refConflict(t, deleteVia(s, "x", commitB)); c.Current == nil || *c.Current != first {
		t.Errorf("stale delete carries %+v, want %+v", c.Current, first)
	}
	second, err := s.UpdateRef(ctx, repoText, "x", commitA, commitB, meta.WriterWrite)
	if err != nil || second.Commit != commitB || second.Generation == first.Generation {
		t.Fatalf("update = %+v, %v", second, err)
	}
	if got, err := s.Ref(ctx, repoText, "x"); err != nil || got != second {
		t.Errorf("ref = %+v, %v, want %+v", got, err, second)
	}
	ok(t, deleteVia(s, "x", commitB))
	_, err = s.Ref(ctx, repoText, "x")
	is(t, err, meta.ErrNotFound)
	third, err := s.UpdateRef(ctx, repoText, "x", "", commitA, meta.WriterWrite)
	if err != nil || third.Generation <= second.Generation {
		t.Errorf("recreate = %+v, %v, want a generation above %d", third, err, second.Generation)
	}
}

func deleteVia(s meta.Store, name, old string) error {
	_, err := s.UpdateRef(ctx, repoText, name, old, "", meta.WriterWrite)
	return err
}

func oneRefUpdateWins(t *testing.T, s meta.Store) {
	isConflict := func(err error) bool { var c *meta.RefConflict; return errors.As(err, &c) }
	errs := race(func(i int) error {
		_, err := s.UpdateRef(ctx, repoText, "x", "", fmt.Sprintf("%040x", i), meta.WriterWrite)
		return err
	})
	onlyOneWins(t, errs, isConflict)
	cur, err := s.Ref(ctx, repoText, "x")
	ok(t, err)
	errs = race(func(i int) error {
		_, err := s.UpdateRef(ctx, repoText, "x", cur.Commit, fmt.Sprintf("%040x", i+100), meta.WriterWrite)
		return err
	})
	onlyOneWins(t, errs, isConflict)
}

func refsList(t *testing.T, s meta.Store) {
	claim(t, s, "ws1", 1)
	if _, err := s.Claim(ctx, "ws2", 1, "o1", "gh:acme/other"); err != nil {
		t.Fatal(err)
	}
	names := []string{"b", "a/b", "a", "B", "a-b", "é"}
	for _, n := range names {
		_, err := s.SwapRef(ctx, "ws1", 1, repoText, n, 0, commitA)
		ok(t, err)
	}
	_, err := s.SwapRef(ctx, "ws2", 1, "gh:acme/other", "z", 0, commitB)
	ok(t, err)
	got, err := s.Refs(ctx, repoText)
	want := []string{"B", "a", "a-b", "a/b", "b", "é"}
	if err != nil || len(got) != len(want) {
		t.Fatalf("refs = %+v, %v", got, err)
	}
	for i, r := range got {
		if r.Name != want[i] || r.Commit != commitA || r.Generation <= 0 {
			t.Errorf("refs[%d] = %+v, want %q in byte order", i, r, want[i])
		}
	}
	if none, err := s.Refs(ctx, "gh:acme/none"); err != nil || len(none) != 0 {
		t.Errorf("refs of an unknown repo = %+v, %v", none, err)
	}
}

func packs(t *testing.T, s meta.Store) {
	for _, id := range []string{"p3", "p1", "p2"} {
		ok(t, s.AddPack(ctx, repoText, id, int64(len(id))*10))
		time.Sleep(5 * time.Millisecond)
	}
	ok(t, s.AddPack(ctx, repoText, "p3", 999))
	ok(t, s.AddPack(ctx, "gh:acme/other", "p9", 1))
	got, err := s.Packs(ctx, repoText)
	want := []meta.Pack{{ID: "p3", Bytes: 20}, {ID: "p1", Bytes: 20}, {ID: "p2", Bytes: 20}}
	if err != nil || len(got) != len(want) {
		t.Fatalf("packs = %+v, %v", got, err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("packs[%d] = %+v, want %+v: oldest first, a repeat keeps the first record", i, got[i], want[i])
		}
	}
	if none, err := s.Packs(ctx, "gh:acme/none"); err != nil || len(none) != 0 {
		t.Errorf("packs of an unknown repo = %+v, %v", none, err)
	}
	if repos, err := s.PackRepos(ctx); err != nil || len(repos) != 2 || repos[0] != "gh:acme/other" || repos[1] != repoText {
		t.Errorf("pack repos = %v, %v, want each repo of a pack once", repos, err)
	}
}
