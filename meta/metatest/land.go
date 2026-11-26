package metatest

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/majorcontext/motte/meta"
)

const (
	commitC = "3333333333333333333333333333333333333333"
	landRef = "main"
)

var (
	t0    = time.Unix(1_700_000_000, 0)
	lease = 30 * time.Second
)

var landCases = []struct {
	name string
	fn   func(*testing.T, meta.Store)
}{
	{"a land is queued once for each id and claimed in arrival order", landQueueOrder},
	{"one owner leads a ref until its lease ends", landLeaseExclusive},
	{"a new leader claims what a lost leader did not end", landCrashRecovery},
	{"a land batch moves the ref, logs, ends its items and appends one event in one unit", landBatchIsAtomic},
	{"a land batch whose condition fails changes nothing", landBatchLoses},
	{"one racer of many moves a ref with a land batch", oneLandBatchWins},
	{"items end with no ref move while the lease holds", landFinish},
	{"deleting a ref deletes its land log and lease", landRefDeleteDropsMeta},
	{"the land log answers from the latest row of a base and keeps a bounded tail", landLogRules},
}

func seedLandRef(t *testing.T, s meta.Store) meta.Ref {
	t.Helper()
	r, err := s.MergeRef(ctx, repoText, landRef, 0, commitA)
	ok(t, err)
	return r
}

func enqueue(t *testing.T, s meta.Store, id string) meta.LandItem {
	t.Helper()
	it, err := s.EnqueueLand(ctx, meta.LandRequest{ID: id, Repo: repoText, Ref: landRef, Base: commitA, Head: commitB, Kind: meta.WriterMerge}, t0)
	ok(t, err)
	return it
}

// acquire is AcquireLandLease without the end of the lease.
func acquire(s meta.Store, repo, ref, owner string, now, until time.Time) error {
	_, err := s.AcquireLandLease(ctx, repo, ref, owner, now, until)
	return err
}

func lead(t *testing.T, s meta.Store, owner string) {
	t.Helper()
	ok(t, acquire(s, repoText, landRef, owner, t0, t0.Add(lease)))
}

func claimIDs(t *testing.T, s meta.Store, owner string, limit int) []string {
	t.Helper()
	items, err := s.ClaimLands(ctx, repoText, landRef, owner, t0, limit)
	ok(t, err)
	var ids []string
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	return ids
}

func landQueueOrder(t *testing.T, s meta.Store) {
	a, b := enqueue(t, s, "a"), enqueue(t, s, "b")
	if again := enqueue(t, s, "a"); again.Seq != a.Seq || again.Done || again.Base != commitA {
		t.Errorf("a repeated id = %+v, want the first item %+v", again, a)
	}
	enqueue(t, s, "c")
	if b.Seq <= a.Seq {
		t.Errorf("seq of b = %d, want above %d", b.Seq, a.Seq)
	}
	is(t, func() error { _, err := s.LandItem(ctx, repoText, "none"); return err }(), meta.ErrNotFound)
	keys, err := s.LandPending(ctx)
	ok(t, err)
	if len(keys) != 1 || keys[0] != (meta.LandKey{Repo: repoText, Ref: landRef}) {
		t.Errorf("pending = %v", keys)
	}
	_, err = s.ClaimLands(ctx, repoText, landRef, "o1", t0, 10)
	is(t, err, meta.ErrLeaseHeld)
	lead(t, s, "o1")
	if got := claimIDs(t, s, "o1", 2); fmt.Sprint(got) != "[a b]" {
		t.Errorf("first claim = %v, want [a b]", got)
	}
}

func landLeaseExclusive(t *testing.T, s meta.Store) {
	lead(t, s, "o1")
	is(t, acquire(s, repoText, landRef, "o2", t0.Add(lease-time.Second), t0.Add(2*lease)), meta.ErrLeaseHeld)
	end, err := s.AcquireLandLease(ctx, repoText, landRef, "o2", t0.Add(lease-time.Second), t0.Add(2*lease))
	is(t, err, meta.ErrLeaseHeld)
	if !end.Equal(t0.Add(lease)) {
		t.Errorf("lease end reported to a loser = %v, want %v", end, t0.Add(lease))
	}
	ok(t, acquire(s, repoText, landRef, "o1", t0.Add(time.Second), t0.Add(lease+time.Second)))
	ok(t, acquire(s, "gh:acme/other", landRef, "o2", t0, t0.Add(lease)))
	ok(t, s.ReleaseLandLease(ctx, repoText, landRef, "o2"))
	is(t, acquire(s, repoText, landRef, "o2", t0.Add(2*time.Second), t0.Add(2*lease)), meta.ErrLeaseHeld)
	ok(t, s.ReleaseLandLease(ctx, repoText, landRef, "o1"))
	ok(t, acquire(s, repoText, landRef, "o2", t0.Add(2*time.Second), t0.Add(2*lease)))
	ok(t, acquire(s, repoText, landRef, "o3", t0.Add(2*lease), t0.Add(3*lease)))
	onlyOneWins(t, race(func(i int) error {
		return acquire(s, repoText, "raced", fmt.Sprint("o", i), t0, t0.Add(lease))
	}), func(err error) bool { return err == meta.ErrLeaseHeld })
}

func landCrashRecovery(t *testing.T, s meta.Store) {
	for _, id := range []string{"a", "b", "c"} {
		enqueue(t, s, id)
	}
	lead(t, s, "o1")
	claimIDs(t, s, "o1", 2)
	ok(t, s.FinishLands(ctx, repoText, landRef, "o1", t0, []meta.LandDone{{ID: "a", Result: "r"}}))
	later := t0.Add(2 * lease)
	_, err := s.ClaimLands(ctx, repoText, landRef, "o1", later, 10)
	is(t, err, meta.ErrLeaseHeld)
	ok(t, acquire(s, repoText, landRef, "o2", later, later.Add(lease)))
	items, err := s.ClaimLands(ctx, repoText, landRef, "o2", later, 10)
	ok(t, err)
	if len(items) != 2 || items[0].ID != "b" || items[1].ID != "c" {
		t.Errorf("new leader claimed %+v, want the claimed b and the queued c", items)
	}
	is(t, s.FinishLands(ctx, repoText, landRef, "o1", later, []meta.LandDone{{ID: "b", Result: "x"}}), meta.ErrLeaseHeld)
	if it, err := s.LandItem(ctx, repoText, "b"); err != nil || it.Done {
		t.Errorf("b = %+v, %v, want it open: a lost leader ends nothing", it, err)
	}
	if it, err := s.LandItem(ctx, repoText, "a"); err != nil || !it.Done || it.Result != "r" {
		t.Errorf("a = %+v, %v, want it done with its result", it, err)
	}
}

func landMove(old, to string, ids ...string) meta.LandMove {
	m := meta.LandMove{Repo: repoText, Ref: landRef, Old: old, New: to, Kind: meta.WriterMerge,
		Rows: []meta.LandLogRow{{Old: old, New: commitC, Paths: []string{"a", "b/c"}}, {Old: commitC, New: to}}}
	for _, id := range ids {
		m.Results = append(m.Results, meta.LandDone{ID: id, Result: "done " + id, Landed: id == "a"})
	}
	return m
}

func landBatchIsAtomic(t *testing.T, s meta.Store) {
	seed := seedLandRef(t, s)
	enqueue(t, s, "a")
	enqueue(t, s, "b")
	lead(t, s, "o1")
	claimIDs(t, s, "o1", 10)
	ref, err := s.LandRef(ctx, landMove(commitA, commitB, "a", "b"), "o1", t0)
	ok(t, err)
	if got, err := s.Ref(ctx, repoText, landRef); err != nil || got != ref || ref.Commit != commitB || ref.Generation <= seed.Generation {
		t.Fatalf("ref = %+v, %v, want the moved %+v above generation %d", got, err, ref, seed.Generation)
	}
	for _, id := range []string{"a", "b"} {
		want := int64(0)
		if id == "a" {
			want = ref.Generation
		}
		if it, err := s.LandItem(ctx, repoText, id); err != nil || !it.Done || it.Result != "done "+id || it.Generation != want {
			t.Errorf("item %s = %+v, %v, want generation %d", id, it, err, want)
		}
	}
	evs := refEvents(t, s, repoText, landRef)
	if len(evs) != 2 || evs[1].Old != commitA || evs[1].New != commitB || evs[1].Kind != meta.WriterMerge || evs[1].Generation != ref.Generation {
		t.Errorf("events = %+v, want the create and one event for the batch", evs)
	}
	rows, err := s.LandLogSince(ctx, repoText, landRef, commitA, 10)
	ok(t, err)
	if len(rows) != 2 || rows[0].New != commitC || !slices.Equal(rows[0].Paths, []string{"a", "b/c"}) || rows[1].New != commitB {
		t.Errorf("log = %+v", rows)
	}
	if pending, _ := s.LandPending(ctx); len(pending) != 0 {
		t.Errorf("pending = %v, want none", pending)
	}
}

func landBatchLoses(t *testing.T, s meta.Store) {
	seed := seedLandRef(t, s)
	enqueue(t, s, "a")
	lead(t, s, "o1")
	claimIDs(t, s, "o1", 10)
	before := len(refEvents(t, s, repoText, landRef))
	assertUnchanged := func(why string) {
		t.Helper()
		if got, _ := s.Ref(ctx, repoText, landRef); got != seed {
			t.Errorf("%s: ref = %+v, want %+v", why, got, seed)
		}
		if it, _ := s.LandItem(ctx, repoText, "a"); it.Done {
			t.Errorf("%s: item ended", why)
		}
		if rows, _ := s.LandLogSince(ctx, repoText, landRef, commitA, 10); len(rows) != 0 {
			t.Errorf("%s: log = %+v", why, rows)
		}
		if n := len(refEvents(t, s, repoText, landRef)); n != before {
			t.Errorf("%s: %d events, want %d", why, n, before)
		}
	}
	_, err := s.LandRef(ctx, landMove(commitB, commitC, "a"), "o1", t0)
	if c := refConflict(t, err); c.Current == nil || *c.Current != seed {
		t.Errorf("conflict carries %+v, want %+v", c.Current, seed)
	}
	assertUnchanged("a ref that is not at the old tip")
	_, err = s.LandRef(ctx, landMove(commitA, commitB, "a"), "o2", t0)
	is(t, err, meta.ErrLeaseHeld)
	assertUnchanged("an owner without the lease")
	_, err = s.LandRef(ctx, landMove(commitA, commitB, "a"), "o1", t0.Add(2*lease))
	is(t, err, meta.ErrLeaseHeld)
	assertUnchanged("an expired lease")
}

func oneLandBatchWins(t *testing.T, s meta.Store) {
	seedLandRef(t, s)
	lead(t, s, "o1")
	onlyOneWins(t, race(func(i int) error {
		m := landMove(commitA, commitB)
		m.Rows = []meta.LandLogRow{{Old: commitA, New: commitB, Paths: []string{fmt.Sprint("p", i)}}}
		_, err := s.LandRef(ctx, m, "o1", t0)
		return err
	}), func(err error) bool { _, ok := err.(*meta.RefConflict); return ok })
	if n := len(refEvents(t, s, repoText, landRef)); n != 2 {
		t.Errorf("%d events, want the create and one batch", n)
	}
	if rows, _ := s.LandLogSince(ctx, repoText, landRef, commitA, 10); len(rows) != 1 {
		t.Errorf("log = %+v, want the row of the one winner", rows)
	}
}

func landFinish(t *testing.T, s meta.Store) {
	seed := seedLandRef(t, s)
	enqueue(t, s, "a")
	is(t, s.FinishLands(ctx, repoText, landRef, "o1", t0, []meta.LandDone{{ID: "a", Result: "r"}}), meta.ErrLeaseHeld)
	lead(t, s, "o1")
	ok(t, s.FinishLands(ctx, repoText, landRef, "o1", t0, []meta.LandDone{{ID: "a", Result: "r"}, {ID: "unknown", Result: "r"}}))
	if it, _ := s.LandItem(ctx, repoText, "a"); !it.Done || it.Result != "r" {
		t.Errorf("item = %+v", it)
	}
	ok(t, s.FinishLands(ctx, repoText, landRef, "o1", t0, []meta.LandDone{{ID: "a", Result: "second"}}))
	if it, _ := s.LandItem(ctx, repoText, "a"); it.Result != "r" {
		t.Errorf("result = %q after a second end, want an item that is done to stay as it was", it.Result)
	}
	if got, _ := s.Ref(ctx, repoText, landRef); got != seed {
		t.Errorf("ref = %+v, want it unmoved", got)
	}
	ok(t, s.PruneLands(ctx, t0.Add(-time.Hour)))
	if _, err := s.LandItem(ctx, repoText, "a"); err != nil {
		t.Errorf("an item that ended after the cut was pruned: %v", err)
	}
	ok(t, s.PruneLands(ctx, time.Now().Add(time.Hour)))
	is(t, func() error { _, err := s.LandItem(ctx, repoText, "a"); return err }(), meta.ErrNotFound)
}

func landLogRules(t *testing.T, s meta.Store) {
	seedLandRef(t, s)
	lead(t, s, "o1")
	const keep, n = 50, 55
	tip := func(i int) string { return fmt.Sprintf("%040x", i+1) }
	for from := 0; from < n; from += 20 {
		to := min(from+20, n)
		old := commitA
		if from > 0 {
			old = tip(from)
		}
		m := meta.LandMove{Repo: repoText, Ref: landRef, Old: old, New: tip(to), Kind: meta.WriterWrite, Keep: keep}
		for i := from + 1; i <= to; i++ {
			prev := commitA
			if i > 1 {
				prev = tip(i - 1)
			}
			row := meta.LandLogRow{Old: prev, New: tip(i), Paths: []string{fmt.Sprint("f", i)}}
			if i == n-2 {
				row.Paths = nil
			}
			m.Rows = append(m.Rows, row)
		}
		_, err := s.LandRef(ctx, m, "o1", t0)
		ok(t, err)
	}
	rows, err := s.LandLogSince(ctx, repoText, landRef, tip(n-4), 10)
	ok(t, err)
	if len(rows) != 4 || rows[0].Old != tip(n-4) || rows[0].Paths == nil || rows[1].Paths != nil || rows[3].New != tip(n) {
		t.Errorf("rows after a base that ends a row = %+v, want 4 with the overflow second", rows)
	}
	if rows, err = s.LandLogSince(ctx, repoText, landRef, tip(n-2), 10); err != nil || len(rows) != 2 || rows[0].Old != tip(n-2) {
		t.Errorf("rows from a base that is the old tip of a row = %+v, %v", rows, err)
	}
	for name, base := range map[string]string{"unknown": commitC, "the tip": tip(n), "pruned": tip(n - keep - 1)} {
		if rows, err := s.LandLogSince(ctx, repoText, landRef, base, 10); err != nil || len(rows) != 0 {
			t.Errorf("%s: rows = %+v, %v, want none", name, rows, err)
		}
	}
	if rows, err := s.LandLogSince(ctx, repoText, landRef, tip(n-keep), 2*n); err != nil || len(rows) != keep {
		t.Errorf("rows kept = %d, %v, want the last %d", len(rows), err, keep)
	}
	if rows, _ := s.LandLogSince(ctx, repoText, landRef, tip(n-4), 2); rows != nil {
		t.Errorf("a window of 2 for 4 rows = %+v, want none", rows)
	}
}

func landRefDeleteDropsMeta(t *testing.T, s meta.Store) {
	for name, del := range map[string]func(meta.Ref) error{
		"delete by generation": func(r meta.Ref) error { return s.MergeDeleteRef(ctx, repoText, landRef, r.Generation) },
		"delete by commit": func(r meta.Ref) error {
			_, err := s.UpdateRef(ctx, repoText, landRef, r.Commit, "", meta.WriterMerge)
			return err
		},
	} {
		seedLandRef(t, s)
		lead(t, s, "o1")
		ref, err := s.LandRef(ctx, landMove(commitA, commitB), "o1", t0)
		ok(t, err)
		ok(t, del(ref))
		if rows, err := s.LandLogSince(ctx, repoText, landRef, commitA, 10); err != nil || len(rows) != 0 {
			t.Errorf("%s: log = %+v, %v, want none", name, rows, err)
		}
		ok(t, acquire(s, repoText, landRef, "o2", t0, t0.Add(lease)))
		ok(t, s.ReleaseLandLease(ctx, repoText, landRef, "o2"))
	}
}
