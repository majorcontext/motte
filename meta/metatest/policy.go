package metatest

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/majorcontext/motte/meta"
)

var policyCases = []struct {
	name string
	fn   func(*testing.T, meta.Store)
}{
	{"merge writes a ref without a workspace", mergeRules},
	{"one racer of many merges a ref", oneMergeWins},
	{"every ref write appends one event in its unit", eventPerWrite},
	{"event sequence is gap free for a concurrent reader", eventSequenceGapFree},
	{"pruned events refuse an older cursor", eventPrune},
	{"protected refs are kept for each repo", protectedRefs},
	{"subscriptions start at the newest event", subscriptions},
	{"one owner holds a lease", leaseExclusive},
}

func swapErr(_ meta.Ref, err error) error { return err }

func mergeRules(t *testing.T, s meta.Store) {
	first, err := s.MergeRef(ctx, repoText, "main", 0, commitA)
	if err != nil || first.Commit != commitA || first.Generation <= 0 {
		t.Fatalf("create = %+v, %v", first, err)
	}
	if c := refConflict(t, swapErr(s.MergeRef(ctx, repoText, "main", 0, commitB))); c.Current == nil || *c.Current != first {
		t.Errorf("create over a ref carries %+v, want %+v", c.Current, first)
	}
	if c := refConflict(t, swapErr(s.MergeRef(ctx, repoText, "gone", 7, commitB))); c.Current != nil {
		t.Errorf("swap of a missing ref carries %+v, want nil", c.Current)
	}
	second, err := s.MergeRef(ctx, repoText, "main", first.Generation, commitB)
	if err != nil || second.Commit != commitB || second.Generation == first.Generation {
		t.Fatalf("swap = %+v, %v", second, err)
	}
	refConflict(t, s.MergeDeleteRef(ctx, repoText, "main", first.Generation))
	ok(t, s.MergeDeleteRef(ctx, repoText, "main", second.Generation))
	_, err = s.Ref(ctx, repoText, "main")
	is(t, err, meta.ErrNotFound)
}

func oneMergeWins(t *testing.T, s meta.Store) {
	isConflict := func(err error) bool { _, is := err.(*meta.RefConflict); return is }
	onlyOneWins(t, race(func(i int) error {
		return swapErr(s.MergeRef(ctx, repoText, "x", 0, fmt.Sprintf("%040x", i)))
	}), isConflict)
	cur, err := s.Ref(ctx, repoText, "x")
	ok(t, err)
	onlyOneWins(t, race(func(int) error { return s.MergeDeleteRef(ctx, repoText, "x", cur.Generation) }), isConflict)
	if evs, err := s.Events(ctx, 0, 100); err != nil || len(evs) != 2 {
		t.Errorf("events = %+v, %v, want the one create and the one delete", evs, err)
	}
}

func eventPerWrite(t *testing.T, s meta.Store) {
	ok(t, s.CreateRepo(ctx, meta.Repo{Path: "p1", Name: "n", Owner: "o"}, "native:p1", commitA))
	claim(t, s, "ws1", 1)
	made, err := s.SwapRef(ctx, "ws1", 1, repoText, "x", 0, commitA)
	ok(t, err)
	refConflict(t, swapErr(s.SwapRef(ctx, "ws1", 1, repoText, "x", 0, commitB)))
	is(t, swapErr(s.SwapRef(ctx, "ws1", 2, repoText, "x", made.Generation, commitB)), meta.ErrClaimLost)
	refConflict(t, swapErr(s.MergeRef(ctx, repoText, "x", made.Generation+100, commitB)))
	refConflict(t, s.DeleteRef(ctx, "ws1", 1, repoText, "none", 5))
	moved, err := s.MergeRef(ctx, repoText, "x", made.Generation, commitB)
	ok(t, err)
	ok(t, s.DeleteRef(ctx, "ws1", 1, repoText, "x", moved.Generation))
	want := []meta.Event{
		{Repo: "native:p1", Ref: "main", New: commitA, Kind: meta.WriterOperator},
		{Repo: repoText, Ref: "x", New: commitA, Generation: made.Generation, Kind: meta.WriterNode},
		{Repo: repoText, Ref: "x", Old: commitA, New: commitB, Generation: moved.Generation, Kind: meta.WriterMerge},
		{Repo: repoText, Ref: "x", Old: commitB, Generation: moved.Generation, Kind: meta.WriterNode},
	}
	got, err := s.Events(ctx, 0, 100)
	ok(t, err)
	if len(got) != len(want) {
		t.Fatalf("events = %+v, want %d: only a write that changed a ref has one", got, len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.At.IsZero() || g.Generation <= 0 || (i > 0 && g.Seq <= got[i-1].Seq) {
			t.Errorf("event %d = %+v: want a time, a positive generation and a rising Seq", i, g)
		}
		g.Seq, g.At = 0, time.Time{}
		if i == 0 {
			g.Generation = 0
		}
		if g != w {
			t.Errorf("event %d = %+v, want %+v", i, g, w)
		}
	}
	page, err := s.Events(ctx, got[0].Seq, 2)
	if err != nil || len(page) != 2 || page[0].Seq != got[1].Seq || page[1].Seq != got[2].Seq {
		t.Errorf("page after the first = %+v, %v, want events 1 and 2", page, err)
	}
}

func eventSequenceGapFree(t *testing.T, s meta.Store) {
	const writers, each = 4, 10
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range each {
				_, err := s.MergeRef(ctx, repoText, fmt.Sprintf("r%d-%d", w, i), 0, commitA)
				ok(t, err)
			}
		})
	}
	writersDone := make(chan struct{})
	go func() { wg.Wait(); close(writersDone) }()
	var seen []int64
	var cursor int64
	for finished := false; !finished; {
		select {
		case <-writersDone:
			finished = true
		default:
			runtime.Gosched()
		}
		evs, err := s.Events(ctx, cursor, 7)
		ok(t, err)
		for _, e := range evs {
			if len(seen) > 0 && e.Seq != seen[len(seen)-1]+1 {
				t.Fatalf("a reader at %d saw %d after %d: a later write became visible before an earlier one", cursor, e.Seq, seen[len(seen)-1])
			}
			seen, cursor = append(seen, e.Seq), e.Seq
		}
	}
	for len(seen) < writers*each {
		evs, err := s.Events(ctx, cursor, 100)
		ok(t, err)
		if len(evs) == 0 {
			t.Fatalf("a reader saw %d events, want %d", len(seen), writers*each)
		}
		for _, e := range evs {
			if e.Seq != seen[len(seen)-1]+1 {
				t.Fatalf("seq %d follows %d, want no gap", e.Seq, seen[len(seen)-1])
			}
			seen, cursor = append(seen, e.Seq), e.Seq
		}
	}
}

func eventPrune(t *testing.T, s meta.Store) {
	first, err := s.MergeRef(ctx, repoText, "a", 0, commitA)
	ok(t, err)
	ok(t, s.PruneEvents(ctx, time.Now().Add(-time.Hour)))
	if evs, err := s.Events(ctx, 0, 10); err != nil || len(evs) != 1 {
		t.Fatalf("after a prune of nothing: %+v, %v", evs, err)
	}
	ok(t, s.PruneEvents(ctx, time.Now().Add(time.Hour)))
	_, err = s.Events(ctx, 0, 10)
	is(t, err, meta.ErrEventsPruned)
	sub, err := s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://x.invalid", Secret: "k"})
	if err != nil || sub.Cursor != 1 {
		t.Fatalf("a subscription after a prune = %+v, %v, want the cursor at the newest Seq 1", sub, err)
	}
	if _, err := s.Events(ctx, sub.Cursor, 10); err != nil {
		t.Errorf("events from that cursor: %v", err)
	}
	_, err = s.MergeRef(ctx, repoText, "a", first.Generation, commitB)
	ok(t, err)
	evs, err := s.Events(ctx, 1, 10)
	if err != nil || len(evs) != 1 || evs[0].Seq != 2 {
		t.Errorf("after a prune: %+v, %v, want the one new event at Seq 2", evs, err)
	}
}

func protectedRefs(t *testing.T, s meta.Store) {
	if got, err := s.ProtectedRefs(ctx, repoText); err != nil || len(got) != 0 {
		t.Fatalf("new repo = %v, %v, want none", got, err)
	}
	ok(t, s.SetProtectedRefs(ctx, repoText, []string{"main", "release/*"}))
	ok(t, s.SetProtectedRefs(ctx, "gh:acme/other", []string{"x"}))
	if got, err := s.ProtectedRefs(ctx, repoText); err != nil || fmt.Sprint(got) != "[main release/*]" {
		t.Errorf("set = %v, %v", got, err)
	}
	ok(t, s.SetProtectedRefs(ctx, repoText, []string{"dev"}))
	if got, _ := s.ProtectedRefs(ctx, repoText); fmt.Sprint(got) != "[dev]" {
		t.Errorf("replace = %v, want [dev]", got)
	}
	ok(t, s.SetProtectedRefs(ctx, repoText, nil))
	if got, err := s.ProtectedRefs(ctx, repoText); err != nil || len(got) != 0 {
		t.Errorf("clear = %v, %v", got, err)
	}
	if got, _ := s.ProtectedRefs(ctx, "gh:acme/other"); fmt.Sprint(got) != "[x]" {
		t.Errorf("other repo = %v, want [x]", got)
	}
}

func subscriptions(t *testing.T, s meta.Store) {
	_, err := s.MergeRef(ctx, repoText, "a", 0, commitA)
	ok(t, err)
	_, err = s.MergeRef(ctx, repoText, "b", 0, commitA)
	ok(t, err)
	sub, err := s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://x.invalid/h", Secret: "k", Repo: repoText})
	if err != nil || sub.Cursor != 2 || sub.CreatedAt.IsZero() || sub.Secret != "k" {
		t.Fatalf("create = %+v, %v, want the cursor at the newest event", sub, err)
	}
	_, err = s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://y.invalid", Secret: "k"})
	is(t, err, meta.ErrExists)
	_, err = s.CreateSubscription(ctx, meta.Subscription{ID: "s2", URL: "https://z.invalid", Secret: "k"})
	ok(t, err)
	list, err := s.Subscriptions(ctx)
	if err != nil || len(list) != 2 || list[0].ID != "s1" || list[1].ID != "s2" || list[0].Repo != repoText {
		t.Errorf("list = %+v, %v, want oldest first", list, err)
	}
	ok(t, s.DeleteSubscription(ctx, "s1"))
	is(t, s.DeleteSubscription(ctx, "s1"), meta.ErrNotFound)
}

func leaseExclusive(t *testing.T, s meta.Store) {
	_, err := s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://x.invalid", Secret: "k"})
	ok(t, err)
	now := time.Now().UTC().Truncate(time.Millisecond)
	until := now.Add(30 * time.Second)
	if sub, err := s.AcquireLease(ctx, "s1", "a", now, until); err != nil || sub.URL != "https://x.invalid" || sub.Secret != "k" {
		t.Fatalf("first acquire = %+v, %v", sub, err)
	}
	_, err = s.AcquireLease(ctx, "s1", "b", now.Add(time.Second), until.Add(time.Second))
	is(t, err, meta.ErrLeaseHeld)
	_, err = s.AcquireLease(ctx, "s1", "a", now.Add(10*time.Second), until.Add(10*time.Second))
	ok(t, err)
	_, err = s.AcquireLease(ctx, "s1", "b", until, until.Add(30*time.Second))
	is(t, err, meta.ErrLeaseHeld)
	later := until.Add(10 * time.Second)
	_, err = s.AcquireLease(ctx, "s1", "b", later, later.Add(30*time.Second))
	ok(t, err)
	is(t, s.AdvanceCursor(ctx, "s1", "a", 5), meta.ErrLeaseHeld)
	ok(t, s.AdvanceCursor(ctx, "s1", "b", 5))
	ok(t, s.AdvanceCursor(ctx, "s1", "b", 3))
	if sub, err := s.AcquireLease(ctx, "s1", "b", later, later.Add(time.Minute)); err != nil || sub.Cursor != 5 {
		t.Errorf("cursor = %+v, %v, want 5: a lower cursor changes nothing", sub, err)
	}
	_, err = s.AcquireLease(ctx, "none", "a", now, until)
	is(t, err, meta.ErrNotFound)
	is(t, s.AdvanceCursor(ctx, "none", "a", 1), meta.ErrNotFound)

	_, err = s.CreateSubscription(ctx, meta.Subscription{ID: "s2", URL: "https://x.invalid", Secret: "k"})
	ok(t, err)
	onlyOneWins(t, race(func(i int) error {
		_, err := s.AcquireLease(ctx, "s2", fmt.Sprint("o", i), now, until)
		return err
	}), func(err error) bool { return err == meta.ErrLeaseHeld })
}
