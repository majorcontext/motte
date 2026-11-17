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
	{"event sequence is gap free in each partition for a concurrent reader", eventSequenceGapFree},
	{"each partition has its own counter", eventPartitions},
	{"pruned events refuse an older cursor in their partition", eventPrune},
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
	if evs := refEvents(t, s, repoText, "x"); len(evs) != 2 {
		t.Errorf("events = %+v, want the one create and the one delete", evs)
	}
}

// refEvents returns the events of one ref, in order. It fails unless the whole partition of the ref has Seq 1, 2, 3
// and so on with no gap.
func refEvents(t *testing.T, s meta.Store, repo, ref string) []meta.Event {
	t.Helper()
	part := meta.Partition(repo, ref)
	all, err := s.Events(ctx, part, 0, 1000)
	ok(t, err)
	var out []meta.Event
	for i, e := range all {
		if e.Partition != part || e.Seq != int64(i+1) {
			t.Fatalf("event %d of partition %d = %+v, want Seq %d with no gap", i, part, e, i+1)
		}
		if e.Repo == repo && e.Ref == ref {
			out = append(out, e)
		}
	}
	return out
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
	main := refEvents(t, s, "native:p1", "main")
	if len(main) != 1 || main[0].Kind != meta.WriterOperator || main[0].New != commitA || main[0].Old != "" || main[0].At.IsZero() || main[0].Generation <= 0 {
		t.Errorf("main events = %+v, want the one create of kind operator", main)
	}
	want := []meta.Event{
		{Partition: meta.Partition(repoText, "x"), Repo: repoText, Ref: "x", New: commitA, Generation: made.Generation, Kind: meta.WriterNode},
		{Partition: meta.Partition(repoText, "x"), Repo: repoText, Ref: "x", Old: commitA, New: commitB, Generation: moved.Generation, Kind: meta.WriterMerge},
		{Partition: meta.Partition(repoText, "x"), Repo: repoText, Ref: "x", Old: commitB, Generation: moved.Generation, Kind: meta.WriterNode},
	}
	got := refEvents(t, s, repoText, "x")
	if len(got) != len(want) {
		t.Fatalf("events = %+v, want %d: only a write that changed a ref has one", got, len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.At.IsZero() || (i > 0 && g.Seq <= got[i-1].Seq) {
			t.Errorf("event %d = %+v: want a time and a rising Seq", i, g)
		}
		g.Seq, g.At = 0, time.Time{}
		if g != w {
			t.Errorf("event %d = %+v, want %+v", i, g, w)
		}
	}
	page, err := s.Events(ctx, got[0].Partition, got[0].Seq, 1)
	if err != nil || len(page) != 1 || page[0].Seq != got[0].Seq+1 {
		t.Errorf("page of one after the first = %+v, %v, want the next Seq of the partition", page, err)
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
	cursors := make([]int64, meta.Partitions)
	seen := 0
	drain := func() {
		for p := range meta.Partitions {
			evs, err := s.Events(ctx, p, cursors[p], 7)
			ok(t, err)
			for _, e := range evs {
				if e.Seq != cursors[p]+1 {
					t.Fatalf("partition %d: a reader at %d saw %d: a later write became visible before an earlier one, or left a gap", p, cursors[p], e.Seq)
				}
				cursors[p] = e.Seq
				seen++
			}
		}
	}
	for finished := false; !finished; {
		select {
		case <-writersDone:
			finished = true
		default:
			runtime.Gosched()
		}
		drain()
	}
	for before := -1; seen != before; {
		before = seen
		drain()
	}
	if seen != writers*each {
		t.Errorf("a reader saw %d events, want %d", seen, writers*each)
	}
}

// distinctPartitionRefs returns n ref names in n different partitions of repoText.
func distinctPartitionRefs(n int) []string {
	var names []string
	used := map[int]bool{}
	for i := 0; len(names) < n; i++ {
		name := fmt.Sprint("ref", i)
		if p := meta.Partition(repoText, name); !used[p] {
			used[p] = true
			names = append(names, name)
		}
	}
	return names
}

func eventPartitions(t *testing.T, s meta.Store) {
	spread := map[int]bool{}
	for i := range 1000 {
		p := meta.Partition(repoText, fmt.Sprint("n", i))
		if p < 0 || p >= meta.Partitions || p != meta.Partition(repoText, fmt.Sprint("n", i)) {
			t.Fatalf("partition %d of n%d is out of range or not stable", p, i)
		}
		spread[p] = true
	}
	if len(spread) < meta.Partitions/2 {
		t.Errorf("1000 names fall in %d partitions, want a spread", len(spread))
	}
	names := distinctPartitionRefs(8)
	for _, err := range race(func(i int) error { return swapErr(s.MergeRef(ctx, repoText, names[i], 0, commitA)) }) {
		ok(t, err)
	}
	for _, n := range names {
		if evs := refEvents(t, s, repoText, n); len(evs) != 1 || evs[0].Seq != 1 || evs[0].Partition != meta.Partition(repoText, n) {
			t.Errorf("%s: events %+v, want one at Seq 1: a partition has its own counter", n, evs)
		}
	}
	first, err := s.Ref(ctx, repoText, names[0])
	ok(t, err)
	_, err = s.MergeRef(ctx, repoText, names[0], first.Generation, commitB)
	ok(t, err)
	if evs := refEvents(t, s, repoText, names[0]); len(evs) != 2 || evs[1].Seq != 2 {
		t.Errorf("a second write of one ref = %+v, want Seq 2 in the same partition", evs)
	}
	heads, err := s.EventHeads(ctx)
	var total int64
	for _, h := range heads {
		total += h
	}
	if err != nil || len(heads) != meta.Partitions || total != 9 {
		t.Errorf("heads = %v, %v, want %d partitions that hold 9 events", heads, err, meta.Partitions)
	}
}

func eventPrune(t *testing.T, s meta.Store) {
	first, err := s.MergeRef(ctx, repoText, "a", 0, commitA)
	ok(t, err)
	pa := meta.Partition(repoText, "a")
	other := (pa + 1) % meta.Partitions
	ok(t, s.PruneEvents(ctx, time.Now().Add(-time.Hour)))
	if evs, err := s.Events(ctx, pa, 0, 10); err != nil || len(evs) != 1 {
		t.Fatalf("after a prune of nothing: %+v, %v", evs, err)
	}
	ok(t, s.PruneEvents(ctx, time.Now().Add(time.Hour)))
	_, err = s.Events(ctx, pa, 0, 10)
	is(t, err, meta.ErrEventsPruned)
	if _, err := s.Events(ctx, other, 0, 10); err != nil {
		t.Errorf("a partition that lost no event refuses Seq 0: %v", err)
	}
	sub, err := s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://x.invalid", Secret: "k"})
	if err != nil || len(sub.Cursors) != meta.Partitions || sub.Cursors[pa] != 1 {
		t.Fatalf("a subscription after a prune = %+v, %v, want the cursor of the partition at Seq 1", sub, err)
	}
	if _, err := s.Events(ctx, pa, sub.Cursors[pa], 10); err != nil {
		t.Errorf("events from that cursor: %v", err)
	}
	_, err = s.MergeRef(ctx, repoText, "a", first.Generation, commitB)
	ok(t, err)
	evs, err := s.Events(ctx, pa, 1, 10)
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
	heads, err := s.EventHeads(ctx)
	ok(t, err)
	sub, err := s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://x.invalid/h", Secret: "k", Repo: repoText})
	if err != nil || fmt.Sprint(sub.Cursors) != fmt.Sprint(heads) || sub.CreatedAt.IsZero() || sub.Secret != "k" {
		t.Fatalf("create = %+v, %v, want the cursors at the heads %v", sub, err, heads)
	}
	_, err = s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://y.invalid", Secret: "k"})
	is(t, err, meta.ErrExists)
	_, err = s.CreateSubscription(ctx, meta.Subscription{ID: "s2", URL: "https://z.invalid", Secret: "k"})
	ok(t, err)
	list, err := s.Subscriptions(ctx)
	if err != nil || len(list) != 2 || list[0].ID != "s1" || list[1].ID != "s2" || list[0].Repo != repoText || fmt.Sprint(list[0].Cursors) != fmt.Sprint(heads) {
		t.Errorf("list = %+v, %v, want oldest first with a cursor for each partition", list, err)
	}
	ok(t, s.DeleteSubscription(ctx, "s1"))
	is(t, s.DeleteSubscription(ctx, "s1"), meta.ErrNotFound)
	_, err = s.AcquireLease(ctx, "s1", 0, "a", time.Now(), time.Now().Add(time.Second))
	is(t, err, meta.ErrNotFound)
}

func leaseExclusive(t *testing.T, s meta.Store) {
	_, err := s.CreateSubscription(ctx, meta.Subscription{ID: "s1", URL: "https://x.invalid", Secret: "k"})
	ok(t, err)
	now := time.Now().UTC().Truncate(time.Millisecond)
	until := now.Add(30 * time.Second)
	if l, err := s.AcquireLease(ctx, "s1", 3, "a", now, until); err != nil || l.URL != "https://x.invalid" || l.Secret != "k" || l.Partition != 3 {
		t.Fatalf("first acquire = %+v, %v", l, err)
	}
	if _, err := s.AcquireLease(ctx, "s1", 4, "b", now, until); err != nil {
		t.Errorf("another partition of the same subscription: %v", err)
	}
	_, err = s.AcquireLease(ctx, "s1", 3, "b", now.Add(time.Second), until.Add(time.Second))
	is(t, err, meta.ErrLeaseHeld)
	_, err = s.AcquireLease(ctx, "s1", 3, "a", now.Add(10*time.Second), until.Add(10*time.Second))
	ok(t, err)
	_, err = s.AcquireLease(ctx, "s1", 3, "b", until, until.Add(30*time.Second))
	is(t, err, meta.ErrLeaseHeld)
	later := until.Add(10 * time.Second)
	_, err = s.AcquireLease(ctx, "s1", 3, "b", later, later.Add(30*time.Second))
	ok(t, err)
	is(t, s.AdvanceCursor(ctx, "s1", 3, "a", 5), meta.ErrLeaseHeld)
	ok(t, s.AdvanceCursor(ctx, "s1", 3, "b", 5))
	ok(t, s.AdvanceCursor(ctx, "s1", 3, "b", 3))
	if l, err := s.AcquireLease(ctx, "s1", 3, "b", later, later.Add(time.Minute)); err != nil || l.Cursor != 5 {
		t.Errorf("cursor = %+v, %v, want 5: a lower cursor changes nothing", l, err)
	}
	if l, err := s.AcquireLease(ctx, "s1", 4, "b", later, later.Add(time.Minute)); err != nil || l.Cursor != 0 {
		t.Errorf("cursor of another partition = %+v, %v, want 0", l, err)
	}
	_, err = s.AcquireLease(ctx, "none", 3, "a", now, until)
	is(t, err, meta.ErrNotFound)
	is(t, s.AdvanceCursor(ctx, "none", 3, "a", 1), meta.ErrNotFound)

	_, err = s.CreateSubscription(ctx, meta.Subscription{ID: "s2", URL: "https://x.invalid", Secret: "k"})
	ok(t, err)
	onlyOneWins(t, race(func(i int) error {
		_, err := s.AcquireLease(ctx, "s2", 7, fmt.Sprint("o", i), now, until)
		return err
	}), func(err error) bool { return err == meta.ErrLeaseHeld })
}
